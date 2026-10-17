import Foundation
import Combine
import UIKit
import MWDATCore
import MWDATCamera

@MainActor
class RayBanManager: NSObject, ObservableObject, FrameSource {

    // MARK: - FrameSource Protocol

    @Published var latestFrame: Data?
    @Published var latestImage: UIImage?
    @Published private(set) var isRunning: Bool = false
    @Published private(set) var connectionStatus: FrameSourceStatus = .disconnected
    @Published var frameCount: Int = 0

    let sourceType: FrameSourceType = .rayBan

    // MARK: - State

    @Published var glassesName: String = "Not Connected"
    @Published var hasActiveDevice: Bool = false
    @Published var registrationState: String = "unregistered"
    @Published var isRegistered: Bool = false

    // DAT SDK (0.9: DeviceSession → Camera → Stream)
    private var deviceSession: DeviceSession?
    private var camera: Camera?
    private var deviceSelector: AutoDeviceSelector?
    private var stateToken: AnyListenerToken?
    private var frameToken: AnyListenerToken?
    private var errorToken: AnyListenerToken?
    private var sessionErrorToken: AnyListenerToken?
    private var deviceMonitorTask: Task<Void, Never>?
    private var sessionTask: Task<Void, Never>?
    private var registrationTask: Task<Void, Never>?

    private var frameInterval: TimeInterval = 1.0
    private var jpegQuality: CGFloat = 0.5
    private var lastCaptureTime: Date = .distantPast

    // MARK: - Configuration

    func configure(frameInterval: TimeInterval = 1.0, jpegQuality: CGFloat = 0.5) {
        self.frameInterval = frameInterval
        self.jpegQuality = jpegQuality
    }

    // MARK: - Registration (MUST complete before streaming)

    func startMonitoringRegistration() {
        let wearables = Wearables.shared
        registrationTask = Task { @MainActor in
            for await state in wearables.registrationStateStream() {
                switch state {
                case .registered:
                    self.registrationState = "registered"
                    self.isRegistered = true
                    print("[RayBan] Registration complete")
                case .registering:
                    self.registrationState = "registering"
                    print("[RayBan] Registration in progress...")
                case .available, .unavailable:
                    self.registrationState = "unregistered"
                    self.isRegistered = false
                    print("[RayBan] Not registered")
                @unknown default:
                    break
                }
            }
        }

        // Check current state
        let currentState = wearables.registrationState
        switch currentState {
        case .registered:
            isRegistered = true
            registrationState = "registered"
        case .registering:
            registrationState = "registering"
        case .available, .unavailable:
            registrationState = "unregistered"
        @unknown default:
            break
        }
    }

    func register() async {
        guard !isRegistered else {
            print("[RayBan] Already registered")
            return
        }

        do {
            print("[RayBan] Starting registration with Meta AI...")
            connectionStatus = .connecting
            try await Wearables.shared.startRegistration()
        } catch {
            print("[RayBan] Registration error: \(error)")
            connectionStatus = .error("Registration failed: \(error.localizedDescription)")
        }
    }

    // MARK: - FrameSource Implementation

    func start() throws {
        guard !isRunning else { return }

        // Re-check registration state directly from SDK
        let currentState = Wearables.shared.registrationState
        if currentState == .registered {
            isRegistered = true
            registrationState = "registered"
        }

        guard isRegistered else {
            print("[RayBan] Not registered (state: \(currentState)) — starting registration first")
            connectionStatus = .error("Tap 'Connect Glasses' in Settings to register with Meta AI first")
            Task { await register() }
            return
        }

        connectionStatus = .connecting
        isRunning = true
        print("[RayBan] Starting device session...")

        let wearables = Wearables.shared
        let selector = AutoDeviceSelector(wearables: wearables)
        self.deviceSelector = selector

        // Monitor device availability
        deviceMonitorTask = Task { @MainActor in
            for await device in selector.activeDeviceStream() {
                self.hasActiveDevice = device != nil
                if device != nil {
                    self.glassesName = "Ray-Ban Meta"
                    print("[RayBan] Device active")
                } else {
                    self.glassesName = "No Device"
                    print("[RayBan] No active device")
                }
            }
        }

        sessionTask = Task { @MainActor in
            await self.startSessionAndStream(wearables: wearables, selector: selector)
        }
    }

    /// Permission → DeviceSession → Camera → Stream (DAT SDK 0.9 lifecycle).
    private func startSessionAndStream(wearables: WearablesInterface, selector: AutoDeviceSelector) async {
        // Camera permission is granted through Meta AI
        do {
            let status = try await wearables.checkPermissionStatus(.camera)
            print("[RayBan] Camera permission status: \(status)")
            if status != .granted {
                print("[RayBan] Requesting camera permission (will open Meta AI)...")
                let result = try await wearables.requestPermission(.camera)
                print("[RayBan] Permission result: \(result)")
                guard result == .granted else {
                    fail("Camera permission denied. Grant in Meta AI app.")
                    return
                }
            }
        } catch {
            print("[RayBan] Permission error: \(error)")
            fail("Camera permission error: \(error.localizedDescription)")
            return
        }

        // Device session
        let session: DeviceSession
        do {
            session = try wearables.createSession(deviceSelector: selector)
        } catch {
            print("[RayBan] createSession error: \(error)")
            fail("Could not create glasses session: \(error.localizedDescription)")
            return
        }
        self.deviceSession = session

        sessionErrorToken = session.errorPublisher.listen { [weak self] error in
            Task { @MainActor [weak self] in
                print("[RayBan] Session error: \(error)")
                if case .datAppOnTheGlassesUpdateRequired = error {
                    try? await Wearables.shared.openDATGlassesAppUpdate()
                }
                self?.connectionStatus = .error(error.localizedDescription)
            }
        }

        do {
            try session.start()
        } catch {
            print("[RayBan] Session start error: \(error)")
            fail("Glasses session failed: \(error.localizedDescription)")
            return
        }

        // Wait until the session is ready for capabilities
        for await state in session.stateStream() {
            print("[RayBan] Session state: \(state)")
            if state == .started { break }
            if state == .stopped {
                fail("Glasses session stopped. Power on glasses and open hinges.")
                return
            }
        }
        guard !Task.isCancelled else { return }

        // Camera + stream at 30fps, high resolution (720x1280)
        let config = StreamConfiguration(videoCodec: .raw, resolution: .high, frameRate: 30)
        let camera: Camera
        do {
            guard let added = try session.addCamera(config: config) else {
                fail("Glasses session not ready for camera.")
                return
            }
            camera = added
        } catch {
            print("[RayBan] addCamera error: \(error)")
            fail("Could not start glasses camera: \(error.localizedDescription)")
            return
        }
        self.camera = camera
        let stream = camera.stream

        // State changes
        stateToken = stream.statePublisher.listen { [weak self] (state: StreamState) in
            Task { @MainActor [weak self] in
                guard let self else { return }
                print("[RayBan] State: \(state)")
                switch state {
                case .streaming:
                    self.isRunning = true
                    self.connectionStatus = .connected
                case .stopped:
                    self.isRunning = false
                    self.connectionStatus = .disconnected
                case .waitingForDevice, .starting, .stopping, .paused:
                    self.connectionStatus = .connecting
                @unknown default:
                    break
                }
            }
        }

        // Video frames
        frameToken = stream.videoFramePublisher.listen { [weak self] (videoFrame: VideoFrame) in
            Task { @MainActor [weak self] in
                guard let self else { return }

                guard let uiImage = videoFrame.makeUIImage() else { return }

                // Always update preview
                self.latestImage = uiImage
                self.frameCount += 1

                // Throttle JPEG for Claude
                let now = Date()
                if now.timeIntervalSince(self.lastCaptureTime) >= self.frameInterval {
                    self.lastCaptureTime = now
                    if let jpegData = uiImage.jpegData(compressionQuality: self.jpegQuality) {
                        self.latestFrame = jpegData
                        print("[RayBan] Captured JPEG frame (\(jpegData.count) bytes)")
                    }
                }
            }
        }

        // Errors
        errorToken = stream.errorPublisher.listen { [weak self] (error: StreamError) in
            Task { @MainActor [weak self] in
                guard let self else { return }
                print("[RayBan] Error: \(error)")
                self.connectionStatus = .error(error.localizedDescription)
            }
        }

        stream.start()
        print("[RayBan] Stream start requested")
    }

    private func fail(_ message: String) {
        connectionStatus = .error(message)
        isRunning = false
    }

    func stop() {
        sessionTask?.cancel()
        sessionTask = nil
        camera?.stop()
        deviceSession?.stop()
        stateToken = nil
        frameToken = nil
        errorToken = nil
        sessionErrorToken = nil
        deviceMonitorTask?.cancel()
        deviceMonitorTask = nil
        camera = nil
        deviceSession = nil
        deviceSelector = nil
        isRunning = false
        connectionStatus = .disconnected
        latestFrame = nil
        latestImage = nil
        glassesName = "Not Connected"
        hasActiveDevice = false
    }

    func consumeFrame() -> Data? {
        return latestFrame
    }

    func cleanup() {
        stop()
        registrationTask?.cancel()
        registrationTask = nil
    }
}
