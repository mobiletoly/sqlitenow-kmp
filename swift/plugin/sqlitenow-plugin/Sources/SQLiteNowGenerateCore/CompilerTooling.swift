import Foundation

public enum CompilerTool: Equatable {
    case executable(URL)
    case jar(URL)

    var executableURL: URL {
        switch self {
        case .executable(let url):
            return url
        case .jar:
            return URL(fileURLWithPath: "/usr/bin/env")
        }
    }

    func arguments(_ compilerArguments: [String]) -> [String] {
        switch self {
        case .executable:
            return compilerArguments
        case .jar(let jar):
            return ["java", "-jar", jar.path] + compilerArguments
        }
    }
}

public enum CompilerToolResolver {
    public static func resolve(
        explicitExecutablePath: String?,
        explicitJarPath: String?,
        environment: [String: String],
        relativeTo packageRoot: URL,
        bundledCompilerJar: URL? = CompilerJarResolver.bundledCompilerJarURL(),
        fileManager: FileManager = .default
    ) throws -> CompilerTool {
        if let explicitExecutablePath, !explicitExecutablePath.trimmed().isEmpty {
            return .executable(try resolveConfiguredExecutable(
                explicitExecutablePath,
                relativeTo: packageRoot,
                fileManager: fileManager
            ))
        }

        let jar = try CompilerJarResolver.resolve(
            explicitPath: explicitJarPath,
            environment: environment,
            relativeTo: packageRoot,
            bundledCompilerJar: bundledCompilerJar,
            fileManager: fileManager
        )
        return .jar(jar)
    }

    private static func resolveConfiguredExecutable(
        _ configuredPath: String,
        relativeTo packageRoot: URL,
        fileManager: FileManager
    ) throws -> URL {
        let resolved = SQLiteNowPath.resolve(configuredPath, relativeTo: packageRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler executable not found at \(resolved.path).")
        }
        guard fileManager.isExecutableFile(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler executable is not executable at \(resolved.path).")
        }
        return resolved
    }
}

public enum CompilerJarResolver {
    public static let environmentKey = "SQLITENOW_COMPILER_JAR"

    public static func bundledCompilerJarURL() -> URL? {
        #if SWIFT_PACKAGE
        return Bundle.module.url(forResource: "sqlitenow-compiler", withExtension: "jar", subdirectory: "Resources")
            ?? Bundle.module.url(forResource: "sqlitenow-compiler", withExtension: "jar")
        #else
        return nil
        #endif
    }

    public static func resolve(
        explicitPath: String?,
        environment: [String: String],
        relativeTo packageRoot: URL,
        bundledCompilerJar: URL? = bundledCompilerJarURL(),
        fileManager: FileManager = .default
    ) throws -> URL {
        if let explicitPath, !explicitPath.trimmed().isEmpty {
            return try resolveConfiguredJar(
                explicitPath,
                relativeTo: packageRoot,
                fileManager: fileManager
            )
        }

        if let environmentPath = environment[environmentKey], !environmentPath.trimmed().isEmpty {
            return try resolveConfiguredJar(
                environmentPath,
                relativeTo: packageRoot,
                fileManager: fileManager
            )
        }

        if let bundledCompilerJar {
            let resolved = bundledCompilerJar.standardizedFileURL
            guard fileManager.fileExists(atPath: resolved.path) else {
                throw SQLiteNowGenerateError(
                    "Bundled compiler jar not found at \(resolved.path). Run ./gradlew :sqlitenow-compiler:syncSwiftPmPluginCompilerJar, pass --compiler-executable, pass --compiler-jar, or set \(environmentKey)."
                )
            }
            return resolved
        }

        throw SQLiteNowGenerateError(
            "Compiler jar not found. Run ./gradlew :sqlitenow-compiler:syncSwiftPmPluginCompilerJar, pass --compiler-executable, pass --compiler-jar, or set \(environmentKey)."
        )
    }

    private static func resolveConfiguredJar(
        _ configuredPath: String,
        relativeTo packageRoot: URL,
        fileManager: FileManager
    ) throws -> URL {
        let resolved = SQLiteNowPath.resolve(configuredPath, relativeTo: packageRoot)
        guard fileManager.fileExists(atPath: resolved.path) else {
            throw SQLiteNowGenerateError("Compiler jar not found at \(resolved.path).")
        }
        return resolved
    }
}

public protocol ProcessRunning {
    func run(executable: URL, arguments: [String], workingDirectory: URL) throws -> ProcessResult
}

public struct ProcessResult: Equatable {
    public let exitCode: Int32
    public let stdout: String
    public let stderr: String

    public init(exitCode: Int32, stdout: String, stderr: String) {
        self.exitCode = exitCode
        self.stdout = stdout
        self.stderr = stderr
    }
}

public struct SystemProcessRunner: ProcessRunning {
    public init() {}

    public func run(executable: URL, arguments: [String], workingDirectory: URL) throws -> ProcessResult {
        let fileManager = FileManager.default
        let captureDirectory = fileManager.temporaryDirectory
            .appendingPathComponent("sqlitenow-process-\(UUID().uuidString)", isDirectory: true)
        try fileManager.createDirectory(at: captureDirectory, withIntermediateDirectories: true)
        defer { try? fileManager.removeItem(at: captureDirectory) }

        let stdoutURL = captureDirectory.appendingPathComponent("stdout.txt")
        let stderrURL = captureDirectory.appendingPathComponent("stderr.txt")
        fileManager.createFile(atPath: stdoutURL.path, contents: nil)
        fileManager.createFile(atPath: stderrURL.path, contents: nil)
        let stdoutHandle = try FileHandle(forWritingTo: stdoutURL)
        let stderrHandle = try FileHandle(forWritingTo: stderrURL)
        defer {
            try? stdoutHandle.close()
            try? stderrHandle.close()
        }

        let process = Process()
        process.executableURL = executable
        process.arguments = arguments
        process.currentDirectoryURL = workingDirectory
        process.standardOutput = stdoutHandle
        process.standardError = stderrHandle

        try process.run()
        process.waitUntilExit()

        try stdoutHandle.close()
        try stderrHandle.close()
        let stdoutData = try Data(contentsOf: stdoutURL)
        let stderrData = try Data(contentsOf: stderrURL)
        return ProcessResult(
            exitCode: process.terminationStatus,
            stdout: String(decoding: stdoutData, as: UTF8.self),
            stderr: String(decoding: stderrData, as: UTF8.self)
        )
    }
}

struct SQLiteNowCompilerDriver {
    let compilerTool: CompilerTool
    let processRunner: ProcessRunning
    let workingDirectory: URL

    func version() throws -> String {
        let result = try processRunner.run(
            executable: compilerTool.executableURL,
            arguments: compilerTool.arguments(["--version"]),
            workingDirectory: workingDirectory
        )
        guard result.exitCode == 0 else {
            throw SQLiteNowGenerateError(
                "Unable to run SQLiteNow compiler. Ensure Java 17+ is available. stderr: \(result.stderr.trimmed())"
            )
        }
        let version = result.stdout.trimmed()
        guard !version.isEmpty else {
            throw SQLiteNowGenerateError("SQLiteNow compiler --version returned an empty version.")
        }
        return version
    }

    func execute(requestFile: URL) throws {
        let result = try processRunner.run(
            executable: compilerTool.executableURL,
            arguments: compilerTool.arguments(["--request", requestFile.path]),
            workingDirectory: workingDirectory
        )
        if result.exitCode != 0 {
            if
                let data = result.stdout.data(using: .utf8),
                let response = try? JSONDecoder().decode(CompilerResponse.self, from: data)
            {
                try response.requireSuccess()
            }
            throw SQLiteNowGenerateError(
                "SQLiteNow compiler failed with exit code \(result.exitCode). stderr: \(result.stderr.trimmed()) stdout: \(result.stdout.trimmed())"
            )
        }
        guard let data = result.stdout.data(using: .utf8) else {
            throw SQLiteNowGenerateError("SQLiteNow compiler produced non-UTF-8 output.")
        }
        do {
            let response = try JSONDecoder().decode(CompilerResponse.self, from: data)
            try response.requireSuccess()
        } catch let error as SQLiteNowGenerateError {
            throw error
        } catch {
            throw SQLiteNowGenerateError(
                "SQLiteNow compiler produced invalid JSON output: \(error.localizedDescription). stdout: \(result.stdout.trimmed()) stderr: \(result.stderr.trimmed())"
            )
        }
    }
}

private struct CompilerResponse: Decodable {
    let success: Bool
    let failure: CompilerFailure?

    func requireSuccess() throws {
        guard success else {
            throw SQLiteNowGenerateError(failure?.message ?? "SQLiteNow compiler request failed.")
        }
    }
}

private struct CompilerFailure: Decodable {
    let message: String
}
