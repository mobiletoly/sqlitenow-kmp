import Foundation

public struct SQLiteNowGenerator {
    private let fileManager: FileManager
    private let processRunner: ProcessRunning
    private let environment: [String: String]
    private let bundledCompilerJar: URL?
    private let releaseDistribution: SQLiteNowReleaseDistribution?
    private let releaseDistributionURL: URL?

    public init(
        fileManager: FileManager = .default,
        processRunner: ProcessRunning = SystemProcessRunner(),
        environment: [String: String] = ProcessInfo.processInfo.environment,
        bundledCompilerJar: URL? = CompilerJarResolver.bundledCompilerJarURL(),
        releaseDistribution: SQLiteNowReleaseDistribution? = nil,
        releaseDistributionURL: URL? = SQLiteNowReleaseDistributionResolver.bundledReleaseDistributionURL()
    ) {
        self.fileManager = fileManager
        self.processRunner = processRunner
        self.environment = environment
        self.bundledCompilerJar = bundledCompilerJar
        self.releaseDistribution = releaseDistribution
        self.releaseDistributionURL = releaseDistributionURL
    }

    public func generate(options: CommandLineOptions) throws {
        let packageRoot = options.packageRoot.standardizedFileURL
        let configPath = (options.configPath ?? packageRoot.appendingPathComponent("SQLiteNow.json")).standardizedFileURL
        let config = try SQLiteNowConfigLoader.load(configPath: configPath, fileManager: fileManager)
        let compilerTool = try CompilerToolResolver.resolve(
            explicitExecutablePath: options.compilerExecutablePath,
            explicitJarPath: options.compilerJarPath,
            environment: environment,
            relativeTo: packageRoot,
            bundledCompilerJar: bundledCompilerJar,
            fileManager: fileManager
        )
        let compilerDriver = SQLiteNowCompilerDriver(
            compilerTool: compilerTool,
            processRunner: processRunner,
            workingDirectory: packageRoot
        )
        let compilerVersion = try compilerDriver.version()
        let effectiveReleaseDistribution: SQLiteNowReleaseDistribution?
        if let releaseDistribution {
            effectiveReleaseDistribution = releaseDistribution
        } else if let releaseDistributionURL {
            effectiveReleaseDistribution = try SQLiteNowReleaseDistributionResolver.load(url: releaseDistributionURL)
        } else {
            effectiveReleaseDistribution = nil
        }
        let invocations = try SQLiteNowCompilerRequestBuilder.buildInvocations(
            config: config,
            configPath: configPath,
            packageRoot: packageRoot,
            compilerVersion: compilerVersion,
            releaseDistribution: effectiveReleaseDistribution,
            fileManager: fileManager
        )

        for invocation in invocations {
            for directory in invocation.directoriesToCreate {
                try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            }
            try invocation.requestJSON.write(to: invocation.requestFile, atomically: true, encoding: .utf8)
            try compilerDriver.execute(requestFile: invocation.requestFile)
        }

        print(InstructionRenderer.render(invocations.map(\.instructions)))
    }
}

public enum InstructionRenderer {
    public static func render(_ instructions: [GeneratedPackageInstructions]) -> String {
        guard !instructions.isEmpty else {
            return "SQLiteNow did not generate any packages."
        }
        let packageLines = instructions.map { instruction in
            """
            SQLiteNow generated \(instruction.packageName) at \(instruction.relativePackagePath)

            SwiftPM dependency:
                .package(path: "\(instruction.relativePackagePath)")

            Target dependency:
                .product(name: "\(instruction.productName)", package: "\(instruction.packageName)")

            Xcode:
                Add Local Package: \(instruction.relativePackagePath)
                Link product: \(instruction.productName)
            """
        }
        return packageLines.joined(separator: "\n\n")
    }
}
