import Foundation
import PackagePlugin

@main
struct SQLiteNowGeneratePlugin: CommandPlugin {
    func performCommand(context: PluginContext, arguments: [String]) async throws {
        let packageRoot = context.package.directoryURL
        try Self.runGenerator(
            tool: context.tool(named: "SQLiteNowGenerateTool"),
            packageRoot: packageRoot,
            compilerTool: try Self.resolvedCompilerTool { try context.tool(named: "SQLiteNowCompiler") },
            arguments: arguments
        )
    }
}

#if canImport(XcodeProjectPlugin)
import XcodeProjectPlugin

extension SQLiteNowGeneratePlugin: XcodeCommandPlugin {
    func performCommand(context: XcodePluginContext, arguments: [String]) throws {
        let packageRoot = URL(fileURLWithPath: context.xcodeProject.directory.string).standardizedFileURL
        try Self.runGenerator(
            tool: context.tool(named: "SQLiteNowGenerateTool"),
            packageRoot: packageRoot,
            compilerTool: try Self.resolvedCompilerTool { try context.tool(named: "SQLiteNowCompiler") },
            arguments: arguments
        )
    }
}
#endif

private extension SQLiteNowGeneratePlugin {
    static func runGenerator(
        tool: PluginContext.Tool,
        packageRoot: URL,
        compilerTool: PluginContext.Tool?,
        arguments: [String]
    ) throws {
        var generatorArguments = ["--package-root", packageRoot.path]
        if let compilerTool {
            generatorArguments += ["--compiler-executable", compilerTool.url.path]
        } else {
            let message = """
            SQLiteNow warning: executable tool 'SQLiteNowCompiler' was not resolved by SwiftPM; falling back to compiler jar resolution. Release distributions should declare the SQLiteNowCompiler binary target.

            """
            FileHandle.standardError.write(Data(message.utf8))
        }

        let process = Process()
        process.executableURL = tool.url
        process.currentDirectoryURL = packageRoot
        process.arguments = generatorArguments + arguments
        process.standardOutput = FileHandle.standardOutput
        process.standardError = FileHandle.standardError

        try process.run()
        process.waitUntilExit()

        if process.terminationReason != .exit || process.terminationStatus != 0 {
            throw SQLiteNowPluginError.generationFailed(status: process.terminationStatus)
        }
    }

    static func resolvedCompilerTool(resolve: () throws -> PluginContext.Tool) throws -> PluginContext.Tool? {
        do {
            return try resolve()
        } catch {
            if isReleaseDistribution() {
                throw SQLiteNowPluginError.missingReleaseCompilerTool(errorDescription: "\(error)")
            }
            return nil
        }
    }

    static func isReleaseDistribution(sourceFilePath: String = #filePath) -> Bool {
        releaseDistributionMarker(sourceFilePath: sourceFilePath) != nil
    }

    static func releaseDistributionMarker(sourceFilePath: String) -> URL? {
        let fileManager = FileManager.default
        var directory = URL(fileURLWithPath: sourceFilePath)
            .standardizedFileURL
            .deletingLastPathComponent()

        while true {
            let marker = directory
                .appendingPathComponent(".sqlitenow")
                .appendingPathComponent("release-distribution.json")
            if fileManager.fileExists(atPath: marker.path) {
                return marker
            }

            let parent = directory.deletingLastPathComponent()
            if parent.path == directory.path {
                return nil
            }
            directory = parent
        }
    }
}

private enum SQLiteNowPluginError: Error, CustomStringConvertible {
    case generationFailed(status: Int32)
    case missingReleaseCompilerTool(errorDescription: String)

    var description: String {
        switch self {
        case .generationFailed(let status):
            "SQLiteNow generation failed with exit code \(status)."
        case .missingReleaseCompilerTool(let errorDescription):
            "SQLiteNow release distribution is missing the SQLiteNowCompiler binary target: \(errorDescription)"
        }
    }
}
