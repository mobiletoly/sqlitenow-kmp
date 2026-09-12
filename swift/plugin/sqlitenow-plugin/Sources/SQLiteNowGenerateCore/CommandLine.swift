import Foundation

public enum SQLiteNowGenerateCommand {
    public static func main() -> Never {
        do {
            let options = try CommandLineOptions.parse(arguments: Array(CommandLine.arguments.dropFirst()))
            if options.showHelp {
                print(Self.helpText)
                exit(0)
            }
            try SQLiteNowGenerator().generate(options: options)
            exit(0)
        } catch {
            fputs("SQLiteNow generation failed: \(error.localizedDescription)\n", stderr)
            exit(1)
        }
    }

    public static let helpText = """
    Usage: sqlite-now-generate [--package-root <path>] [--config <path>] [--compiler-executable <path>] [--compiler-jar <path>]

    Options:
      --package-root <path>        Swift package root. Defaults to the current directory.
                                   Xcode command plugins default this to the .xcodeproj directory; pass this explicitly when SQLiteNow.json, SQL, and output live above it.
      --config <path>              SQLiteNow config file. Defaults to <package-root>/SQLiteNow.json.
      --compiler-executable <path> Runnable SQLiteNow compiler executable. Takes precedence over jar options.
      --compiler-jar <path>        Runnable SQLiteNow compiler jar. Defaults to SQLITENOW_COMPILER_JAR.
    """
}

public struct CommandLineOptions: Equatable {
    public var packageRoot: URL
    public var configPath: URL?
    public var compilerExecutablePath: String?
    public var compilerJarPath: String?
    public var showHelp: Bool

    public static func parse(
        arguments: [String],
        currentDirectory: URL = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    ) throws -> CommandLineOptions {
        var packageRoot = currentDirectory
        var configPath: String?
        var compilerExecutablePath: String?
        var compilerJarPath: String?
        var showHelp = false
        var index = 0

        func requireValue(after flag: String) throws -> String {
            let valueIndex = index + 1
            guard valueIndex < arguments.count else {
                throw SQLiteNowGenerateError("Missing value after \(flag).")
            }
            index = valueIndex
            return arguments[valueIndex]
        }

        while index < arguments.count {
            let argument = arguments[index]
            switch argument {
            case "--help", "-h":
                showHelp = true
            case "--package-root":
                packageRoot = SQLiteNowPath.resolve(try requireValue(after: argument), relativeTo: currentDirectory)
            case "--config":
                configPath = try requireValue(after: argument)
            case "--compiler-executable":
                compilerExecutablePath = try requireValue(after: argument)
            case "--compiler-jar":
                compilerJarPath = try requireValue(after: argument)
            default:
                throw SQLiteNowGenerateError("Unsupported argument '\(argument)'.")
            }
            index += 1
        }

        let resolvedPackageRoot = packageRoot.standardizedFileURL
        return CommandLineOptions(
            packageRoot: resolvedPackageRoot,
            configPath: configPath.map { SQLiteNowPath.resolve($0, relativeTo: resolvedPackageRoot).standardizedFileURL },
            compilerExecutablePath: compilerExecutablePath,
            compilerJarPath: compilerJarPath,
            showHelp: showHelp
        )
    }
}
