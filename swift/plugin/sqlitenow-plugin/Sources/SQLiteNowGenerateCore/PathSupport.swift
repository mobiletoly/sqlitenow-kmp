import Foundation

public struct SQLiteNowGenerateError: LocalizedError, Equatable {
    public let message: String

    public init(_ message: String) {
        self.message = message
    }

    public var errorDescription: String? { message }
}

enum SQLiteNowPath {
    static func resolve(_ path: String, relativeTo baseURL: URL) -> URL {
        let expanded = (path as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return URL(fileURLWithPath: expanded).standardizedFileURL
        }
        return baseURL.appendingPathComponent(expanded).standardizedFileURL
    }

    static func requestPath(_ url: URL, relativeTo baseURL: URL) -> String {
        let standardizedURL = url.standardizedFileURL
        let standardizedBaseURL = baseURL.standardizedFileURL
        let path = standardizedURL.path
        let basePath = standardizedBaseURL.path

        if path == basePath {
            return "."
        }
        if path.hasPrefix(basePath + "/") {
            let relative = String(path.dropFirst(basePath.count + 1))
            return normalizedRequestPath(relative)
        }
        return path
    }

    static func localSwiftPackageDependencyPath(
        _ url: URL,
        packageRoot: URL,
        generatedPackageRoot: URL
    ) -> String {
        let standardizedURL = url.standardizedFileURL
        let standardizedPackageRoot = packageRoot.standardizedFileURL
        guard standardizedURL.isDescendantOrEqual(to: standardizedPackageRoot) else {
            return requestPath(standardizedURL, relativeTo: standardizedPackageRoot)
        }
        return relativeRequestPath(standardizedURL, from: generatedPackageRoot)
    }

    static func defaultMetadataPackageName(_ databaseName: String) -> String {
        let rawSegments = databaseName
            .lowercased()
            .split { !$0.isLetter && !$0.isNumber }
            .map(String.init)
        let segments = rawSegments.isEmpty ? ["database"] : rawSegments.map { segment in
            guard let first = segment.first, first.isLetter else {
                return "db\(segment)"
            }
            return segment
        }
        return "sqlitenow.generated.\(segments.joined(separator: ".")).metadata"
    }

    static func safeFileName(_ value: String) -> String {
        let mapped = value.map { character -> Character in
            if character.isLetter || character.isNumber || character == "-" || character == "_" {
                return character
            }
            return "-"
        }
        let result = String(mapped).trimmingCharacters(in: CharacterSet(charactersIn: "-_"))
        return result.isEmpty ? "database" : result
    }

    private static func normalizedRequestPath(_ path: String) -> String {
        if path.hasPrefix("/") {
            return URL(fileURLWithPath: path).standardizedFileURL.path
        }
        var segments: [String] = []
        for segment in path.split(separator: "/", omittingEmptySubsequences: true).map(String.init) {
            switch segment {
            case ".":
                continue
            case "..":
                if let last = segments.last, last != ".." {
                    segments.removeLast()
                } else {
                    segments.append(segment)
                }
            default:
                segments.append(segment)
            }
        }
        return segments.isEmpty ? "." : segments.joined(separator: "/")
    }

    private static func relativeRequestPath(_ url: URL, from baseURL: URL) -> String {
        let targetComponents = url.standardizedFileURL.pathComponents
        let baseComponents = baseURL.standardizedFileURL.pathComponents
        var commonPrefixCount = 0
        while commonPrefixCount < targetComponents.count,
              commonPrefixCount < baseComponents.count,
              targetComponents[commonPrefixCount] == baseComponents[commonPrefixCount] {
            commonPrefixCount += 1
        }

        let parentSegments = Array(repeating: "..", count: baseComponents.count - commonPrefixCount)
        let childSegments = Array(targetComponents.dropFirst(commonPrefixCount))
        return normalizedRequestPath((parentSegments + childSegments).joined(separator: "/"))
    }
}

extension URL {
    func isDescendantOrEqual(to baseURL: URL) -> Bool {
        path == baseURL.path || path.hasPrefix(baseURL.path + "/")
    }

    func resolvingSymlinkComponents(fileManager: FileManager) throws -> URL {
        var resolvedComponents: [String] = []
        var pendingComponents = Array(standardizedFileURL.pathComponents.dropFirst())
        var symlinkExpansionCount = 0

        while !pendingComponents.isEmpty {
            let component = pendingComponents.removeFirst()
            switch component {
            case ".":
                continue
            case "..":
                if !resolvedComponents.isEmpty {
                    resolvedComponents.removeLast()
                }
                continue
            default:
                break
            }

            let candidatePath = "/" + (resolvedComponents + [component]).joined(separator: "/")
            if let destination = try? fileManager.destinationOfSymbolicLink(atPath: candidatePath) {
                symlinkExpansionCount += 1
                guard symlinkExpansionCount <= 64 else {
                    throw SQLiteNowGenerateError("Too many symbolic links while resolving path: \(path).")
                }
                if destination.hasPrefix("/") {
                    resolvedComponents = []
                }
                pendingComponents = destination.split(separator: "/", omittingEmptySubsequences: true).map(String.init) + pendingComponents
            } else {
                resolvedComponents.append(component)
            }
        }

        return URL(fileURLWithPath: "/" + resolvedComponents.joined(separator: "/"))
    }
}

extension FileManager {
    func directoryExists(at url: URL) -> Bool {
        var isDirectory: ObjCBool = false
        return fileExists(atPath: url.path, isDirectory: &isDirectory) && isDirectory.boolValue
    }
}

extension String {
    func trimmed() -> String {
        trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

extension Array where Element == CodingKey {
    var pathDescription: String {
        guard !isEmpty else {
            return "<root>"
        }
        return map(\.stringValue).joined(separator: ".")
    }
}
