/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.SwiftPackageLeakChecker
import java.io.File
import java.net.URI
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption.COPY_ATTRIBUTES
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObjectBuilder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

const val SWIFT_PACKAGE_MANIFEST_VERSION = 3
const val SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC = "dynamic"
const val DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS = "15"
const val DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS = "14"
val DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS: List<String> = listOf("macosArm64", "iosArm64", "iosSimulatorArm64")
private val swiftIdentifierRegex = Regex("[A-Za-z_][A-Za-z0-9_]*")
private val swiftPackageNameDisallowedCharacters = Regex("""[\\/\p{Cntrl}]""")

fun requireValidSwiftIdentifier(value: String, label: String): String {
    require(value.isNotBlank()) {
        "$label must not be blank."
    }
    require(value == value.trim() && swiftIdentifierRegex.matches(value)) {
        "$label must be a valid Swift identifier using ASCII letters, digits, and underscores with a non-digit first character: '$value'."
    }
    return value
}

internal fun requireValidSwiftPackageName(value: String, label: String): String {
    require(value.isNotBlank()) {
        "$label must not be blank."
    }
    require(value == value.trim()) {
        "$label must not have leading or trailing whitespace: '$value'."
    }
    require(!swiftPackageNameDisallowedCharacters.containsMatchIn(value)) {
        "$label must not contain path separators or control characters: '$value'."
    }
    return value
}

fun requireSupportedSwiftPackageFrameworkMode(frameworkMode: String): String {
    val normalized = frameworkMode.trim().lowercase()
    require(normalized == SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC) {
        "Swift package frameworkMode '$frameworkMode' is not supported. Only '$SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC' is supported."
    }
    return normalized
}

internal fun requireSupportedSwiftPackageAppleTargets(requestedAppleTargets: List<String>): List<String> {
    require(requestedAppleTargets.isNotEmpty()) {
        "Swift package requestedAppleTargets must not be empty."
    }
    val supportedTargets = DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS.toSet()
    val unsupportedTargets = requestedAppleTargets.filterNot { it in supportedTargets }
    require(unsupportedTargets.isEmpty()) {
        "Swift package requestedAppleTargets contains unsupported Apple target(s): " +
            "${unsupportedTargets.joinToString(", ")}. Supported native Swift runtime targets are " +
            "${DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS.joinToString(", ")}. x86_64 Apple slices are not supported."
    }
    return requestedAppleTargets
}

data class SwiftPackageMinimumPlatforms(
    val ios: String = DEFAULT_SWIFT_PACKAGE_MINIMUM_IOS,
    val macos: String = DEFAULT_SWIFT_PACKAGE_MINIMUM_MACOS,
)

enum class SwiftPackageRuntimeArtifactKind(val id: String) {
    LOCAL_XCFRAMEWORK("localXcframework"),
    LOCAL_ZIP("localZip"),
    REMOTE_ZIP("remoteZip"),
    ;

    companion object {
        fun fromId(value: String): SwiftPackageRuntimeArtifactKind =
            entries.firstOrNull { it.id == value }
                ?: error(
                    "Unsupported Swift package runtime artifact kind '$value'. " +
                        "Expected '${LOCAL_XCFRAMEWORK.id}', '${LOCAL_ZIP.id}', or '${REMOTE_ZIP.id}'."
                )
    }
}

data class SwiftPackageRuntimeArtifact(
    val kind: SwiftPackageRuntimeArtifactKind,
    val file: File? = null,
    val url: String? = null,
    val checksum: String? = null,
    val sqliteNowVersion: String? = null,
) {
    companion object {
        fun localXcframework(directory: File): SwiftPackageRuntimeArtifact =
            SwiftPackageRuntimeArtifact(
                kind = SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK,
                file = directory,
            )

        fun localZip(
            file: File,
            checksum: String? = null,
            sqliteNowVersion: String? = null,
        ): SwiftPackageRuntimeArtifact =
            SwiftPackageRuntimeArtifact(
                kind = SwiftPackageRuntimeArtifactKind.LOCAL_ZIP,
                file = file,
                checksum = checksum,
                sqliteNowVersion = sqliteNowVersion,
            )

        fun remoteZip(
            url: String,
            checksum: String? = null,
            sqliteNowVersion: String? = null,
        ): SwiftPackageRuntimeArtifact =
            SwiftPackageRuntimeArtifact(
                kind = SwiftPackageRuntimeArtifactKind.REMOTE_ZIP,
                url = url,
                checksum = checksum,
                sqliteNowVersion = sqliteNowVersion,
            )
    }
}

internal data class SwiftPackageRuntimeArtifactValidationContext(
    val runtimeModuleName: String,
    val sqliteNowVersion: String,
    val generatorVersion: String,
)

internal fun resolveSwiftPackageRuntimeArtifact(
    runtimeXcframeworkDirectory: File?,
    runtimeArtifact: SwiftPackageRuntimeArtifact?,
): SwiftPackageRuntimeArtifact {
    require(runtimeArtifact == null || runtimeXcframeworkDirectory == null) {
        "Swift package request cannot specify both runtimeXcframeworkDirectory and runtimeArtifact."
    }
    return runtimeArtifact
        ?: runtimeXcframeworkDirectory?.let { SwiftPackageRuntimeArtifact.localXcframework(it) }
        ?: error("Swift package request requires runtimeXcframeworkDirectory or runtimeArtifact.")
}

internal fun parseSwiftPackageRuntimeArtifactRequest(
    kind: String,
    path: String?,
    url: String?,
    checksum: String?,
    sqliteNowVersion: String?,
): SwiftPackageRuntimeArtifact =
    when (kind) {
        SwiftPackageRuntimeArtifactKind.LOCAL_ZIP.id -> {
            require(url == null) {
                "runtimeArtifact.kind 'localZip' must not specify url."
            }
            SwiftPackageRuntimeArtifact.localZip(
                file = File(path ?: throw IllegalArgumentException("Missing required request field 'path'.")),
                checksum = checksum,
                sqliteNowVersion = sqliteNowVersion,
            )
        }

        SwiftPackageRuntimeArtifactKind.REMOTE_ZIP.id -> {
            require(path == null) {
                "runtimeArtifact.kind 'remoteZip' must not specify path."
            }
            SwiftPackageRuntimeArtifact.remoteZip(
                url = url.orEmpty(),
                checksum = checksum,
                sqliteNowVersion = sqliteNowVersion,
            )
        }

        SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK.id -> error(
            "runtimeArtifact.kind '${SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK.id}' is not accepted in " +
                "runtimeArtifact; use runtimeXcframeworkDirectory instead."
        )

        else -> error(
            "Unsupported runtimeArtifact.kind '$kind'. Expected " +
                "'${SwiftPackageRuntimeArtifactKind.LOCAL_ZIP.id}' or " +
                "'${SwiftPackageRuntimeArtifactKind.REMOTE_ZIP.id}'."
        )
    }

internal fun requireValidSwiftPackageRuntimeArtifact(
    runtimeArtifact: SwiftPackageRuntimeArtifact,
    context: SwiftPackageRuntimeArtifactValidationContext,
) {
    val runtimeModuleName = context.runtimeModuleName
    val sqliteNowVersion = context.sqliteNowVersion
    val generatorVersion = context.generatorVersion
    when (runtimeArtifact.kind) {
        SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK -> {
            val file = runtimeArtifact.requireFile()
            require(runtimeArtifact.url == null) {
                "Local XCFramework runtime artifacts must not specify a URL."
            }
            require(runtimeArtifact.checksum == null) {
                "Local XCFramework runtime artifacts must not specify a checksum."
            }
            require(runtimeArtifact.sqliteNowVersion == null) {
                "Local XCFramework runtime artifacts must not specify sqliteNowVersion."
            }
            require(file.isDirectory) {
                "Expected reusable runtime XCFramework at ${file.absolutePath}."
            }
        }

        SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> {
            val file = runtimeArtifact.requireFile()
            require(runtimeArtifact.url == null) {
                "Local ZIP runtime artifacts must not specify a URL."
            }
            require(file.isFile) {
                "Expected reusable runtime XCFramework zip at ${file.absolutePath}."
            }
            require(!runtimeArtifact.checksum.isNullOrBlank()) {
                "Runtime artifact checksum is required for localZip runtime artifacts."
            }
            require(!runtimeArtifact.sqliteNowVersion.isNullOrBlank()) {
                "Runtime artifact sqliteNowVersion is required for localZip runtime artifacts."
            }
            validateRuntimeArtifactFileName(file.name, runtimeModuleName)
            require(generatorVersion == sqliteNowVersion) {
                "Generator version '$generatorVersion' is not compatible with requested SQLiteNow version '$sqliteNowVersion'."
            }
            require(runtimeArtifact.sqliteNowVersion == sqliteNowVersion) {
                "Runtime artifact version '${runtimeArtifact.sqliteNowVersion}' is not compatible with " +
                    "requested SQLiteNow version '$sqliteNowVersion'."
            }
            val actualChecksum = sha256(file)
            require(runtimeArtifact.checksum.equals(actualChecksum, ignoreCase = true)) {
                "Runtime artifact checksum mismatch for ${file.absolutePath}: " +
                    "expected ${runtimeArtifact.checksum}, actual $actualChecksum."
            }
        }

        SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> {
            val url = runtimeArtifact.url
            require(runtimeArtifact.file == null) {
                "Remote ZIP runtime artifacts must not specify a path."
            }
            require(!url.isNullOrBlank()) {
                "Runtime artifact URL is required for remoteZip runtime artifacts."
            }
            require(!runtimeArtifact.checksum.isNullOrBlank()) {
                "Runtime artifact checksum is required for remoteZip runtime artifacts."
            }
            require(!runtimeArtifact.sqliteNowVersion.isNullOrBlank()) {
                "Runtime artifact sqliteNowVersion is required for remoteZip runtime artifacts."
            }
            validateRuntimeArtifactUrl(url, runtimeModuleName)
            require(generatorVersion == sqliteNowVersion) {
                "Generator version '$generatorVersion' is not compatible with requested SQLiteNow version '$sqliteNowVersion'."
            }
            require(runtimeArtifact.sqliteNowVersion == sqliteNowVersion) {
                "Runtime artifact version '${runtimeArtifact.sqliteNowVersion}' is not compatible with " +
                    "requested SQLiteNow version '$sqliteNowVersion'."
            }
        }
    }
}

data class SwiftPackageToolsMetadata(
    val gradleVersion: String? = null,
    val xcodebuildPath: String? = null,
    val xcodebuildVersion: String? = null,
) {
    companion object {
        fun discover(gradleVersion: String? = null): SwiftPackageToolsMetadata {
            val xcodebuild = findExecutableOnPath("xcodebuild")
            return SwiftPackageToolsMetadata(
                gradleVersion = gradleVersion,
                xcodebuildPath = xcodebuild?.absolutePath,
                xcodebuildVersion = xcodebuild?.let { runAndCapture(it, "-version") },
            )
        }
    }
}

enum class SwiftPackageCleanPolicy {
    REQUIRE_BUILD_DIRECTORY,
    REQUIRE_GENERATED_MARKER_OR_EMPTY,
}

data class SwiftPackageAssemblerInput(
    val databaseName: String,
    val swiftPackageName: String,
    val swiftTargetName: String,
    val runtimeMode: SwiftProductRuntimeMode,
    val runtimeModuleName: String = runtimeMode.defaultRuntimeModuleName(),
    val frameworkMode: String = SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC,
    val requestedAppleTargets: List<String> = DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS,
    val minimumPlatforms: SwiftPackageMinimumPlatforms = SwiftPackageMinimumPlatforms(),
    val sqliteNowVersion: String,
    val generatorVersion: String = sqliteNowVersion,
    val generatedBy: String,
    val generatedSwiftSourceDirectory: File,
    val runtimeXcframeworkDirectory: File? = null,
    val runtimeArtifact: SwiftPackageRuntimeArtifact? = null,
    val sqlInputFiles: List<File>,
    val sourceDigestBaseDirectory: File,
    val packageRootDirectory: File,
    val metadataBaseDirectory: File,
    val tools: SwiftPackageToolsMetadata = SwiftPackageToolsMetadata.discover(),
    val cleanPolicy: SwiftPackageCleanPolicy = SwiftPackageCleanPolicy.REQUIRE_GENERATED_MARKER_OR_EMPTY,
    val forbiddenTokenPatterns: List<String> = SwiftPackageLeakChecker.DEFAULT_FORBIDDEN_REGEX_PATTERNS,
    val syncTables: List<SwiftSyncTable> = emptyList(),
)

data class SwiftPackageAssemblerResult(
    val packageRootDirectory: File,
    val packageSwiftFile: File,
    val metadataManifestFile: File,
    val generatedFiles: List<File>,
    val packagedSwiftFiles: List<String>,
    val packagedSwiftFilePaths: List<File>,
    val runtimeArtifactPaths: List<String>,
    val runtimeArtifactAbsolutePaths: List<File>,
    val runtimeArtifactKind: SwiftPackageRuntimeArtifactKind,
    val runtimeArtifactChecksum: String?,
    val runtimeArtifactVersion: String?,
    val runtimeArtifactUrl: String?,
    val sourceInputDigest: String,
    val generatorInputs: List<String>,
    val syncTables: List<SwiftSyncTable>,
)

data class SwiftSyncTable(
    val tableName: String,
    val syncKeyColumnName: String,
)

object SwiftPackageAssembler {
    private val json = Json { prettyPrint = true }

    fun assemble(input: SwiftPackageAssemblerInput): SwiftPackageAssemblerResult {
        val normalizedInput = input.copy(
            frameworkMode = requireSupportedSwiftPackageFrameworkMode(input.frameworkMode),
            requestedAppleTargets = requireSupportedSwiftPackageAppleTargets(input.requestedAppleTargets),
            forbiddenTokenPatterns = input.forbiddenTokenPatterns
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .distinct(),
        )
        val runtimeArtifact = resolveSwiftPackageRuntimeArtifact(
            runtimeXcframeworkDirectory = normalizedInput.runtimeXcframeworkDirectory,
            runtimeArtifact = normalizedInput.runtimeArtifact,
        )
        normalizedInput.validate(runtimeArtifact)

        val generatedSourceDir = normalizedInput.generatedSwiftSourceDirectory.canonicalFile
        val packageRoot = normalizedInput.packageRootDirectory.canonicalFile
        val runtimeSource = runtimeArtifact.file?.canonicalFile
        val metadataBaseDirectory = normalizedInput.metadataBaseDirectory.canonicalFile
        val sourceDigestBaseDirectory = normalizedInput.sourceDigestBaseDirectory.canonicalFile

        requireNoGeneratedSwiftPackageOverlap(generatedSourceDir, packageRoot)
        requireSafeGeneratedPackageDirectory(
            packageRoot = packageRoot,
            swiftPackageName = normalizedInput.swiftPackageName,
            cleanPolicy = normalizedInput.cleanPolicy,
        )

        packageRoot.deleteRecursively()
        val sourcesDir = packageRoot.resolve("Sources/${normalizedInput.swiftTargetName}").apply { mkdirs() }
        val manifestDir = packageRoot.resolve(".sqlitenow").apply { mkdirs() }
        val runtimeTarget = runtimeArtifact.packagedFileName(normalizedInput.runtimeModuleName)?.let { packagedFileName ->
            packageRoot.resolve("Binaries").resolve(packagedFileName)
        }
        val runtimeArtifactPaths = listOfNotNull(runtimeTarget?.relativePathFrom(packageRoot))
        val runtimeArtifactAbsolutePaths = listOfNotNull(runtimeTarget)

        copyGeneratedSwiftSources(generatedSourceDir, sourcesDir)
        if (runtimeSource != null && runtimeTarget != null) {
            copyRuntimeArtifact(runtimeArtifact, runtimeSource, runtimeTarget)
        }

        val packageSwiftFile = packageRoot.resolve("Package.swift").apply {
            writeText(swiftPackageManifest(normalizedInput, runtimeArtifact, runtimeArtifactPaths.singleOrNull()))
        }

        val syncTables = if (normalizedInput.runtimeMode == SwiftProductRuntimeMode.SYNC) {
            normalizedInput.syncTables
        } else {
            emptyList()
        }
        require(normalizedInput.runtimeMode != SwiftProductRuntimeMode.SYNC || syncTables.isNotEmpty()) {
            "Swift sync package assembly requires structured sync table metadata."
        }

        val generatorConfigInputs = swiftPackageGeneratorConfigInputs(
            databaseName = normalizedInput.databaseName,
            swiftTargetName = normalizedInput.swiftTargetName,
            runtimeMode = normalizedInput.runtimeMode,
            runtimeModuleName = normalizedInput.runtimeModuleName,
            frameworkMode = normalizedInput.frameworkMode,
            minimumPlatforms = normalizedInput.minimumPlatforms,
            requestedAppleTargets = normalizedInput.requestedAppleTargets,
            runtimeArtifactKind = runtimeArtifact.kind,
            runtimeArtifactChecksum = runtimeArtifact.checksum,
            runtimeArtifactVersion = runtimeArtifact.sqliteNowVersion,
            runtimeArtifactUrl = runtimeArtifact.url,
            forbiddenTokenPatterns = normalizedInput.forbiddenTokenPatterns,
            syncTables = syncTables,
        )
        val sqlInputFiles = normalizedInput.sqlInputFiles
            .filter { it.isFile }
            .sortedBy { it.relativePathFrom(sourceDigestBaseDirectory) }
        val generatorInputs = sqlInputFiles.map { it.relativePathFrom(sourceDigestBaseDirectory) } + generatorConfigInputs
        val packagedSwiftFiles = sourcesDir.walkTopDown()
            .filter { it.isFile }
            .map { it.relativePathFrom(packageRoot) }
            .sorted()
            .toList()
        val packagedSwiftFilePaths = packagedSwiftFiles.map { packageRoot.resolve(it) }
        SwiftPackageLeakChecker.requireNoLeaks(
            files = packagedSwiftFilePaths,
            rootDirectory = packageRoot,
            forbiddenRegexPatterns = normalizedInput.forbiddenTokenPatterns,
            messagePrefix = "Generated Swift package leaked forbidden support tokens:",
        )
        val sourceInputDigest = swiftPackageSourceInputDigest(
            baseDir = sourceDigestBaseDirectory,
            files = sqlInputFiles,
            logicalInputs = generatorConfigInputs,
        )

        val metadataManifestFile = manifestDir.resolve("package-manifest.json").apply {
            writeText(
                packageMetadataManifest(
                    input = normalizedInput,
                    packageRoot = packageRoot,
                    runtimeArtifactPaths = runtimeArtifactPaths,
                    generatedSourceDir = generatedSourceDir,
                    generatedSwiftFiles = packagedSwiftFiles,
                    sourceInputDigest = sourceInputDigest,
                    generatorInputs = generatorInputs,
                    syncTables = syncTables,
                    runtimeArtifact = runtimeArtifact,
                    metadataBaseDirectory = metadataBaseDirectory,
                )
            )
        }

        val generatedFiles = packageRoot.walkTopDown()
            .filter { it.isFile }
            .sortedBy { it.relativePathFrom(packageRoot) }
            .toList()

        return SwiftPackageAssemblerResult(
            packageRootDirectory = packageRoot,
            packageSwiftFile = packageSwiftFile,
            metadataManifestFile = metadataManifestFile,
            generatedFiles = generatedFiles,
            packagedSwiftFiles = packagedSwiftFiles,
            packagedSwiftFilePaths = packagedSwiftFilePaths,
            runtimeArtifactPaths = runtimeArtifactPaths,
            runtimeArtifactAbsolutePaths = runtimeArtifactAbsolutePaths,
            runtimeArtifactKind = runtimeArtifact.kind,
            runtimeArtifactChecksum = runtimeArtifact.checksum,
            runtimeArtifactVersion = runtimeArtifact.sqliteNowVersion,
            runtimeArtifactUrl = runtimeArtifact.url,
            sourceInputDigest = sourceInputDigest,
            generatorInputs = generatorInputs,
            syncTables = syncTables,
        )
    }

    private fun SwiftPackageAssemblerInput.validate(runtimeArtifact: SwiftPackageRuntimeArtifact) {
        requireValidSwiftIdentifier(databaseName, "Swift package databaseName")
        requireValidSwiftPackageName(swiftPackageName, "Swift package swiftPackageName")
        requireValidSwiftIdentifier(swiftTargetName, "Swift package swiftTargetName")
        requireValidSwiftIdentifier(runtimeModuleName, "Swift package runtimeModuleName")
        require(frameworkMode.isNotBlank()) { "Swift package frameworkMode must not be blank." }
        require(requestedAppleTargets.isNotEmpty()) { "Swift package requestedAppleTargets must not be empty." }
        require(sqliteNowVersion.isNotBlank()) { "Swift package sqliteNowVersion must not be blank." }
        require(generatorVersion.isNotBlank()) { "Swift package generatorVersion must not be blank." }
        require(generatedBy.isNotBlank()) { "Swift package generatedBy must not be blank." }

        val sourceFile = generatedSwiftSourceDirectory.resolve("$databaseName.swift")
        require(sourceFile.isFile) {
            "Expected generated Swift source at ${sourceFile.absolutePath}."
        }
        requireValidSwiftPackageRuntimeArtifact(
            runtimeArtifact = runtimeArtifact,
            context = SwiftPackageRuntimeArtifactValidationContext(
                runtimeModuleName = runtimeModuleName,
                sqliteNowVersion = sqliteNowVersion,
                generatorVersion = generatorVersion,
            ),
        )
    }

    private fun requireNoGeneratedSwiftPackageOverlap(
        generatedSourceDir: File,
        packageRoot: File,
    ) {
        require(
            !generatedSourceDir.isDescendantOrEqual(packageRoot) &&
                !packageRoot.isDescendantOrEqual(generatedSourceDir)
        ) {
            "Generated Swift source directory and package output directory must not overlap: " +
                "source=${generatedSourceDir.absolutePath}, package=${packageRoot.absolutePath}."
        }
    }

    private fun copyGeneratedSwiftSources(
        generatedSourceDir: File,
        sourcesDir: File,
    ) {
        generatedSourceDir.walkTopDown()
            .filter { it.isFile && it.extension == "swift" }
            .forEach { sourceFile ->
                val target = sourcesDir.resolve(sourceFile.relativePathFrom(generatedSourceDir))
                target.parentFile.mkdirs()
                Files.copy(sourceFile.toPath(), target.toPath(), REPLACE_EXISTING, COPY_ATTRIBUTES)
            }
    }

    private fun requireSafeGeneratedPackageDirectory(
        packageRoot: File,
        swiftPackageName: String,
        cleanPolicy: SwiftPackageCleanPolicy,
    ) {
        require(packageRoot.name == swiftPackageName) {
            "Swift package output directory must end with package name '$swiftPackageName': $packageRoot"
        }
        require(!packageRoot.exists() || packageRoot.isDirectory) {
            "Swift package output path exists but is not a directory: $packageRoot"
        }

        when (cleanPolicy) {
            SwiftPackageCleanPolicy.REQUIRE_BUILD_DIRECTORY -> {
                val segments = packageRoot.toPath().map { it.toString() }.toList()
                require("build" in segments) {
                    "Refusing to clean generated Swift package outside a build directory: $packageRoot"
                }
            }

            SwiftPackageCleanPolicy.REQUIRE_GENERATED_MARKER_OR_EMPTY -> {
                if (!packageRoot.exists() || packageRoot.listFiles().orEmpty().isEmpty()) {
                    return
                }
                val manifestFile = packageRoot.resolve(".sqlitenow/package-manifest.json")
                val existingPackageName = runCatching {
                    json.parseToJsonElement(manifestFile.readText())
                        .jsonObject["packageName"]
                        ?.jsonPrimitive
                        ?.content
                }.getOrNull()
                if (existingPackageName == null && packageRoot.hasGeneratedOutputSegment()) {
                    return
                }
                require(existingPackageName == swiftPackageName) {
                    "Refusing to clean existing Swift package output without SQLiteNow package metadata for " +
                        "'$swiftPackageName': $packageRoot"
                }
            }
        }
    }

    private fun swiftPackageManifest(
        input: SwiftPackageAssemblerInput,
        runtimeArtifact: SwiftPackageRuntimeArtifact,
        runtimeArtifactPath: String?,
    ): String = buildString {
        val minimumIos = swiftPackagePlatformVersion(input.minimumPlatforms.ios, "iOS")
        val minimumMacos = swiftPackagePlatformVersion(input.minimumPlatforms.macos, "macOS")
        appendLine("// swift-tools-version: 6.0")
        appendLine()
        appendLine("import PackageDescription")
        appendLine()
        appendLine("let package = Package(")
        appendLine("    name: ${swiftStringLiteral(input.swiftPackageName)},")
        appendLine("    platforms: [")
        appendLine("        .iOS(.v$minimumIos),")
        appendLine("        .macOS(.v$minimumMacos),")
        appendLine("    ],")
        appendLine("    products: [")
        appendLine("        .library(")
        appendLine("            name: ${swiftStringLiteral(input.swiftPackageName)},")
        appendLine("            targets: [${swiftStringLiteral(input.swiftTargetName)}]")
        appendLine("        ),")
        appendLine("    ],")
        appendLine("    targets: [")
        when (runtimeArtifact.kind) {
            SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK,
            SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> {
                require(!runtimeArtifactPath.isNullOrBlank()) {
                    "Local runtime artifacts require a packaged artifact path."
                }
                appendLine("        .binaryTarget(")
                appendLine("            name: ${swiftStringLiteral(input.runtimeModuleName)},")
                appendLine("            path: ${swiftStringLiteral(runtimeArtifactPath)}")
                appendLine("        ),")
            }

            SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> {
                appendLine("        .binaryTarget(")
                appendLine("            name: ${swiftStringLiteral(input.runtimeModuleName)},")
                appendLine("            url: ${swiftStringLiteral(runtimeArtifact.url.orEmpty())},")
                appendLine("            checksum: ${swiftStringLiteral(runtimeArtifact.checksum.orEmpty())}")
                appendLine("        ),")
            }
        }
        appendLine("        .target(")
        appendLine("            name: ${swiftStringLiteral(input.swiftTargetName)},")
        appendLine("            dependencies: [${swiftStringLiteral(input.runtimeModuleName)}]")
        appendLine("        ),")
        appendLine("    ]")
        appendLine(")")
    }

    private fun swiftStringLiteral(value: String): String =
        buildString {
            append('"')
            value.forEach { char ->
                when (char) {
                    '\\' -> append("\\\\")
                    '"' -> append("\\\"")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    '\t' -> append("\\t")
                    else -> append(char)
                }
            }
            append('"')
        }

    private fun swiftPackagePlatformVersion(version: String, platformName: String): String {
        require(Regex("""[1-9]\d*""").matches(version)) {
            "Swift package minimum $platformName version must be a positive integer: $version"
        }
        return version
    }

    private fun File.isDescendantOrEqual(parent: File): Boolean =
        toPath().normalize().startsWith(parent.toPath().normalize())

    private fun File.hasGeneratedOutputSegment(): Boolean {
        val segments = toPath().normalize().map { it.toString().lowercase() }.toList()
        return segments.any { segment ->
            segment == "build" ||
                segment == ".build" ||
                segment == "sqlitenowgenerated" ||
                "generated" in segment
        }
    }

    private fun packageMetadataManifest(
        input: SwiftPackageAssemblerInput,
        packageRoot: File,
        runtimeArtifactPaths: List<String>,
        generatedSourceDir: File,
        generatedSwiftFiles: List<String>,
        sourceInputDigest: String,
        generatorInputs: List<String>,
        syncTables: List<SwiftSyncTable>,
        runtimeArtifact: SwiftPackageRuntimeArtifact,
        metadataBaseDirectory: File,
    ): String {
        val manifest = buildJsonObject {
            put("manifestVersion", SWIFT_PACKAGE_MANIFEST_VERSION)
            put("sqliteNowVersion", input.sqliteNowVersion)
            put("generatorVersion", input.generatorVersion)
            put("databaseName", input.databaseName)
            put("packageName", input.swiftPackageName)
            put("swiftTargetName", input.swiftTargetName)
            put("generatedPackagePath", packageRoot.relativePathFrom(metadataBaseDirectory))
            put("runtimeMode", input.runtimeMode.id)
            put("runtimeBinaryTargets", buildJsonArray { add(JsonPrimitive(input.runtimeModuleName)) })
            put("runtimeArtifactKind", runtimeArtifact.kind.id)
            put("runtimeArtifactPaths", stringArray(runtimeArtifactPaths))
            putNullableString("runtimeArtifactSourcePath", runtimeArtifact.file?.relativePathFrom(metadataBaseDirectory))
            putNullableString("runtimeArtifactChecksum", runtimeArtifact.checksum)
            putNullableString(
                "runtimeArtifactChecksumAlgorithm",
                runtimeArtifact.checksum?.let { "swiftpm-sha256" },
            )
            putNullableString("runtimeArtifactVersion", runtimeArtifact.sqliteNowVersion)
            putNullableString("runtimeArtifactUrl", runtimeArtifact.url)
            put("requestedAppleTargets", stringArray(input.requestedAppleTargets))
            put(
                "minimumPlatforms",
                buildJsonObject {
                    put("iOS", input.minimumPlatforms.ios)
                    put("macOS", input.minimumPlatforms.macos)
                }
            )
            put("frameworkMode", input.frameworkMode)
            if (syncTables.isNotEmpty()) {
                put(
                    "syncTables",
                    buildJsonArray {
                        syncTables.forEach { table ->
                            add(
                                buildJsonObject {
                                    put("tableName", table.tableName)
                                    put("syncKeyColumnName", table.syncKeyColumnName)
                                }
                            )
                        }
                    }
                )
            }
            put("generatedSwiftSourcePath", generatedSourceDir.relativePathFrom(metadataBaseDirectory))
            put("generatedSwiftFiles", stringArray(generatedSwiftFiles))
            put("sourceInputDigest", sourceInputDigest)
            put("generatorInputs", stringArray(generatorInputs))
            put("generatedBy", input.generatedBy)
        }
        return json.encodeToString(manifest) + "\n"
    }

    private fun JsonObjectBuilder.stringArray(values: List<String>) =
        buildJsonArray {
            values.forEach { add(JsonPrimitive(it)) }
        }

    private fun SwiftPackageRuntimeArtifact.packagedFileName(runtimeModuleName: String): String? =
        when (kind) {
            SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK -> "$runtimeModuleName.xcframework"
            SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> requireFile().name
            SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> null
        }

    private fun copyRuntimeArtifact(
        runtimeArtifact: SwiftPackageRuntimeArtifact,
        source: File,
        target: File,
    ) {
        when (runtimeArtifact.kind) {
            SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK -> copyRuntimeArtifactPreservingAttributes(source, target)
            SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> {
                target.parentFile.mkdirs()
                Files.copy(source.toPath(), target.toPath(), REPLACE_EXISTING, COPY_ATTRIBUTES)
            }
            SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> {
                error("Remote ZIP runtime artifacts are not copied into generated packages.")
            }
        }
    }

    private fun copyRuntimeArtifactPreservingAttributes(source: File, target: File) {
        val sourcePath = source.toPath()
        val targetPath = target.toPath()
        Files.walkFileTree(
            sourcePath,
            object : SimpleFileVisitor<Path>() {
                override fun preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult {
                    Files.createDirectories(targetPath.resolve(sourcePath.relativize(dir)))
                    return FileVisitResult.CONTINUE
                }

                override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                    val targetFile = targetPath.resolve(sourcePath.relativize(file))
                    Files.createDirectories(targetFile.parent)
                    if (Files.isSymbolicLink(file)) {
                        Files.deleteIfExists(targetFile)
                        Files.createSymbolicLink(targetFile, Files.readSymbolicLink(file))
                    } else {
                        Files.copy(file, targetFile, REPLACE_EXISTING, COPY_ATTRIBUTES)
                        if (file.toFile().canExecute() && !targetFile.toFile().canExecute()) {
                            targetFile.toFile().setExecutable(true, false)
                        }
                    }
                    return FileVisitResult.CONTINUE
                }
            }
        )
    }

}

private fun SwiftPackageRuntimeArtifact.requireFile(): File =
    file ?: error("Runtime artifact path is required for ${kind.id} runtime artifacts.")

private fun validateRuntimeArtifactFileName(fileName: String, runtimeModuleName: String) {
    require(fileName.endsWith(".xcframework.zip")) {
        "Runtime artifact zip must end with .xcframework.zip: $fileName"
    }
    val exactName = "$runtimeModuleName.xcframework.zip"
    val versionedNamePrefix = "$runtimeModuleName-"
    require(fileName == exactName || fileName.startsWith(versionedNamePrefix)) {
        "Runtime artifact zip '$fileName' does not match runtime module '$runtimeModuleName'."
    }
}

private fun validateRuntimeArtifactUrl(url: String, runtimeModuleName: String) {
    val uri = runCatching { URI(url) }.getOrElse {
        error("Remote runtime artifact URL is invalid: $url")
    }
    require(uri.scheme == "https") {
        "Remote runtime artifact URL must use https: $url"
    }
    val fileName = File(uri.path.orEmpty()).name
    require(fileName.isNotBlank()) {
        "Remote runtime artifact URL must include an artifact file name: $url"
    }
    validateRuntimeArtifactFileName(fileName, runtimeModuleName)
}

internal fun JsonObjectBuilder.putNullableString(key: String, value: String?) {
    if (value == null) {
        put(key, JsonNull)
    } else {
        put(key, value)
    }
}

fun sha256(file: File): String {
    val digest = MessageDigest.getInstance("SHA-256")
    file.inputStream().buffered().use { input ->
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
            val read = input.read(buffer)
            if (read < 0) {
                break
            }
            digest.update(buffer, 0, read)
        }
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
}

fun swiftPackageGeneratorConfigInputs(
    databaseName: String,
    swiftTargetName: String,
    runtimeMode: SwiftProductRuntimeMode,
    runtimeModuleName: String,
    frameworkMode: String,
    minimumPlatforms: SwiftPackageMinimumPlatforms,
    requestedAppleTargets: List<String>,
    runtimeArtifactKind: SwiftPackageRuntimeArtifactKind = SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK,
    runtimeArtifactChecksum: String? = null,
    runtimeArtifactVersion: String? = null,
    runtimeArtifactUrl: String? = null,
    forbiddenTokenPatterns: List<String> = SwiftPackageLeakChecker.DEFAULT_FORBIDDEN_REGEX_PATTERNS,
    syncTables: List<SwiftSyncTable> = emptyList(),
): List<String> =
    buildList {
        val normalizedForbiddenTokenPatterns = forbiddenTokenPatterns
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .distinct()

        add("databaseName=$databaseName")
        add("swiftTargetName=$swiftTargetName")
        add("runtimeMode=${runtimeMode.id}")
        add("runtimeBinaryTarget=$runtimeModuleName")
        add("runtimeArtifactKind=${runtimeArtifactKind.id}")
        runtimeArtifactChecksum?.let { add("runtimeArtifactChecksum=$it") }
        runtimeArtifactVersion?.let { add("runtimeArtifactVersion=$it") }
        runtimeArtifactUrl?.let { add("runtimeArtifactUrl=$it") }
        add("frameworkMode=$frameworkMode")
        add("minimumPlatforms=iOS${minimumPlatforms.ios},macOS${minimumPlatforms.macos}")
        add("requestedAppleTargets=${requestedAppleTargets.joinToString(",")}")
        normalizedForbiddenTokenPatterns.forEachIndexed { index, pattern ->
            add("forbiddenTokenPattern[$index]=$pattern")
        }
        if (syncTables.isNotEmpty()) {
            add("syncTables=${syncTables.joinToString(",") { "${it.tableName}:${it.syncKeyColumnName}" }}")
        }
    }

fun swiftPackageSourceInputDigest(
    baseDir: File,
    files: List<File>,
    logicalInputs: List<String> = emptyList(),
): String {
    val digest = MessageDigest.getInstance("SHA-256")
    val normalizedBaseDir = baseDir.canonicalFile
    files.forEach { file ->
        digest.update(file.relativePathFrom(normalizedBaseDir).toByteArray())
        digest.update(0)
        digest.update(file.readBytes())
        digest.update(0)
    }
    logicalInputs.forEach { input ->
        digest.update(input.toByteArray())
        digest.update(0)
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
}

fun parseSwiftPackageSyncTables(generatedSourceDir: File): List<SwiftSyncTable> {
    val syncTableRegex = Regex(
        """SQLiteNowSyncRuntimeTableSpec\(tableName:\s*"([^"]+)",\s*syncKeyColumnName:\s*"([^"]+)"\)"""
    )
    return generatedSourceDir.walkTopDown()
        .filter { it.isFile && it.extension == "swift" }
        .flatMap { file ->
            syncTableRegex.findAll(file.readText()).map { match ->
                SwiftSyncTable(
                    tableName = match.groupValues[1],
                    syncKeyColumnName = match.groupValues[2],
                )
            }
        }
        .distinct()
        .toList()
}

fun findExecutableOnPath(name: String): File? =
    System.getenv("PATH")
        .orEmpty()
        .split(File.pathSeparator)
        .asSequence()
        .filter { it.isNotBlank() }
        .map { File(it, name) }
        .firstOrNull { it.isFile && it.canExecute() }

private fun runAndCapture(executable: File, vararg args: String): String {
    val process = ProcessBuilder(executable.absolutePath, *args)
        .redirectErrorStream(true)
        .start()
    val output = process.inputStream.bufferedReader().readText()
    val exitCode = process.waitFor()
    require(exitCode == 0) {
        "${executable.name} ${args.joinToString(" ")} failed with exit code $exitCode:\n$output"
    }
    return output.trim()
}

private fun File.relativePathFrom(baseDir: File): String =
    runCatching {
        canonicalFile.normalize().relativeTo(baseDir.canonicalFile.normalize()).invariantSeparatorsPath
    }.getOrElse {
        absoluteFile.normalize().invariantSeparatorsPath
    }
