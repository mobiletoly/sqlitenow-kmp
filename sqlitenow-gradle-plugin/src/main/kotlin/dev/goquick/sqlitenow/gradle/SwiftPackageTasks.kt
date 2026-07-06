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
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.swift.SWIFT_PACKAGE_MANIFEST_VERSION
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageAssembler
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageAssemblerInput
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageCleanPolicy
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageMinimumPlatforms
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageRuntimeArtifact
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageRuntimeArtifactKind
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageToolsMetadata
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import dev.goquick.sqlitenow.gradle.swift.SwiftSyncTable
import dev.goquick.sqlitenow.gradle.swift.requireSupportedSwiftPackageFrameworkMode
import dev.goquick.sqlitenow.gradle.swift.swiftPackageGeneratorConfigInputs
import dev.goquick.sqlitenow.gradle.swift.swiftPackageSourceInputDigest
import groovy.json.JsonSlurper
import java.io.File
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

abstract class VerifySwiftProductSourceTask : DefaultTask() {
    @get:Input
    abstract val databaseName: Property<String>

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val generatedSwiftSourceDirectory: DirectoryProperty

    @TaskAction
    fun verify() {
        val sourceFile = generatedSwiftSourceDirectory.get().asFile.resolve("${databaseName.get()}.swift")
        require(sourceFile.isFile) {
            "Expected generated Swift product source at ${sourceFile.absolutePath}."
        }
    }
}

abstract class PackageSwiftPackageTask @Inject constructor(
    objects: ObjectFactory,
) : DefaultTask() {
    @get:Input
    abstract val databaseName: Property<String>

    @get:Input
    abstract val swiftPackageName: Property<String>

    @get:Input
    abstract val swiftTargetName: Property<String>

    @get:Input
    abstract val runtimeMode: Property<String>

    @get:Input
    abstract val runtimeModuleName: Property<String>

    @get:Input
    abstract val frameworkMode: Property<String>

    @get:Input
    abstract val forbiddenTokenPatterns: ListProperty<String>

    @get:Input
    abstract val requestedAppleTargets: ListProperty<String>

    @get:Input
    abstract val minimumIos: Property<String>

    @get:Input
    abstract val minimumMacos: Property<String>

    @get:Input
    abstract val sqliteNowVersion: Property<String>

    @get:Input
    abstract val generatedBy: Property<String>

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val generatedSwiftSourceDirectory: DirectoryProperty

    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val swiftProductMetadataFile: RegularFileProperty

    @get:Input
    abstract val runtimeArtifactKind: Property<String>

    @get:InputDirectory
    @get:Optional
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val runtimeXcframeworkDirectory: DirectoryProperty

    @get:InputFile
    @get:Optional
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val runtimeZipFile: RegularFileProperty

    @get:Input
    @get:Optional
    abstract val runtimeArtifactUrl: Property<String>

    @get:Input
    @get:Optional
    abstract val runtimeArtifactChecksum: Property<String>

    @get:Input
    @get:Optional
    abstract val runtimeArtifactVersion: Property<String>

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    val sqlFiles: ConfigurableFileCollection = objects.fileCollection()

    @get:OutputDirectory
    abstract val swiftPackageOutputDirectory: DirectoryProperty

    @TaskAction
    fun packageSwiftPackage() {
        val packageOutput = swiftPackageOutputDirectory.get().asFile
        val resolvedFrameworkMode = requireSupportedSwiftPackageFrameworkMode(frameworkMode.get())
        val parsedRuntimeMode = SwiftProductRuntimeMode.fromId(runtimeMode.get())
        val swiftProductMetadata = readSwiftProductMetadata(swiftProductMetadataFile.get().asFile)
        val result = SwiftPackageAssembler.assemble(
            SwiftPackageAssemblerInput(
                databaseName = databaseName.get(),
                swiftPackageName = swiftPackageName.get(),
                swiftTargetName = swiftTargetName.get(),
                runtimeMode = parsedRuntimeMode,
                runtimeModuleName = runtimeModuleName.get(),
                frameworkMode = resolvedFrameworkMode,
                requestedAppleTargets = requestedAppleTargets.get(),
                minimumPlatforms = SwiftPackageMinimumPlatforms(
                    ios = minimumIos.get(),
                    macos = minimumMacos.get(),
                ),
                sqliteNowVersion = sqliteNowVersion.get(),
                generatedBy = generatedBy.get(),
                generatedSwiftSourceDirectory = generatedSwiftSourceDirectory.get().asFile,
                runtimeArtifact = runtimeArtifact(sqliteNowVersion.get()),
                sqlInputFiles = sortedSqlFiles(),
                sourceDigestBaseDirectory = project.projectDir,
                packageRootDirectory = packageOutput,
                metadataBaseDirectory = project.rootDir,
                tools = SwiftPackageToolsMetadata.discover(gradleVersion = project.gradle.gradleVersion),
                cleanPolicy = SwiftPackageCleanPolicy.REQUIRE_BUILD_DIRECTORY,
                forbiddenTokenPatterns = forbiddenTokenPatterns.get(),
                syncTables = if (parsedRuntimeMode == SwiftProductRuntimeMode.SYNC) {
                    swiftProductMetadata.syncTables
                } else {
                    emptyList()
                },
            )
        )

        logger.lifecycle("Generated Swift package at ${result.packageRootDirectory.absolutePath}")
    }

    private fun sortedSqlFiles(): List<File> =
        sqlFiles.files
            .filter { it.isFile }
            .sortedBy { it.relativeTo(project.projectDir).invariantSeparatorsPath }

    private fun runtimeArtifact(defaultVersion: String): SwiftPackageRuntimeArtifact =
        runtimeArtifactFromTaskInputs(
            kind = runtimeArtifactKind.get(),
            runtimeModuleName = runtimeModuleName.get(),
            runtimeXcframeworkDirectory = optionalFile(runtimeXcframeworkDirectory),
            runtimeZipFile = optionalFile(runtimeZipFile),
            runtimeArtifactUrl = runtimeArtifactUrl.orNull,
            runtimeArtifactChecksum = runtimeArtifactChecksum.orNull,
            runtimeArtifactVersion = runtimeArtifactVersion.orNull,
            defaultVersion = defaultVersion,
        )
}

abstract class ValidateSwiftPackageManifestTask @Inject constructor(
    objects: ObjectFactory,
) : DefaultTask() {
    @get:Input
    abstract val databaseName: Property<String>

    @get:Input
    abstract val swiftPackageName: Property<String>

    @get:Input
    abstract val swiftTargetName: Property<String>

    @get:Input
    abstract val runtimeMode: Property<String>

    @get:Input
    abstract val runtimeModuleName: Property<String>

    @get:Input
    abstract val frameworkMode: Property<String>

    @get:Input
    abstract val forbiddenTokenPatterns: ListProperty<String>

    @get:Input
    abstract val requestedAppleTargets: ListProperty<String>

    @get:Input
    abstract val minimumIos: Property<String>

    @get:Input
    abstract val minimumMacos: Property<String>

    @get:Input
    abstract val sqliteNowVersion: Property<String>

    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val manifestFile: org.gradle.api.file.RegularFileProperty

    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val generatedSwiftSourceDirectory: DirectoryProperty

    @get:InputFile
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val swiftProductMetadataFile: RegularFileProperty

    @get:Input
    abstract val runtimeArtifactKind: Property<String>

    @get:InputDirectory
    @get:Optional
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val runtimeXcframeworkDirectory: DirectoryProperty

    @get:InputFile
    @get:Optional
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val runtimeZipFile: RegularFileProperty

    @get:Input
    @get:Optional
    abstract val runtimeArtifactUrl: Property<String>

    @get:Input
    @get:Optional
    abstract val runtimeArtifactChecksum: Property<String>

    @get:Input
    @get:Optional
    abstract val runtimeArtifactVersion: Property<String>

    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    val sqlFiles: ConfigurableFileCollection = objects.fileCollection()

    @TaskAction
    fun validate() {
        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(manifestFile.get().asFile) as Map<String, Any?>
        val parsedRuntimeMode = SwiftProductRuntimeMode.fromId(runtimeMode.get())
        val resolvedFrameworkMode = requireSupportedSwiftPackageFrameworkMode(frameworkMode.get())
        val runtimeArtifact = runtimeArtifact(sqliteNowVersion.get())
        val expectedSyncTables = if (runtimeMode.get() == "sync") {
            readSwiftProductMetadata(swiftProductMetadataFile.get().asFile).syncTables
        } else {
            emptyList()
        }
        val minimumPlatforms = SwiftPackageMinimumPlatforms(
            ios = minimumIos.get(),
            macos = minimumMacos.get(),
        )
        val generatorConfigInputs = swiftPackageGeneratorConfigInputs(
            databaseName = databaseName.get(),
            swiftTargetName = swiftTargetName.get(),
            runtimeMode = parsedRuntimeMode,
            runtimeModuleName = runtimeModuleName.get(),
            frameworkMode = resolvedFrameworkMode,
            minimumPlatforms = minimumPlatforms,
            requestedAppleTargets = requestedAppleTargets.get(),
            runtimeArtifactKind = runtimeArtifact.kind,
            runtimeArtifactChecksum = runtimeArtifact.checksum,
            runtimeArtifactVersion = runtimeArtifact.sqliteNowVersion,
            runtimeArtifactUrl = runtimeArtifact.url,
            forbiddenTokenPatterns = forbiddenTokenPatterns.get(),
            syncTables = expectedSyncTables,
        )
        val sqlInputFiles = sqlFiles.files
            .filter { it.isFile }
            .sortedBy { it.relativeTo(project.projectDir).invariantSeparatorsPath }

        requireManifestNumber(manifest, "manifestVersion", SWIFT_PACKAGE_MANIFEST_VERSION)
        requireManifestString(manifest, "sqliteNowVersion", sqliteNowVersion.get())
        requireManifestString(manifest, "generatorVersion", sqliteNowVersion.get())
        requireManifestString(manifest, "databaseName", databaseName.get())
        requireManifestString(manifest, "packageName", swiftPackageName.get())
        requireManifestString(manifest, "swiftTargetName", swiftTargetName.get())
        requireManifestString(manifest, "runtimeMode", runtimeMode.get())
        requireManifestStringList(manifest, "runtimeBinaryTargets", listOf(runtimeModuleName.get()))
        requireManifestString(manifest, "runtimeArtifactKind", runtimeArtifact.kind.id)
        requireManifestStringList(
            manifest,
            "runtimeArtifactPaths",
            expectedRuntimeArtifactPaths(runtimeArtifact, runtimeModuleName.get()),
        )
        requireManifestOptionalString(manifest, "runtimeArtifactChecksum", runtimeArtifact.checksum)
        requireManifestOptionalString(manifest, "runtimeArtifactVersion", runtimeArtifact.sqliteNowVersion)
        requireManifestOptionalString(manifest, "runtimeArtifactUrl", runtimeArtifact.url)
        requireManifestStringList(manifest, "requestedAppleTargets", requestedAppleTargets.get())
        requireManifestString(manifest, "frameworkMode", resolvedFrameworkMode)
        requireManifestString(
            manifest,
            "sourceInputDigest",
            swiftPackageSourceInputDigest(project.projectDir, sqlInputFiles, generatorConfigInputs),
        )
        requireManifestStringList(
            manifest,
            "generatorInputs",
            sqlInputFiles.map { it.relativeTo(project.projectDir).invariantSeparatorsPath } + generatorConfigInputs,
        )

        val manifestMinimumPlatforms = manifest["minimumPlatforms"] as? Map<*, *>
            ?: error("Expected manifest minimumPlatforms object.")
        require(manifestMinimumPlatforms["iOS"] == minimumPlatforms.ios) {
            "Expected manifest minimumPlatforms.iOS=${minimumPlatforms.ios}, got ${manifestMinimumPlatforms["iOS"]}."
        }
        require(manifestMinimumPlatforms["macOS"] == minimumPlatforms.macos) {
            "Expected manifest minimumPlatforms.macOS=${minimumPlatforms.macos}, got ${manifestMinimumPlatforms["macOS"]}."
        }

        val generatedSwiftFiles = (manifest["generatedSwiftFiles"] as? List<*>)?.map { it as? String }
            ?: error("Expected manifest generatedSwiftFiles array.")
        val expectedSwiftFile = "Sources/${swiftTargetName.get()}/${databaseName.get()}.swift"
        require(expectedSwiftFile in generatedSwiftFiles) {
            "Expected generatedSwiftFiles to include $expectedSwiftFile, got $generatedSwiftFiles."
        }
        require(generatedSwiftFiles.size > 1) {
            "Expected generatedSwiftFiles to include split Swift source files, got $generatedSwiftFiles."
        }

        if (runtimeMode.get() == "sync") {
            val actualSyncTables = (manifest["syncTables"] as? List<*>)
                ?.map { table ->
                    val tableMap = table as? Map<*, *> ?: error("Expected sync table object, got $table.")
                    SwiftSyncTable(
                        tableName = tableMap["tableName"] as? String ?: error("Expected sync tableName."),
                        syncKeyColumnName = tableMap["syncKeyColumnName"] as? String
                            ?: error("Expected syncKeyColumnName."),
                    )
                }
                ?: error("Expected manifest syncTables array.")
            require(actualSyncTables == expectedSyncTables) {
                "Expected manifest syncTables=$expectedSyncTables, got $actualSyncTables."
            }
        }
    }

    private fun runtimeArtifact(defaultVersion: String): SwiftPackageRuntimeArtifact =
        runtimeArtifactFromTaskInputs(
            kind = runtimeArtifactKind.get(),
            runtimeModuleName = runtimeModuleName.get(),
            runtimeXcframeworkDirectory = optionalFile(runtimeXcframeworkDirectory),
            runtimeZipFile = optionalFile(runtimeZipFile),
            runtimeArtifactUrl = runtimeArtifactUrl.orNull,
            runtimeArtifactChecksum = runtimeArtifactChecksum.orNull,
            runtimeArtifactVersion = runtimeArtifactVersion.orNull,
            defaultVersion = defaultVersion,
        )
}

abstract class CheckSwiftPackageLeaksTask @Inject constructor(
    objects: ObjectFactory,
) : DefaultTask() {
    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val sourceDirectory: DirectoryProperty

    @get:Input
    abstract val forbiddenTokenPatterns: ListProperty<String>

    @TaskAction
    fun check() {
        val sourceDir = sourceDirectory.get().asFile
        val swiftFiles = sourceDir.walkTopDown()
            .filter { it.isFile && it.extension == "swift" }
            .asIterable()
        SwiftPackageLeakChecker.requireNoLeaks(
            files = swiftFiles,
            rootDirectory = sourceDir.parentFile,
            forbiddenRegexPatterns = forbiddenTokenPatterns.get(),
            messagePrefix = "Generated Swift package leaked forbidden support tokens:",
        )
    }
}

private fun requireManifestNumber(manifest: Map<String, Any?>, key: String, expected: Int) {
    val actual = (manifest[key] as? Number)?.toInt()
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

private fun requireManifestString(manifest: Map<String, Any?>, key: String, expected: String) {
    val actual = manifest[key] as? String
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

private fun requireManifestStringList(manifest: Map<String, Any?>, key: String, expected: List<String>) {
    val actual = (manifest[key] as? List<*>)?.map { it as? String }
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

private fun requireManifestOptionalString(manifest: Map<String, Any?>, key: String, expected: String?) {
    val actual = manifest[key] as? String
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

private data class SwiftProductMetadata(
    val syncTables: List<SwiftSyncTable>,
)

private fun readSwiftProductMetadata(file: File): SwiftProductMetadata {
    @Suppress("UNCHECKED_CAST")
    val metadata = JsonSlurper().parse(file) as Map<String, Any?>
    val syncTables = (metadata["syncTables"] as? List<*>)
        ?.map { table ->
            val tableMap = table as? Map<*, *> ?: error("Expected sync table object in ${file.absolutePath}.")
            SwiftSyncTable(
                tableName = tableMap["tableName"] as? String ?: error("Expected sync tableName in ${file.absolutePath}."),
                syncKeyColumnName = tableMap["syncKeyColumnName"] as? String
                    ?: error("Expected syncKeyColumnName in ${file.absolutePath}."),
            )
        }
        ?: emptyList()
    return SwiftProductMetadata(syncTables = syncTables)
}

private fun runtimeArtifactFromTaskInputs(
    kind: String,
    runtimeModuleName: String,
    runtimeXcframeworkDirectory: File?,
    runtimeZipFile: File?,
    runtimeArtifactUrl: String?,
    runtimeArtifactChecksum: String?,
    runtimeArtifactVersion: String?,
    defaultVersion: String,
): SwiftPackageRuntimeArtifact =
    when (SwiftPackageRuntimeArtifactKind.fromId(kind)) {
        SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK -> {
            require(runtimeXcframeworkDirectory != null) {
                "Swift package runtime artifact '$kind' requires runtimeXcframework."
            }
            SwiftPackageRuntimeArtifact.localXcframework(runtimeXcframeworkDirectory)
        }
        SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> {
            require(runtimeZipFile != null) {
                "Swift package runtime artifact '$kind' requires runtimeArtifact.localZipFile."
            }
            SwiftPackageRuntimeArtifact.localZip(
                file = runtimeZipFile,
                checksum = runtimeArtifactChecksum,
                sqliteNowVersion = runtimeArtifactVersion ?: defaultVersion,
            )
        }
        SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> {
            SwiftPackageRuntimeArtifact.remoteZip(
                url = runtimeArtifactUrl.orEmpty(),
                checksum = runtimeArtifactChecksum,
                sqliteNowVersion = runtimeArtifactVersion ?: defaultVersion,
            )
        }
    }

private fun expectedRuntimeArtifactPaths(
    runtimeArtifact: SwiftPackageRuntimeArtifact,
    runtimeModuleName: String,
): List<String> =
    when (runtimeArtifact.kind) {
        SwiftPackageRuntimeArtifactKind.LOCAL_XCFRAMEWORK -> listOf("Binaries/$runtimeModuleName.xcframework")
        SwiftPackageRuntimeArtifactKind.LOCAL_ZIP -> listOf("Binaries/${runtimeArtifact.requireFile().name}")
        SwiftPackageRuntimeArtifactKind.REMOTE_ZIP -> emptyList()
    }

private fun SwiftPackageRuntimeArtifact.requireFile(): File =
    file ?: error("Swift package runtime artifact '${kind.id}' requires a file.")

private fun optionalFile(property: DirectoryProperty): File? =
    if (property.isPresent) property.get().asFile else null

private fun optionalFile(property: RegularFileProperty): File? =
    if (property.isPresent) property.get().asFile else null
