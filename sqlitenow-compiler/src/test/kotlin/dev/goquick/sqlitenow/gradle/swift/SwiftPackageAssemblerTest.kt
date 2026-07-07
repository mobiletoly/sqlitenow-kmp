package dev.goquick.sqlitenow.gradle.swift

import dev.goquick.sqlitenow.gradle.SwiftPackageLeakChecker
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SwiftPackageAssemblerTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun assemblerBuildsCorePackageAndMetadata() {
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "CorePackageDatabase",
            source = """
                @preconcurrency import SQLiteNowCoreRuntime

                public enum CorePackageDatabase {}
            """.trimIndent(),
            extraSources = mapOf(
                "CorePackageDatabaseAdapters.swift" to """
                    @preconcurrency import SQLiteNowCoreRuntime
                    import Foundation

                    public struct CorePackageDatabaseAdapters {}
                """.trimIndent(),
            ),
        )
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME)
        val sqlFiles = createSqlInputs("CorePackageDatabase")
        val packageDir = tempDir.resolve("SQLiteNowGenerated/CorePackageDatabase-SQLiteNow")

        val input = assemblerInput(
            databaseName = "CorePackageDatabase",
            packageName = "CorePackageDatabase-SQLiteNow",
            targetName = "CorePackageDatabaseSQLiteNow",
            runtimeMode = SwiftProductRuntimeMode.CORE,
            sourceDir = sourceDir,
            runtimeDir = runtimeDir,
            sqlFiles = sqlFiles,
            packageDir = packageDir,
        )

        val result = SwiftPackageAssembler.assemble(input)

        assertTrue(packageDir.resolve("Package.swift").isFile)
        assertTrue(packageDir.resolve("Sources/CorePackageDatabaseSQLiteNow/CorePackageDatabase.swift").isFile)
        assertTrue(packageDir.resolve("Sources/CorePackageDatabaseSQLiteNow/CorePackageDatabaseAdapters.swift").isFile)
        assertTrue(packageDir.resolve("Binaries/SQLiteNowCoreRuntime.xcframework/Info.plist").isFile)
        assertTrue(
            packageDir
                .resolve("Binaries/SQLiteNowCoreRuntime.xcframework/macos-arm64/SQLiteNowCoreRuntime.framework/SQLiteNowCoreRuntime")
                .canExecute(),
            "Copied runtime framework binary must retain executable permissions",
        )
        assertEquals(
            listOf(
                "Sources/CorePackageDatabaseSQLiteNow/CorePackageDatabase.swift",
                "Sources/CorePackageDatabaseSQLiteNow/CorePackageDatabaseAdapters.swift",
            ),
            result.packagedSwiftFiles,
        )
        assertEquals(listOf("Binaries/SQLiteNowCoreRuntime.xcframework"), result.runtimeArtifactPaths)

        val packageSwift = result.packageSwiftFile.readText()
        assertTrue(packageSwift.contains("""name: "CorePackageDatabase-SQLiteNow""""))
        assertTrue(packageSwift.contains("""name: "SQLiteNowCoreRuntime""""))

        val manifest = Json.parseToJsonElement(result.metadataManifestFile.readText()).jsonObject
        assertEquals(3, manifest.getValue("manifestVersion").jsonPrimitive.content.toInt())
        assertEquals("CorePackageDatabase", manifest.getValue("databaseName").jsonPrimitive.content)
        assertEquals("1.2.3", manifest.getValue("generatorVersion").jsonPrimitive.content)
        assertEquals("CorePackageDatabase-SQLiteNow", manifest.getValue("packageName").jsonPrimitive.content)
        assertEquals("core", manifest.getValue("runtimeMode").jsonPrimitive.content)
        assertEquals(
            "SQLiteNowGenerated/CorePackageDatabase-SQLiteNow",
            manifest.getValue("generatedPackagePath").jsonPrimitive.content,
        )
        assertEquals(listOf("SQLiteNowCoreRuntime"), manifest.stringArray("runtimeBinaryTargets"))
        assertEquals("localXcframework", manifest.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(listOf("Binaries/SQLiteNowCoreRuntime.xcframework"), manifest.stringArray("runtimeArtifactPaths"))
        assertEquals(result.packagedSwiftFiles, manifest.stringArray("generatedSwiftFiles"))
        assertFalse(manifest.containsKey("syncTables"))

        val expectedGeneratorConfigInputs = swiftPackageGeneratorConfigInputs(
            databaseName = "CorePackageDatabase",
            swiftTargetName = "CorePackageDatabaseSQLiteNow",
            runtimeMode = SwiftProductRuntimeMode.CORE,
            runtimeModuleName = DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME,
            frameworkMode = "dynamic",
            minimumPlatforms = SwiftPackageMinimumPlatforms(),
            requestedAppleTargets = DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS,
        )
        val expectedGeneratorInputs = sqlFiles.map { it.relativeTo(tempDir).invariantSeparatorsPath } +
            expectedGeneratorConfigInputs
        assertEquals(expectedGeneratorInputs, manifest.stringArray("generatorInputs"))
        assertEquals(
            swiftPackageSourceInputDigest(tempDir, sqlFiles, expectedGeneratorConfigInputs),
            manifest.getValue("sourceInputDigest").jsonPrimitive.content,
        )
    }

    @Test
    fun assemblerUsesConfiguredForbiddenTokenPatterns() {
        val databaseName = "DataFlowDatabase"
        val sourceDir = createGeneratedSwiftSource(
            databaseName = databaseName,
            source = """
                @preconcurrency import SQLiteNowCoreRuntime

                public struct DataFlowRow: Equatable, Sendable {
                    public let id: Int64
                }
            """.trimIndent(),
        )
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME)
        val sqlFiles = createSqlInputs(databaseName)
        val packageDir = tempDir.resolve("SQLiteNowGenerated/${databaseName}SQLiteNow")

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = databaseName,
                packageName = "${databaseName}SQLiteNow",
                targetName = "${databaseName}SQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.CORE,
                sourceDir = sourceDir,
                runtimeDir = runtimeDir,
                sqlFiles = sqlFiles,
                packageDir = packageDir,
                forbiddenTokenPatterns = listOf("Flow<"),
            )
        )

        assertTrue(
            result.packagedSwiftFilePaths
                .single { it.name == "$databaseName.swift" }
                .readText()
                .contains("DataFlowRow"),
        )
        assertTrue(result.generatorInputs.contains("forbiddenTokenPattern[0]=Flow<"))
    }

    @Test
    fun assemblerBuildsPackageWithLocalRuntimeZipArtifact() {
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "ZipPackageDatabase",
            source = """
                @preconcurrency import SQLiteNowCoreRuntime

                public enum ZipPackageDatabase {}
            """.trimIndent(),
        )
        val runtimeZip = createRuntimeZip(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME, version = "1.2.3")
        val checksum = sha256(runtimeZip)
        val sqlFiles = createSqlInputs("ZipPackageDatabase")
        val packageDir = tempDir.resolve("SQLiteNowGenerated/ZipPackageDatabaseSQLiteNow")

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = "ZipPackageDatabase",
                packageName = "ZipPackageDatabaseSQLiteNow",
                targetName = "ZipPackageDatabaseSQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.CORE,
                sourceDir = sourceDir,
                runtimeDir = null,
                runtimeArtifact = SwiftPackageRuntimeArtifact.localZip(
                    file = runtimeZip,
                    checksum = checksum,
                    sqliteNowVersion = "1.2.3",
                ),
                sqlFiles = sqlFiles,
                packageDir = packageDir,
            )
        )

        assertEquals(listOf("Binaries/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"), result.runtimeArtifactPaths)
        assertEquals(SwiftPackageRuntimeArtifactKind.LOCAL_ZIP, result.runtimeArtifactKind)
        assertEquals(checksum, result.runtimeArtifactChecksum)
        assertEquals("1.2.3", result.runtimeArtifactVersion)
        assertTrue(packageDir.resolve("Binaries/SQLiteNowCoreRuntime-1.2.3.xcframework.zip").isFile)

        val packageSwift = result.packageSwiftFile.readText()
        assertTrue(packageSwift.contains("""path: "Binaries/SQLiteNowCoreRuntime-1.2.3.xcframework.zip""""))

        val manifest = Json.parseToJsonElement(result.metadataManifestFile.readText()).jsonObject
        assertEquals("localZip", manifest.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(checksum, manifest.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals("swiftpm-sha256", manifest.getValue("runtimeArtifactChecksumAlgorithm").jsonPrimitive.content)
        assertEquals("1.2.3", manifest.getValue("runtimeArtifactVersion").jsonPrimitive.content)
        assertEquals(
            listOf("Binaries/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"),
            manifest.stringArray("runtimeArtifactPaths"),
        )
    }

    @Test
    fun assemblerBuildsPackageWithRemoteRuntimeZipArtifact() {
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "RemoteZipPackageDatabase",
            source = """
                @preconcurrency import SQLiteNowCoreRuntime

                public enum RemoteZipPackageDatabase {}
            """.trimIndent(),
        )
        val checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        val runtimeUrl =
            "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"
        val sqlFiles = createSqlInputs("RemoteZipPackageDatabase")
        val packageDir = tempDir.resolve("SQLiteNowGenerated/RemoteZipPackageDatabaseSQLiteNow")

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = "RemoteZipPackageDatabase",
                packageName = "RemoteZipPackageDatabaseSQLiteNow",
                targetName = "RemoteZipPackageDatabaseSQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.CORE,
                sourceDir = sourceDir,
                runtimeDir = null,
                runtimeArtifact = SwiftPackageRuntimeArtifact.remoteZip(
                    url = runtimeUrl,
                    checksum = checksum,
                    sqliteNowVersion = "1.2.3",
                ),
                sqlFiles = sqlFiles,
                packageDir = packageDir,
            )
        )

        assertEquals(emptyList(), result.runtimeArtifactPaths)
        assertEquals(emptyList(), result.runtimeArtifactAbsolutePaths)
        assertEquals(SwiftPackageRuntimeArtifactKind.REMOTE_ZIP, result.runtimeArtifactKind)
        assertEquals(checksum, result.runtimeArtifactChecksum)
        assertEquals("1.2.3", result.runtimeArtifactVersion)
        assertEquals(runtimeUrl, result.runtimeArtifactUrl)
        assertFalse(packageDir.resolve("Binaries").exists())

        val packageSwift = result.packageSwiftFile.readText()
        assertTrue(packageSwift.contains("""url: "$runtimeUrl""""))
        assertTrue(packageSwift.contains("""checksum: "$checksum""""))
        assertFalse(packageSwift.contains("path:"))

        val manifest = Json.parseToJsonElement(result.metadataManifestFile.readText()).jsonObject
        assertEquals("remoteZip", manifest.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(emptyList(), manifest.stringArray("runtimeArtifactPaths"))
        assertEquals(JsonNull, manifest.getValue("runtimeArtifactSourcePath"))
        assertEquals(checksum, manifest.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals("swiftpm-sha256", manifest.getValue("runtimeArtifactChecksumAlgorithm").jsonPrimitive.content)
        assertEquals("1.2.3", manifest.getValue("runtimeArtifactVersion").jsonPrimitive.content)
        assertEquals(runtimeUrl, manifest.getValue("runtimeArtifactUrl").jsonPrimitive.content)
    }

    @Test
    fun assemblerRecordsSqliteNowPackageDependencyInMetadata() {
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "ExternalSupportPackageDatabase",
            source = """
                @preconcurrency import SQLiteNowCoreRuntime
                @_exported import SQLiteNowCoreSupport

                public enum ExternalSupportPackageDatabase {}
            """.trimIndent(),
        )
        val runtimeZip = createRuntimeZip(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME, version = "1.2.3")
        val sqlFiles = createSqlInputs("ExternalSupportPackageDatabase")
        val packageDir = tempDir.resolve("SQLiteNowGenerated/ExternalSupportPackageDatabaseSQLiteNow")
        val sqliteNowPackage = SwiftPackageDependency(
            kind = SwiftPackageDependencyKind.REMOTE_EXACT,
            packageIdentity = "sqlitenow-kmp",
            url = "https://github.com/mobiletoly/sqlitenow-kmp.git",
            version = "1.2.3",
            coreRuntimeProduct = "CustomCoreRuntime",
            syncRuntimeProduct = "CustomSyncRuntime",
            coreSupportProduct = "CustomCoreSupport",
            syncSupportProduct = "CustomSyncSupport",
        )

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = "ExternalSupportPackageDatabase",
                packageName = "ExternalSupportPackageDatabaseSQLiteNow",
                targetName = "ExternalSupportPackageDatabaseSQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.CORE,
                sourceDir = sourceDir,
                runtimeDir = null,
                runtimeArtifact = SwiftPackageRuntimeArtifact.localZip(
                    file = runtimeZip,
                    checksum = sha256(runtimeZip),
                    sqliteNowVersion = "1.2.3",
                ),
                sqliteNowPackage = sqliteNowPackage,
                sqlFiles = sqlFiles,
                packageDir = packageDir,
            )
        )

        assertEquals(emptyList(), result.runtimeArtifactPaths)
        assertFalse(packageDir.resolve("Binaries").exists())

        val packageSwift = result.packageSwiftFile.readText()
        assertTrue(packageSwift.contains("""url: "https://github.com/mobiletoly/sqlitenow-kmp.git""""))
        assertTrue(packageSwift.contains("""exact: "1.2.3""""))
        assertTrue(packageSwift.contains(""".product(name: "CustomCoreRuntime", package: "sqlitenow-kmp")"""))
        assertTrue(packageSwift.contains(""".product(name: "CustomCoreSupport", package: "sqlitenow-kmp")"""))

        val manifest = Json.parseToJsonElement(result.metadataManifestFile.readText()).jsonObject
        val manifestPackage = manifest.getValue("sqliteNowPackage").jsonObject
        assertEquals("remoteExact", manifestPackage.getValue("kind").jsonPrimitive.content)
        assertEquals("sqlitenow-kmp", manifestPackage.getValue("packageIdentity").jsonPrimitive.content)
        assertEquals(
            "https://github.com/mobiletoly/sqlitenow-kmp.git",
            manifestPackage.getValue("url").jsonPrimitive.content,
        )
        assertEquals("1.2.3", manifestPackage.getValue("version").jsonPrimitive.content)
        assertEquals("CustomCoreRuntime", manifestPackage.getValue("coreRuntimeProduct").jsonPrimitive.content)
        assertEquals("CustomSyncRuntime", manifestPackage.getValue("syncRuntimeProduct").jsonPrimitive.content)
        assertEquals("CustomCoreSupport", manifestPackage.getValue("coreSupportProduct").jsonPrimitive.content)
        assertEquals("CustomSyncSupport", manifestPackage.getValue("syncSupportProduct").jsonPrimitive.content)
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("sqliteNowPackage.url=https://github.com/mobiletoly/sqlitenow-kmp.git"),
        )
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("sqliteNowPackage.coreRuntimeProduct=CustomCoreRuntime"),
        )
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("sqliteNowPackage.syncRuntimeProduct=CustomSyncRuntime"),
        )
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("sqliteNowPackage.coreSupportProduct=CustomCoreSupport"),
        )
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("sqliteNowPackage.syncSupportProduct=CustomSyncSupport"),
        )
    }

    @Test
    fun assemblerRejectsInvalidSwiftIdentifiers() {
        val databaseName = "EscapedPackageDatabase"

        val packageFailure = assertAssembleFails(databaseName = databaseName) {
            copy(
                swiftPackageName = "Escaped/PackageSQLiteNow",
                packageRootDirectory = tempDir.resolve("SQLiteNowGenerated/Escaped/PackageSQLiteNow"),
            )
        }
        assertTrue(packageFailure.message!!.contains("Swift package swiftPackageName must not contain path separators"))

        val targetFailure = assertAssembleFails(databaseName = databaseName) {
            copy(swiftTargetName = "1InvalidTarget")
        }
        assertTrue(targetFailure.message!!.contains("Swift package swiftTargetName must be a valid Swift identifier"))

        val runtimeFailure = assertAssembleFails(databaseName = databaseName) {
            copy(runtimeModuleName = "SQLiteNowCore Runtime")
        }
        assertTrue(runtimeFailure.message!!.contains("Swift package runtimeModuleName must be a valid Swift identifier"))
    }

    @Test
    fun assemblerEscapesPackageSwiftStringLiteralsForRemoteRuntimeArtifact() {
        val databaseName = "RemoteEscapedPackageDatabase"
        val sourceDir = createGeneratedSwiftSource(
            databaseName = databaseName,
            source = """
                @preconcurrency import SQLiteNowCoreRuntime

                public enum RemoteEscapedPackageDatabase {}
            """.trimIndent(),
        )
        val checksum = "aa\"bb\\cc\tdd"
        val runtimeUrl =
            "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v1.2.3/SQLiteNowCoreRuntime-1.2.3.xcframework.zip"
        val sqlFiles = createSqlInputs(databaseName)
        val packageDir = tempDir.resolve("SQLiteNowGenerated/RemoteEscapedPackageDatabaseSQLiteNow")

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = databaseName,
                packageName = "RemoteEscapedPackageDatabaseSQLiteNow",
                targetName = "RemoteEscapedPackageDatabaseSQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.CORE,
                sourceDir = sourceDir,
                runtimeDir = null,
                runtimeArtifact = SwiftPackageRuntimeArtifact.remoteZip(
                    url = runtimeUrl,
                    checksum = checksum,
                    sqliteNowVersion = "1.2.3",
                ),
                sqlFiles = sqlFiles,
                packageDir = packageDir,
            )
        )

        val packageSwift = result.packageSwiftFile.readText()
        assertTrue(packageSwift.contains("            url: ${expectedSwiftStringLiteral(runtimeUrl)},"))
        assertTrue(packageSwift.contains("            checksum: ${expectedSwiftStringLiteral(checksum)}"))
    }

    @Test
    fun assemblerBuildsSyncPackageWithSyncTableMetadata() {
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "SyncPackageDatabase",
            source = """
                @preconcurrency import SQLiteNowSyncRuntime

                public let syncTable = SQLiteNowSyncRuntimeTableSpec(tableName: "docs", syncKeyColumnName: "doc_id")
            """.trimIndent(),
        )
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_SYNC_RUNTIME_MODULE_NAME)
        val sqlFiles = createSqlInputs("SyncPackageDatabase")
        val packageDir = tempDir.resolve("SQLiteNowGenerated/SyncPackageDatabaseSQLiteNow")

        val result = SwiftPackageAssembler.assemble(
            assemblerInput(
                databaseName = "SyncPackageDatabase",
                packageName = "SyncPackageDatabaseSQLiteNow",
                targetName = "SyncPackageDatabaseSQLiteNow",
                runtimeMode = SwiftProductRuntimeMode.SYNC,
                sourceDir = sourceDir,
                runtimeDir = runtimeDir,
                sqlFiles = sqlFiles,
                packageDir = packageDir,
                syncTables = listOf(SwiftSyncTable(tableName = "docs", syncKeyColumnName = "doc_id")),
            )
        )

        assertEquals(
            listOf(SwiftSyncTable(tableName = "docs", syncKeyColumnName = "doc_id")),
            result.syncTables,
        )
        assertTrue(packageDir.resolve("Binaries/SQLiteNowSyncRuntime.xcframework/Info.plist").isFile)

        val manifest = Json.parseToJsonElement(result.metadataManifestFile.readText()).jsonObject
        assertEquals("sync", manifest.getValue("runtimeMode").jsonPrimitive.content)
        assertEquals(listOf("SQLiteNowSyncRuntime"), manifest.stringArray("runtimeBinaryTargets"))
        val syncTable = manifest.getValue("syncTables").jsonArray.single().jsonObject
        assertEquals("docs", syncTable.getValue("tableName").jsonPrimitive.content)
        assertEquals("doc_id", syncTable.getValue("syncKeyColumnName").jsonPrimitive.content)
        assertTrue(
            manifest.stringArray("generatorInputs")
                .contains("syncTables=docs:doc_id"),
        )
    }

    @Test
    fun assemblerRejectsSyncPackageWithoutStructuredSyncTableMetadata() {
        val failure = assertAssembleFails(
            databaseName = "MissingSyncMetadataDatabase",
            runtimeMode = SwiftProductRuntimeMode.SYNC,
            source = """
                @preconcurrency import SQLiteNowSyncRuntime

                public let syncTable = SQLiteNowSyncRuntimeTableSpec(tableName: "docs", syncKeyColumnName: "doc_id")
            """.trimIndent(),
        )

        assertTrue(failure.message!!.contains("structured sync table metadata"))
    }

    @Test
    fun assemblerRejectsExistingOutputWithoutGeneratedMetadata() {
        val packageDir = tempDir.resolve("UserOwnedOutput/UnsafeDatabaseSQLiteNow").apply {
            mkdirs()
            resolve("user-owned.txt").writeText("not generated")
        }

        val failure = assertAssembleFails(databaseName = "UnsafeDatabase", packageDir = packageDir)

        assertNotNull(failure.message)
        assertTrue(failure.message!!.contains("Refusing to clean existing Swift package output"))
        assertTrue(packageDir.resolve("user-owned.txt").isFile)
    }

    @Test
    fun assemblerRejectsGeneratedSourceInsidePackageOutput() {
        val packageDir = tempDir.resolve("SQLiteNowGenerated/OverlapDatabaseSQLiteNow")
        val sourceDir = createGeneratedSwiftSource(
            databaseName = "OverlapDatabase",
            source = "public enum OverlapDatabase {}",
            root = packageDir.resolve("IntermediateSource"),
        )
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME)

        val failure = assertFailsWith<IllegalArgumentException> {
            SwiftPackageAssembler.assemble(
                assemblerInput(
                    databaseName = "OverlapDatabase",
                    packageName = "OverlapDatabaseSQLiteNow",
                    targetName = "OverlapDatabaseSQLiteNow",
                    runtimeMode = SwiftProductRuntimeMode.CORE,
                    sourceDir = sourceDir,
                    runtimeDir = runtimeDir,
                    sqlFiles = createSqlInputs("OverlapDatabase"),
                    packageDir = packageDir,
                )
            )
        }

        assertTrue(failure.message!!.contains("Generated Swift source directory and package output directory must not overlap"))
    }

    @Test
    fun assemblerRejectsPackageOutputInsideGeneratedSource() {
        val sourceDir = createGeneratedSwiftSource("ParentOverlapDatabase", "public enum ParentOverlapDatabase {}")
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME)
        val packageDir = sourceDir.resolve("SQLiteNowGenerated/ParentOverlapDatabaseSQLiteNow")

        val failure = assertFailsWith<IllegalArgumentException> {
            SwiftPackageAssembler.assemble(
                assemblerInput(
                    databaseName = "ParentOverlapDatabase",
                    packageName = "ParentOverlapDatabaseSQLiteNow",
                    targetName = "ParentOverlapDatabaseSQLiteNow",
                    runtimeMode = SwiftProductRuntimeMode.CORE,
                    sourceDir = sourceDir,
                    runtimeDir = runtimeDir,
                    sqlFiles = createSqlInputs("ParentOverlapDatabase"),
                    packageDir = packageDir,
                )
            )
        }

        assertTrue(failure.message!!.contains("Generated Swift source directory and package output directory must not overlap"))
    }

    @Test
    fun assemblerCanRegenerateExistingGeneratedPackage() {
        val sourceDir = createGeneratedSwiftSource("RegenerateDatabase", "public enum RegenerateDatabase {}")
        val runtimeDir = createRuntimeXcframework(DEFAULT_SWIFT_CORE_RUNTIME_MODULE_NAME)
        val packageDir = tempDir.resolve("SQLiteNowGenerated/RegenerateDatabaseSQLiteNow")
        val input = assemblerInput(
            databaseName = "RegenerateDatabase",
            packageName = "RegenerateDatabaseSQLiteNow",
            targetName = "RegenerateDatabaseSQLiteNow",
            runtimeMode = SwiftProductRuntimeMode.CORE,
            sourceDir = sourceDir,
            runtimeDir = runtimeDir,
            sqlFiles = createSqlInputs("RegenerateDatabase"),
            packageDir = packageDir,
        )

        SwiftPackageAssembler.assemble(input)
        val staleFile = packageDir.resolve("Sources/RegenerateDatabaseSQLiteNow/stale.swift").apply {
            writeText("stale")
        }

        SwiftPackageAssembler.assemble(input)

        assertFalse(staleFile.exists())
        assertTrue(packageDir.resolve("Sources/RegenerateDatabaseSQLiteNow/RegenerateDatabase.swift").isFile)
    }

    @Test
    fun assemblerRejectsUnsupportedFrameworkMode() {
        val failure = assertAssembleFails(databaseName = "FrameworkModeDatabase") {
            copy(frameworkMode = "static")
        }

        assertTrue(failure.message!!.contains("Only 'dynamic' is supported"))
    }

    @Test
    fun assemblerRejectsInvalidMinimumPlatformVersionsBeforeWritingPackageSwift() {
        val failure = assertAssembleFails(databaseName = "PlatformDatabase") {
            copy(minimumPlatforms = SwiftPackageMinimumPlatforms(ios = "0", macos = "014"))
        }

        assertTrue(failure.message!!.contains("minimum iOS version must be a positive integer"))
    }

    @Test
    fun assemblerRejectsUnsupportedAppleTargetsBeforeWritingPackageSwift() {
        val failure = assertAssembleFails(databaseName = "AppleTargetDatabase") {
            copy(requestedAppleTargets = listOf("macosArm64", "iosX64"))
        }

        assertTrue(failure.message!!.contains("unsupported Apple target(s): iosX64"))
        assertTrue(failure.message!!.contains("x86_64 Apple slices are not supported"))
    }

    private fun assertAssembleFails(
        databaseName: String,
        runtimeMode: SwiftProductRuntimeMode = SwiftProductRuntimeMode.CORE,
        source: String = "public enum $databaseName {}",
        packageDir: File = tempDir.resolve("SQLiteNowGenerated/${databaseName}SQLiteNow"),
        configure: SwiftPackageAssemblerInput.() -> SwiftPackageAssemblerInput = { this },
    ): IllegalArgumentException {
        val sourceDir = createGeneratedSwiftSource(databaseName, source)
        val runtimeDir = createRuntimeXcframework(runtimeMode.defaultRuntimeModuleName())

        return assertFailsWith {
            SwiftPackageAssembler.assemble(
                assemblerInput(
                    databaseName = databaseName,
                    packageName = "${databaseName}SQLiteNow",
                    targetName = "${databaseName}SQLiteNow",
                    runtimeMode = runtimeMode,
                    sourceDir = sourceDir,
                    runtimeDir = runtimeDir,
                    sqlFiles = createSqlInputs(databaseName),
                    packageDir = packageDir,
                ).configure()
            )
        }
    }

    private fun assemblerInput(
        databaseName: String,
        packageName: String,
        targetName: String,
        runtimeMode: SwiftProductRuntimeMode,
        sourceDir: File,
        runtimeDir: File?,
        runtimeArtifact: SwiftPackageRuntimeArtifact? = null,
        sqlFiles: List<File>,
        packageDir: File,
        runtimeModuleName: String = runtimeMode.defaultRuntimeModuleName(),
        frameworkMode: String = SWIFT_PACKAGE_FRAMEWORK_MODE_DYNAMIC,
        minimumPlatforms: SwiftPackageMinimumPlatforms = SwiftPackageMinimumPlatforms(),
        requestedAppleTargets: List<String> = DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS,
        forbiddenTokenPatterns: List<String> = SwiftPackageLeakChecker.DEFAULT_FORBIDDEN_REGEX_PATTERNS,
        sqliteNowPackage: SwiftPackageDependency? = null,
        syncTables: List<SwiftSyncTable> = emptyList(),
    ): SwiftPackageAssemblerInput =
        SwiftPackageAssemblerInput(
            databaseName = databaseName,
            swiftPackageName = packageName,
            swiftTargetName = targetName,
            runtimeMode = runtimeMode,
            runtimeModuleName = runtimeModuleName,
            frameworkMode = frameworkMode,
            generatedSwiftSourceDirectory = sourceDir,
            runtimeXcframeworkDirectory = runtimeDir,
            runtimeArtifact = runtimeArtifact,
            minimumPlatforms = minimumPlatforms,
            requestedAppleTargets = requestedAppleTargets,
            sqlInputFiles = sqlFiles,
            sourceDigestBaseDirectory = tempDir,
            packageRootDirectory = packageDir,
            metadataBaseDirectory = tempDir,
            sqliteNowVersion = "1.2.3",
            generatedBy = "test",
            tools = SwiftPackageToolsMetadata(),
            forbiddenTokenPatterns = forbiddenTokenPatterns,
            sqliteNowPackage = sqliteNowPackage,
            syncTables = syncTables,
        )

    private fun createGeneratedSwiftSource(
        databaseName: String,
        source: String,
        extraSources: Map<String, String> = emptyMap(),
        root: File = tempDir.resolve("generated-swift/$databaseName"),
    ): File {
        val sourceDir = root.apply { mkdirs() }
        sourceDir.resolve("$databaseName.swift").writeText(source)
        extraSources.forEach { (fileName, fileSource) ->
            sourceDir.resolve(fileName).writeText(fileSource)
        }
        return sourceDir
    }

    private fun createRuntimeXcframework(moduleName: String): File =
        tempDir.resolve("runtime/$moduleName.xcframework").apply {
            mkdirs()
            resolve("Info.plist").writeText("fake runtime")
            resolve("macos-arm64/$moduleName.framework/$moduleName").apply {
                parentFile.mkdirs()
                writeText("fake runtime binary")
                assertTrue(setExecutable(true, false), "Test fixture must be able to mark runtime binary executable")
            }
        }

    private fun createRuntimeZip(moduleName: String, version: String): File =
        tempDir.resolve("runtime/$moduleName-$version.xcframework.zip").apply {
            parentFile.mkdirs()
            writeText("fake zipped runtime for $moduleName $version")
        }

    private fun createSqlInputs(databaseName: String): List<File> {
        val sqlDir = tempDir.resolve("sql/$databaseName")
        val schemaFile = sqlDir.resolve("schema/person.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent()
            )
        }
        val queryFile = sqlDir.resolve("queries/person/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    SELECT id, name
                    FROM person;
                """.trimIndent()
            )
        }
        return listOf(schemaFile, queryFile).sortedBy { it.relativeTo(tempDir).invariantSeparatorsPath }
    }

    private fun expectedSwiftStringLiteral(value: String): String =
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

    private fun kotlinx.serialization.json.JsonObject.stringArray(name: String): List<String> =
        getValue(name).jsonArray.map { it.jsonPrimitive.content }
}
