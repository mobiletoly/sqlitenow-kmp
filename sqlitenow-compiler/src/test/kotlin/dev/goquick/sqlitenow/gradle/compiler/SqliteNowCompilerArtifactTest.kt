package dev.goquick.sqlitenow.gradle.compiler

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.jar.JarFile
import java.util.zip.ZipFile
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import dev.goquick.sqlitenow.gradle.swift.sha256
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir

class SqliteNowCompilerArtifactTest {

    @TempDir
    lateinit var tempDir: File

    private data class CompilerRun(
        val exitCode: Int,
        val stdout: String,
        val stderr: String,
        val response: JsonObject,
    )

    private data class FailureScenario(
        val name: String,
        val request: JsonObject,
        val expectedMessage: String,
        val expectedContractVersion: Int = 2,
    )

    @Test
    fun compilerArtifactRunsWithoutGradlePlugin() {
        val sqlDir = tempDir.resolve("sql/ArtifactDatabase")
        sqlDir.resolve("schema/person.sql").apply {
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
        sqlDir.resolve("queries/person/selectById.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    SELECT *
                    FROM person
                    WHERE id = :id;
                """.trimIndent()
            )
        }

        val outputDir = tempDir.resolve("generated")
        val requestFile = tempDir.resolve("request.json").apply {
            writeText(
                buildJsonObject {
                    put("databaseName", "ArtifactDatabase")
                    put("sqlDirectory", sqlDir.absolutePath)
                    put("packageName", "com.test.artifact")
                    put("outputDirectory", outputDir.absolutePath)
                    put("schemaDatabaseFile", JsonNull)
                    put("debug", false)
                    put("oversqlite", false)
                }.toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val response = run.response
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(1, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertTrue(response.getValue("warnings").jsonArray.isEmpty())
        assertFalse(response.containsKey("generatedSwiftFiles"))
        assertTrue(
            response.getValue("generatedFiles").jsonArray.any {
                it.jsonPrimitive.content.endsWith("/ArtifactDatabase.kt")
            },
            "Response should list generated database source. stdout:\n${run.stdout}"
        )
        assertTrue(
            outputDir.resolve("com/test/artifact/ArtifactDatabase.kt").isFile,
            "Compiler artifact should write Kotlin output without applying the Gradle plugin"
        )
        assertFalse(
            tempDir.resolve("null").exists(),
            "JSON null for schemaDatabaseFile must not create a file named 'null'"
        )
        assertNotNull(response.getValue("java").jsonObject["minimumMajorVersion"])
    }

    @Test
    fun compilerArtifactPreservesLegacyDartRequestCompatibility() {
        val sqlDir = createLegacyDartSql("LegacyDartDatabase")
        val outputDir = tempDir.resolve("generated-dart")
        val requestFile = tempDir.resolve("legacy-dart-request.json").apply {
            writeText(
                buildJsonObject {
                    put("contractVersion", 1)
                    put("databaseName", "LegacyDartDatabase")
                    put("sqlDirectory", sqlDir.absolutePath)
                    put("packageName", "ignored.for.dart")
                    put("outputDirectory", outputDir.absolutePath)
                    put("schemaDatabaseFile", JsonNull)
                    put("debug", false)
                    put("oversqlite", false)
                    put("backend", "dart")
                }.toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val response = run.response
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(1, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertFalse(response.containsKey("generatedSwiftFiles"))
        assertEquals(listOf(outputDir.resolve("legacy_dart_database.dart").absolutePath), response.stringArray("generatedFiles"))
        assertTrue(outputDir.resolve("legacy_dart_database.dart").isFile)
    }

    @Test
    fun compilerArtifactRejectsUnsupportedLegacyContractWithoutEchoingIt() {
        val requestFile = tempDir.resolve("unsupported-legacy-contract-request.json").apply {
            writeText(
                buildJsonObject {
                    put("contractVersion", 999)
                    put("databaseName", "UnsupportedLegacyContractDatabase")
                    put("sqlDirectory", tempDir.resolve("sql/UnsupportedLegacyContractDatabase").absolutePath)
                    put("packageName", "com.test.unsupported")
                    put("outputDirectory", tempDir.resolve("generated-unsupported").absolutePath)
                    put("schemaDatabaseFile", JsonNull)
                    put("debug", false)
                    put("oversqlite", false)
                }.toString()
            )
        }

        val run = runCompilerJar(requestFile)

        assertEquals(1, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")
        assertEquals(false, run.response.getValue("success").jsonPrimitive.boolean)
        assertEquals(1, run.response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertTrue(
            run.response.getValue("failure").jsonObject.getValue("message").jsonPrimitive.content
                .contains("request specified 999"),
            "Expected unsupported contract version in failure message. stdout:\n${run.stdout}"
        )
    }

    @Test
    fun compilerArtifactGeneratesCoreSwiftProductSourceFromVersionTwoRequest() {
        val sqlDir = createCoreSwiftProductSql("CoreCliDatabase")
        val compilerOutputDir = tempDir.resolve("compiler-output/core")
        val swiftOutputDir = tempDir.resolve("swift-output/CoreCliDatabaseSQLiteNow")
        val requestFile = tempDir.resolve("swift-core-request.json").apply {
            writeText(
                swiftProductRequest(
                    databaseName = "CoreCliDatabase",
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = compilerOutputDir,
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "CoreCliDatabaseSQLiteNow",
                    runtime = "core",
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val response = run.response
        val expectedSwiftFiles = listOf(
            "CoreCliDatabase.swift",
            "SQLiteNowSupport.swift",
            "TaskQueries.swift",
        )
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(2, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertEquals("swiftProductSource", response.getValue("generationMode").jsonPrimitive.content)
        assertEquals(swiftOutputDir.absoluteFile.invariantSeparatorsPath, response.getValue("swiftOutputDirectory").jsonPrimitive.content)
        assertEquals(expectedSwiftFiles, response.stringArray("generatedSwiftFiles"))
        assertEquals(
            expectedSwiftFiles.map { swiftOutputDir.resolve(it).absoluteFile.invariantSeparatorsPath },
            response.stringArray("generatedSwiftFilesAbsolute"),
        )
        assertEquals(response.stringArray("generatedSwiftFilesAbsolute"), response.stringArray("generatedFiles"))
        expectedSwiftFiles.forEach { fileName ->
            assertTrue(swiftOutputDir.resolve(fileName).isFile, "Expected generated Swift file $fileName")
        }

        val databaseSwift = swiftOutputDir.resolve("CoreCliDatabase.swift").readText()
        val querySwift = swiftOutputDir.resolve("TaskQueries.swift").readText()
        val supportSwift = swiftOutputDir.resolve("SQLiteNowSupport.swift").readText()
        assertTrue(databaseSwift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertTrue(querySwift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertTrue(supportSwift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertFalse(databaseSwift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertTrue(querySwift.contains("private static func selectAllLoadRows("))
        assertFalse(querySwift.contains("self.selectAll().list()"))
        assertFalse(compilerOutputDir.exists() && compilerOutputDir.walkTopDown().any { it.isFile && it.extension == "kt" })
    }

    @Test
    fun compilerArtifactGeneratesSyncSwiftProductSourceWithDefaultSyncRuntimeModule() {
        val sqlDir = createSyncSwiftProductSql("SyncCliDatabase")
        val swiftOutputDir = tempDir.resolve("swift-output/SyncCliDatabaseSQLiteNow")
        val requestFile = tempDir.resolve("swift-sync-request.json").apply {
            writeText(
                swiftProductRequest(
                    databaseName = "SyncCliDatabase",
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/sync"),
                    swiftOutputDirectory = swiftOutputDir,
                    swiftModuleName = "SyncCliDatabaseSQLiteNow",
                    runtime = "sync",
                    runtimeModuleName = null,
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val response = run.response
        val expectedSwiftFiles = listOf(
            "DocsQueries.swift",
            "SQLiteNowSupport.swift",
            "SQLiteNowSyncSupport.swift",
            "SyncCliDatabase.swift",
        )
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(2, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertEquals(expectedSwiftFiles, response.stringArray("generatedSwiftFiles"))
        assertEquals(
            expectedSwiftFiles.map { swiftOutputDir.resolve(it).absoluteFile.invariantSeparatorsPath },
            response.stringArray("generatedSwiftFilesAbsolute"),
        )
        expectedSwiftFiles.forEach { fileName ->
            assertTrue(swiftOutputDir.resolve(fileName).isFile, "Expected generated Swift file $fileName")
        }

        val databaseSwift = swiftOutputDir.resolve("SyncCliDatabase.swift").readText()
        val supportSwift = swiftOutputDir.resolve("SQLiteNowSupport.swift").readText()
        val syncSupportSwift = swiftOutputDir.resolve("SQLiteNowSyncSupport.swift").readText()
        assertTrue(databaseSwift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertTrue(supportSwift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertTrue(syncSupportSwift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertTrue(syncSupportSwift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertTrue(syncSupportSwift.contains("refreshedAccessTokenProvider"))
        assertFalse(syncSupportSwift.contains("refreshTokenProvider"))
        assertFalse(supportSwift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertFalse(databaseSwift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertTrue(databaseSwift.contains("SQLiteNowSyncRuntimeTableSpec(tableName: \"docs\", syncKeyColumnName: \"doc_id\")"))
    }

    @Test
    fun compilerArtifactGeneratesCoreSwiftProductPackageFromVersionTwoRequest() {
        val databaseName = "CorePackageCliDatabase"
        val packageName = "CorePackageCliDatabase-SQLiteNow"
        val targetName = "CorePackageCliDatabaseSQLiteNow"
        val sqlDir = createCoreSwiftProductSql(databaseName)
        val runtimeDir = createRuntimeXcframework("SQLiteNowCoreRuntime")
        val requestFile = tempDir.resolve("swift-core-package-request.json").apply {
            writeText(
                swiftProductPackageRequest(
                    databaseName = databaseName,
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/core-package"),
                    swiftPackageName = packageName,
                    swiftTargetName = targetName,
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val response = run.response
        val packageDir = tempDir.resolve("SQLiteNowGenerated/$packageName")
        val packageSwift = packageDir.resolve("Package.swift")
        val metadataManifest = packageDir.resolve(".sqlitenow/package-manifest.json")
        val packagedSwift = packageDir.resolve("Sources/$targetName/$databaseName.swift")
        val expectedGeneratedSwiftFiles = listOf(
            "$databaseName.swift",
            "SQLiteNowSupport.swift",
            "TaskQueries.swift",
        )
        val expectedPackagedSwiftFiles = expectedGeneratedSwiftFiles.map { "Sources/$targetName/$it" }
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(2, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertEquals("swiftProductPackage", response.getValue("generationMode").jsonPrimitive.content)
        assertEquals(packageDir.canonicalFile.normalize().invariantSeparatorsPath, response.getValue("swiftPackageOutputDirectory").jsonPrimitive.content)
        assertEquals(packageSwift.canonicalFile.normalize().invariantSeparatorsPath, response.getValue("packageSwiftFile").jsonPrimitive.content)
        assertEquals(metadataManifest.canonicalFile.normalize().invariantSeparatorsPath, response.getValue("packageMetadataManifestFile").jsonPrimitive.content)
        assertEquals(expectedPackagedSwiftFiles, response.stringArray("packagedSwiftFiles"))
        assertEquals(listOf("Binaries/SQLiteNowCoreRuntime.xcframework"), response.stringArray("runtimeArtifactPaths"))
        assertTrue(packageSwift.isFile)
        assertTrue(metadataManifest.isFile)
        assertTrue(packagedSwift.isFile)
        assertTrue(packageDir.resolve("Sources/$targetName/SQLiteNowSupport.swift").isFile)
        assertTrue(packageDir.resolve("Sources/$targetName/TaskQueries.swift").isFile)
        assertTrue(packageDir.resolve("Binaries/SQLiteNowCoreRuntime.xcframework/Info.plist").isFile)
        assertTrue(response.stringArray("generatedFiles").contains(packageSwift.canonicalFile.normalize().invariantSeparatorsPath))
        assertEquals(expectedGeneratedSwiftFiles, response.stringArray("generatedSwiftFiles"))

        val swift = packagedSwift.readText()
        assertTrue(swift.contains("@preconcurrency import SQLiteNowCoreRuntime"))
        assertFalse(swift.contains("@preconcurrency import SQLiteNowSyncRuntime"))

        val manifest = Json.parseToJsonElement(metadataManifest.readText()).jsonObject
        assertEquals("core", manifest.getValue("runtimeMode").jsonPrimitive.content)
        assertEquals(listOf("SQLiteNowCoreRuntime"), manifest.stringArray("runtimeBinaryTargets"))
        assertEquals(response.getValue("sourceInputDigest").jsonPrimitive.content, manifest.getValue("sourceInputDigest").jsonPrimitive.content)
        assertTrue(manifest.stringArray("generatorInputs").contains("runtimeMode=core"))
    }

    @Test
    fun compilerArtifactGeneratesCoreSwiftProductPackageFromLocalRuntimeZipArtifact() {
        val databaseName = "CoreZipPackageCliDatabase"
        val packageName = "CoreZipPackageCliDatabaseSQLiteNow"
        val version = compilerVersion()
        val sqlDir = createCoreSwiftProductSql(databaseName)
        val runtimeZip = createRuntimeZip("SQLiteNowCoreRuntime", version)
        val checksum = sha256(runtimeZip)
        val requestFile = tempDir.resolve("swift-core-package-zip-request.json").apply {
            writeText(
                swiftProductPackageRequest(
                    databaseName = databaseName,
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/core-package-zip"),
                    swiftPackageName = packageName,
                    swiftTargetName = packageName,
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = checksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val packageDir = tempDir.resolve("SQLiteNowGenerated/$packageName")
        val packageSwift = packageDir.resolve("Package.swift").readText()
        val metadataManifest = packageDir.resolve(".sqlitenow/package-manifest.json")
        val packagedZipPath = "Binaries/SQLiteNowCoreRuntime-$version.xcframework.zip"
        assertTrue(packageDir.resolve(packagedZipPath).isFile)
        assertTrue(packageSwift.contains("""path: "$packagedZipPath""""))
        assertEquals("localZip", run.response.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(checksum, run.response.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals(version, run.response.getValue("runtimeArtifactVersion").jsonPrimitive.content)
        assertEquals(listOf(packagedZipPath), run.response.stringArray("runtimeArtifactPaths"))

        val manifest = Json.parseToJsonElement(metadataManifest.readText()).jsonObject
        assertEquals(3, manifest.getValue("manifestVersion").jsonPrimitive.content.toInt())
        assertEquals(version, manifest.getValue("sqliteNowVersion").jsonPrimitive.content)
        assertEquals(version, manifest.getValue("generatorVersion").jsonPrimitive.content)
        assertEquals("localZip", manifest.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(checksum, manifest.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals("swiftpm-sha256", manifest.getValue("runtimeArtifactChecksumAlgorithm").jsonPrimitive.content)
        assertEquals(version, manifest.getValue("runtimeArtifactVersion").jsonPrimitive.content)
    }

    @Test
    fun compilerArtifactGeneratesCoreSwiftProductPackageFromRemoteRuntimeZipArtifact() {
        val databaseName = "CoreRemoteZipPackageCliDatabase"
        val packageName = "CoreRemoteZipPackageCliDatabaseSQLiteNow"
        val version = compilerVersion()
        val sqlDir = createCoreSwiftProductSql(databaseName)
        val checksum = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        val runtimeUrl =
            "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v$version/SQLiteNowCoreRuntime-$version.xcframework.zip"
        val requestFile = tempDir.resolve("swift-core-package-remote-zip-request.json").apply {
            writeText(
                swiftProductPackageRequest(
                    databaseName = databaseName,
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/core-package-remote-zip"),
                    swiftPackageName = packageName,
                    swiftTargetName = packageName,
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = remoteRuntimeArtifactRequest(
                        url = runtimeUrl,
                        checksum = checksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val packageDir = tempDir.resolve("SQLiteNowGenerated/$packageName")
        val packageSwift = packageDir.resolve("Package.swift").readText()
        val metadataManifest = packageDir.resolve(".sqlitenow/package-manifest.json")
        assertFalse(packageDir.resolve("Binaries").exists())
        assertTrue(packageSwift.contains("""url: "$runtimeUrl""""))
        assertTrue(packageSwift.contains("""checksum: "$checksum""""))
        assertFalse(packageSwift.contains("path:"))
        assertEquals("remoteZip", run.response.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(checksum, run.response.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals(version, run.response.getValue("runtimeArtifactVersion").jsonPrimitive.content)
        assertEquals(runtimeUrl, run.response.getValue("runtimeArtifactUrl").jsonPrimitive.content)
        assertEquals(emptyList(), run.response.stringArray("runtimeArtifactPaths"))
        assertEquals(emptyList(), run.response.stringArray("runtimeArtifactPathsAbsolute"))

        val manifest = Json.parseToJsonElement(metadataManifest.readText()).jsonObject
        assertEquals(3, manifest.getValue("manifestVersion").jsonPrimitive.content.toInt())
        assertEquals(version, manifest.getValue("sqliteNowVersion").jsonPrimitive.content)
        assertEquals(version, manifest.getValue("generatorVersion").jsonPrimitive.content)
        assertEquals("remoteZip", manifest.getValue("runtimeArtifactKind").jsonPrimitive.content)
        assertEquals(emptyList(), manifest.stringArray("runtimeArtifactPaths"))
        assertEquals(JsonNull, manifest.getValue("runtimeArtifactSourcePath"))
        assertEquals(checksum, manifest.getValue("runtimeArtifactChecksum").jsonPrimitive.content)
        assertEquals("swiftpm-sha256", manifest.getValue("runtimeArtifactChecksumAlgorithm").jsonPrimitive.content)
        assertEquals(version, manifest.getValue("runtimeArtifactVersion").jsonPrimitive.content)
        assertEquals(runtimeUrl, manifest.getValue("runtimeArtifactUrl").jsonPrimitive.content)
    }

    @Test
    fun compilerArtifactGeneratesSyncSwiftProductPackageWithDefaultRuntimeModule() {
        val databaseName = "SyncPackageCliDatabase"
        val packageName = "SyncPackageCliDatabaseSQLiteNow"
        val sqlDir = createSyncSwiftProductSql(databaseName)
        val runtimeDir = createRuntimeXcframework("SQLiteNowSyncRuntime")
        val requestFile = tempDir.resolve("swift-sync-package-request.json").apply {
            writeText(
                swiftProductPackageRequest(
                    databaseName = databaseName,
                    sqlDirectory = sqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/sync-package"),
                    swiftPackageName = packageName,
                    swiftTargetName = packageName,
                    runtime = "sync",
                    runtimeXcframeworkDirectory = runtimeDir,
                    runtimeModuleName = null,
                ).toString()
            )
        }

        val run = runCompilerJar(requestFile)
        assertEquals(0, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")

        val packageDir = tempDir.resolve("SQLiteNowGenerated/$packageName")
        val packagedSwift = packageDir.resolve("Sources/$packageName/$databaseName.swift")
        val packagedSyncSupport = packageDir.resolve("Sources/$packageName/SQLiteNowSyncSupport.swift")
        val metadataManifest = packageDir.resolve(".sqlitenow/package-manifest.json")
        assertTrue(packagedSwift.isFile)
        assertTrue(packagedSyncSupport.isFile)
        assertTrue(packageDir.resolve("Binaries/SQLiteNowSyncRuntime.xcframework/Info.plist").isFile)
        assertEquals(listOf("Binaries/SQLiteNowSyncRuntime.xcframework"), run.response.stringArray("runtimeArtifactPaths"))
        assertTrue(run.response.stringArray("packagedSwiftFiles").contains("Sources/$packageName/SQLiteNowSyncSupport.swift"))

        val swift = packagedSwift.readText()
        val syncSupportSwift = packagedSyncSupport.readText()
        assertTrue(swift.contains("@preconcurrency import SQLiteNowSyncRuntime"))
        assertTrue(swift.contains("SQLiteNowSyncRuntimeTableSpec(tableName: \"docs\", syncKeyColumnName: \"doc_id\")"))
        assertTrue(syncSupportSwift.contains("public struct SQLiteNowSyncAuth: Sendable"))
        assertTrue(syncSupportSwift.contains("refreshedAccessTokenProvider"))
        assertFalse(syncSupportSwift.contains("refreshTokenProvider"))

        val manifest = Json.parseToJsonElement(metadataManifest.readText()).jsonObject
        assertEquals("sync", manifest.getValue("runtimeMode").jsonPrimitive.content)
        assertEquals(listOf("SQLiteNowSyncRuntime"), manifest.stringArray("runtimeBinaryTargets"))
        assertTrue(manifest.stringArray("generatedSwiftFiles").contains("Sources/$packageName/SQLiteNowSyncSupport.swift"))
        val syncTable = manifest.getValue("syncTables").jsonArray.single().jsonObject
        assertEquals("docs", syncTable.getValue("tableName").jsonPrimitive.content)
        assertEquals("doc_id", syncTable.getValue("syncKeyColumnName").jsonPrimitive.content)
        assertTrue(manifest.stringArray("generatorInputs").contains("syncTables=docs:doc_id"))
    }

    @TestFactory
    fun compilerArtifactRejectsInvalidSwiftProductRequests(): List<DynamicTest> {
        val validSqlDir = createCoreSwiftProductSql("FailureCoreDatabase")
        val missingSqlDir = tempDir.resolve("sql/MissingDatabase")
        val scenarios = listOf(
            FailureScenario(
                name = "missing SQL directory",
                request = swiftProductRequest(
                    databaseName = "MissingDatabase",
                    sqlDirectory = missingSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/missing-sql"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/MissingDatabaseSQLiteNow"),
                    swiftModuleName = "MissingDatabaseSQLiteNow",
                    runtime = "core",
                ),
                expectedMessage = "SQL database directory '${missingSqlDir.path}' not found",
            ),
            FailureScenario(
                name = "invalid runtime mode",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/invalid-runtime"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/InvalidRuntimeSQLiteNow"),
                    swiftModuleName = "InvalidRuntimeSQLiteNow",
                    runtime = "desktop",
                ),
                expectedMessage = "Unsupported Swift product runtime 'desktop'",
            ),
            FailureScenario(
                name = "missing swiftOutputDirectory",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/missing-swift-output"),
                    swiftOutputDirectory = null,
                    swiftModuleName = "MissingSwiftOutputSQLiteNow",
                    runtime = "core",
                ),
                expectedMessage = "Missing required request field 'swiftOutputDirectory'.",
            ),
            FailureScenario(
                name = "missing swiftModuleName",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/missing-swift-module"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/MissingSwiftModuleSQLiteNow"),
                    swiftModuleName = null,
                    runtime = "core",
                ),
                expectedMessage = "Missing required request field 'swiftModuleName'.",
            ),
            FailureScenario(
                name = "invalid swiftModuleName",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/invalid-swift-module"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/InvalidSwiftModuleSQLiteNow"),
                    swiftModuleName = "Invalid-Swift-Module",
                    runtime = "core",
                ),
                expectedMessage = "Swift product swiftModuleName must be a valid Swift identifier",
            ),
            FailureScenario(
                name = "invalid runtimeModuleName",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/invalid-runtime-module"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/InvalidRuntimeModuleSQLiteNow"),
                    swiftModuleName = "InvalidRuntimeModuleSQLiteNow",
                    runtime = "core",
                    runtimeModuleName = "SQLiteNow Core Runtime",
                ),
                expectedMessage = "Swift product runtimeModuleName must be a valid Swift identifier",
            ),
            FailureScenario(
                name = "unsupported swift product contract version",
                request = swiftProductRequest(
                    contractVersion = 999,
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/unsupported-contract"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/UnsupportedContractSQLiteNow"),
                    swiftModuleName = "UnsupportedContractSQLiteNow",
                    runtime = "core",
                ),
                expectedMessage = "request specified 999",
            ),
            FailureScenario(
                name = "swift product request with legacy backend dart",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/legacy-backend"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/LegacyBackendSQLiteNow"),
                    swiftModuleName = "LegacyBackendSQLiteNow",
                    runtime = "core",
                    legacyBackend = "dart",
                ),
                expectedMessage = "legacy request field(s): backend",
            ),
            FailureScenario(
                name = "swift product request with legacy oversqlite field",
                request = swiftProductRequest(
                    databaseName = "FailureCoreDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/legacy-oversqlite"),
                    swiftOutputDirectory = tempDir.resolve("swift-output/LegacyOversqliteSQLiteNow"),
                    swiftModuleName = "LegacyOversqliteSQLiteNow",
                    runtime = "sync",
                    includeLegacyOversqlite = true,
                ),
                expectedMessage = "legacy request field(s): oversqlite",
            ),
        )

        return scenarios.map { scenario ->
            DynamicTest.dynamicTest(scenario.name) {
                val requestFile = tempDir.resolve("failure-${scenario.name.replace(Regex("[^A-Za-z0-9]+"), "-")}.json")
                    .apply { writeText(scenario.request.toString()) }

                val run = runCompilerJar(requestFile)

                assertEquals(1, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")
                assertEquals(false, run.response.getValue("success").jsonPrimitive.boolean)
                assertEquals(
                    scenario.expectedContractVersion,
                    run.response.getValue("contractVersion").jsonPrimitive.content.toInt(),
                )
                assertTrue(
                    run.response.getValue("failure").jsonObject.getValue("message").jsonPrimitive.content
                        .contains(scenario.expectedMessage),
                    "Expected failure message to contain '${scenario.expectedMessage}'. stdout:\n${run.stdout}"
                )
            }
        }
    }

    @TestFactory
    fun compilerArtifactRejectsInvalidSwiftProductPackageRequests(): List<DynamicTest> {
        val validSqlDir = createCoreSwiftProductSql("FailurePackageDatabase")
        val runtimeDir = createRuntimeXcframework("SQLiteNowCoreRuntime")
        val version = compilerVersion()
        val runtimeZip = createRuntimeZip("SQLiteNowCoreRuntime", version)
        val runtimeZipChecksum = sha256(runtimeZip)
        val syncRuntimeZip = createRuntimeZip("SQLiteNowSyncRuntime", version)
        val remoteZipChecksum = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        val remoteZipUrl = "https://example.com/SQLiteNowCoreRuntime-$version.xcframework.zip"
        val remoteSyncZipUrl = "https://example.com/SQLiteNowSyncRuntime-$version.xcframework.zip"
        val unsafePackageDir = tempDir.resolve("UserOwnedOutput/UnsafePackageSQLiteNow").apply {
            mkdirs()
            resolve("user-owned.txt").writeText("not generated")
        }
        val scenarios = listOf(
            FailureScenario(
                name = "invalid package runtime mode",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-invalid-runtime"),
                    swiftPackageName = "InvalidRuntimeSQLiteNow",
                    swiftTargetName = "InvalidRuntimeSQLiteNow",
                    runtime = "desktop",
                    runtimeXcframeworkDirectory = runtimeDir,
                ),
                expectedMessage = "Unsupported Swift product runtime 'desktop'",
            ),
            FailureScenario(
                name = "missing swift package target name",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-missing-target"),
                    swiftPackageName = "MissingTargetSQLiteNow",
                    swiftTargetName = null,
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                ),
                expectedMessage = "Missing required request field 'swiftTargetName'.",
            ),
            FailureScenario(
                name = "invalid swift package name",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-invalid-name"),
                    swiftPackageName = "Invalid/PackageSQLiteNow",
                    swiftTargetName = "InvalidPackageSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                ),
                expectedMessage = "Swift package swiftPackageName must not contain path separators",
            ),
            FailureScenario(
                name = "invalid swift target name",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-invalid-target"),
                    swiftPackageName = "InvalidTargetSQLiteNow",
                    swiftTargetName = "Invalid-TargetSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                ),
                expectedMessage = "Swift package swiftTargetName must be a valid Swift identifier",
            ),
            FailureScenario(
                name = "invalid package runtime module name",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-invalid-runtime-module"),
                    swiftPackageName = "InvalidPackageRuntimeModuleSQLiteNow",
                    swiftTargetName = "InvalidPackageRuntimeModuleSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                    runtimeModuleName = "SQLiteNow Core Runtime",
                ),
                expectedMessage = "Swift package runtimeModuleName must be a valid Swift identifier",
            ),
            FailureScenario(
                name = "missing runtime xcframework",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-missing-runtime"),
                    swiftPackageName = "MissingRuntimeSQLiteNow",
                    swiftTargetName = "MissingRuntimeSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = tempDir.resolve("runtime/missing.xcframework"),
                ),
                expectedMessage = "Expected reusable runtime XCFramework",
            ),
            FailureScenario(
                name = "package request with both runtime artifact inputs",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-conflict"),
                    swiftPackageName = "RuntimeConflictSQLiteNow",
                    swiftTargetName = "RuntimeConflictSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = runtimeZipChecksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "cannot specify both runtimeXcframeworkDirectory and runtimeArtifact",
            ),
            FailureScenario(
                name = "package request with runtime zip checksum mismatch",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-checksum"),
                    swiftPackageName = "RuntimeChecksumSQLiteNow",
                    swiftTargetName = "RuntimeChecksumSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = "0000000000000000000000000000000000000000000000000000000000000000",
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Runtime artifact checksum mismatch",
            ),
            FailureScenario(
                name = "package request with runtime zip missing checksum",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-missing-checksum"),
                    swiftPackageName = "RuntimeMissingChecksumSQLiteNow",
                    swiftTargetName = "RuntimeMissingChecksumSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = null,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Runtime artifact checksum is required",
            ),
            FailureScenario(
                name = "package request with runtime zip missing version",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-missing-version"),
                    swiftPackageName = "RuntimeMissingVersionSQLiteNow",
                    swiftTargetName = "RuntimeMissingVersionSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = runtimeZipChecksum,
                        sqliteNowVersion = null,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Runtime artifact sqliteNowVersion is required",
            ),
            FailureScenario(
                name = "package request with runtime zip version mismatch",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-version"),
                    swiftPackageName = "RuntimeVersionSQLiteNow",
                    swiftTargetName = "RuntimeVersionSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = runtimeZipChecksum,
                        sqliteNowVersion = "9.9.9",
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Runtime artifact version '9.9.9' is not compatible",
            ),
            FailureScenario(
                name = "package request with generator version mismatch",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-generator-version"),
                    swiftPackageName = "GeneratorVersionSQLiteNow",
                    swiftTargetName = "GeneratorVersionSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = runtimeZip,
                        checksum = runtimeZipChecksum,
                        sqliteNowVersion = "1.2.3",
                    ),
                    sqliteNowVersion = "1.2.3",
                ),
                expectedMessage = "Generator version '$version' is not compatible",
            ),
            FailureScenario(
                name = "package request with runtime zip module mismatch",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-runtime-module"),
                    swiftPackageName = "RuntimeModuleSQLiteNow",
                    swiftTargetName = "RuntimeModuleSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = runtimeArtifactRequest(
                        path = syncRuntimeZip,
                        checksum = sha256(syncRuntimeZip),
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "does not match runtime module 'SQLiteNowCoreRuntime'",
            ),
            FailureScenario(
                name = "package request with remote runtime zip missing url",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-remote-runtime-missing-url"),
                    swiftPackageName = "RemoteRuntimeMissingUrlSQLiteNow",
                    swiftTargetName = "RemoteRuntimeMissingUrlSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = remoteRuntimeArtifactRequest(
                        url = null,
                        checksum = remoteZipChecksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Runtime artifact URL is required",
            ),
            FailureScenario(
                name = "package request with remote runtime zip path conflict",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-remote-runtime-path-conflict"),
                    swiftPackageName = "RemoteRuntimePathConflictSQLiteNow",
                    swiftTargetName = "RemoteRuntimePathConflictSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = remoteRuntimeArtifactRequest(
                        url = remoteZipUrl,
                        checksum = remoteZipChecksum,
                        sqliteNowVersion = version,
                        path = runtimeZip,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "runtimeArtifact.kind 'remoteZip' must not specify path",
            ),
            FailureScenario(
                name = "package request with local runtime zip url conflict",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-local-runtime-url-conflict"),
                    swiftPackageName = "LocalRuntimeUrlConflictSQLiteNow",
                    swiftTargetName = "LocalRuntimeUrlConflictSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = buildJsonObject {
                        put("kind", "localZip")
                        put("path", runtimeZip.absolutePath)
                        put("url", remoteZipUrl)
                        put("checksum", runtimeZipChecksum)
                        put("sqliteNowVersion", version)
                    },
                    sqliteNowVersion = version,
                ),
                expectedMessage = "runtimeArtifact.kind 'localZip' must not specify url",
            ),
            FailureScenario(
                name = "package request with remote runtime zip non https url",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-remote-runtime-non-https"),
                    swiftPackageName = "RemoteRuntimeNonHttpsSQLiteNow",
                    swiftTargetName = "RemoteRuntimeNonHttpsSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = remoteRuntimeArtifactRequest(
                        url = "http://example.com/SQLiteNowCoreRuntime-$version.xcframework.zip",
                        checksum = remoteZipChecksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "Remote runtime artifact URL must use https",
            ),
            FailureScenario(
                name = "package request with remote runtime zip module mismatch",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-remote-runtime-module"),
                    swiftPackageName = "RemoteRuntimeModuleSQLiteNow",
                    swiftTargetName = "RemoteRuntimeModuleSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = null,
                    runtimeArtifact = remoteRuntimeArtifactRequest(
                        url = remoteSyncZipUrl,
                        checksum = remoteZipChecksum,
                        sqliteNowVersion = version,
                    ),
                    sqliteNowVersion = version,
                ),
                expectedMessage = "does not match runtime module 'SQLiteNowCoreRuntime'",
            ),
            FailureScenario(
                name = "package request with unsupported apple target",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-unsupported-apple-target"),
                    swiftPackageName = "UnsupportedAppleTargetSQLiteNow",
                    swiftTargetName = "UnsupportedAppleTargetSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                    requestedAppleTargets = listOf("macosArm64", "macosX64", "iosX64"),
                ),
                expectedMessage = "unsupported Apple target(s): macosX64, iosX64",
            ),
            FailureScenario(
                name = "unsafe existing package output",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-unsafe-output"),
                    swiftPackageName = "UnsafePackageSQLiteNow",
                    swiftTargetName = "UnsafePackageSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                    swiftPackageOutputDirectory = unsafePackageDir,
                ),
                expectedMessage = "Refusing to clean existing Swift package output",
            ),
            FailureScenario(
                name = "package request with legacy backend dart",
                request = swiftProductPackageRequest(
                    databaseName = "FailurePackageDatabase",
                    sqlDirectory = validSqlDir,
                    compilerOutputDirectory = tempDir.resolve("compiler-output/package-legacy-backend"),
                    swiftPackageName = "LegacyBackendPackageSQLiteNow",
                    swiftTargetName = "LegacyBackendPackageSQLiteNow",
                    runtime = "core",
                    runtimeXcframeworkDirectory = runtimeDir,
                    legacyBackend = "dart",
                ),
                expectedMessage = "legacy request field(s): backend",
            ),
        )

        return scenarios.map { scenario ->
            DynamicTest.dynamicTest(scenario.name) {
                val requestFile = tempDir.resolve("package-failure-${scenario.name.replace(Regex("[^A-Za-z0-9]+"), "-")}.json")
                    .apply { writeText(scenario.request.toString()) }

                val run = runCompilerJar(requestFile)

                assertEquals(1, run.exitCode, "stdout:\n${run.stdout}\nstderr:\n${run.stderr}")
                assertEquals(false, run.response.getValue("success").jsonPrimitive.boolean)
                assertEquals(
                    scenario.expectedContractVersion,
                    run.response.getValue("contractVersion").jsonPrimitive.content.toInt(),
                )
                assertTrue(
                    run.response.getValue("failure").jsonObject.getValue("message").jsonPrimitive.content
                        .contains(scenario.expectedMessage),
                    "Expected failure message to contain '${scenario.expectedMessage}'. stdout:\n${run.stdout}"
                )
            }
        }
    }

    @Test
    fun compilerArtifactPrintsGeneratorVersion() {
        val compilerJar = compilerJar()
        val process = ProcessBuilder(
            javaExecutable(),
            "-jar",
            compilerJar.absolutePath,
            "--version",
        )
            .directory(tempDir)
            .start()
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Compiler artifact --version process timed out")

        val stdout = process.inputStream.bufferedReader().readText()
        val stderr = process.errorStream.bufferedReader().readText()
        assertEquals(0, process.exitValue(), "stdout:\n$stdout\nstderr:\n$stderr")
        assertEquals(compilerVersion(), stdout.trim())
    }

    @Test
    fun compilerArtifactContractDescribesLegacyAndSwiftProductRequests() {
        val compilerJar = compilerJar()
        val process = ProcessBuilder(
            javaExecutable(),
            "-jar",
            compilerJar.absolutePath,
            "--contract",
        )
            .directory(tempDir)
            .start()
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Compiler artifact --contract process timed out")

        val stdout = process.inputStream.bufferedReader().readText()
        val stderr = process.errorStream.bufferedReader().readText()
        assertEquals(0, process.exitValue(), "stdout:\n$stdout\nstderr:\n$stderr")

        val response = Json.parseToJsonElement(stdout).jsonObject
        assertEquals(true, response.getValue("success").jsonPrimitive.boolean)
        assertEquals(2, response.getValue("contractVersion").jsonPrimitive.content.toInt())
        assertEquals(compilerVersion(), response.getValue("generatorVersion").jsonPrimitive.content)
        assertEquals(listOf("1", "2"), response.getValue("supportedContractVersions").jsonArray.map { it.jsonPrimitive.content })
        assertNotNull(response.getValue("requests").jsonObject["legacyKotlinOrDart"])
        val swiftRequest = response.getValue("requests").jsonObject.getValue("swiftProductSource").jsonObject
        assertEquals("swiftProductSource", swiftRequest.getValue("generationMode").jsonPrimitive.content)
        assertEquals("string-core-or-sync", swiftRequest.getValue("runtime").jsonPrimitive.content)
        val swiftPackageRequest = response.getValue("requests").jsonObject.getValue("swiftProductPackage").jsonObject
        assertEquals("swiftProductPackage", swiftPackageRequest.getValue("generationMode").jsonPrimitive.content)
        assertEquals("string", swiftPackageRequest.getValue("swiftPackageName").jsonPrimitive.content)
        assertEquals(
            "optional-absolute-or-relative-path-mutually-exclusive-with-runtimeArtifact",
            swiftPackageRequest.getValue("runtimeXcframeworkDirectory").jsonPrimitive.content,
        )
        assertEquals(
            "optional-object-kind-localZip-or-remoteZip",
            swiftPackageRequest.getValue("runtimeArtifact").jsonObject.getValue("kind").jsonPrimitive.content,
        )
        assertEquals(
            "https-url-required-for-remoteZip",
            swiftPackageRequest.getValue("runtimeArtifact").jsonObject.getValue("url").jsonPrimitive.content,
        )
        assertEquals(
            "absolute-path-string-array; package files for swiftProductPackage",
            response.getValue("response").jsonObject.getValue("generatedFiles").jsonPrimitive.content,
        )
        assertEquals(
            "string-array-relative-to-swiftOutputDirectory-for-swiftProductSource-or-swiftProductPackage",
            response.getValue("response").jsonObject.getValue("generatedSwiftFiles").jsonPrimitive.content,
        )
        assertEquals(
            "string-array-relative-to-swiftPackageOutputDirectory-only-for-swiftProductPackage",
            response.getValue("response").jsonObject.getValue("packagedSwiftFiles").jsonPrimitive.content,
        )
        assertEquals(
            "string-only-for-swiftProductPackage",
            response.getValue("response").jsonObject.getValue("runtimeArtifactKind").jsonPrimitive.content,
        )
        assertEquals(
            "optional-string-only-for-swiftProductPackage",
            response.getValue("response").jsonObject.getValue("runtimeArtifactUrl").jsonPrimitive.content,
        )
    }

    @Test
    fun compilerArtifactDoesNotBundleGradleHostRuntime() {
        val compilerJar = compilerJar()
        val forbiddenEntries = listOf(
            "org/gradle/",
            "groovy/",
            "groovyjarjar",
            "org/codehaus/groovy/",
            "META-INF/gradle-plugins/",
            "gradle-wrapper.jar",
            "org/jetbrains/kotlin/gradle/",
        )

        val matches = ZipFile(compilerJar).use { zip ->
            zip.entries()
                .asSequence()
                .map { it.name }
                .filter { entry -> forbiddenEntries.any { forbidden -> entry.contains(forbidden) } }
                .take(20)
                .toList()
        }

        assertTrue(
            matches.isEmpty(),
            "Compiler artifact must not contain Gradle/Groovy host runtime entries. Matches: $matches"
        )
    }

    private fun runCompilerJar(requestFile: File): CompilerRun {
        val compilerJar = compilerJar()
        val process = ProcessBuilder(
            javaExecutable(),
            "-jar",
            compilerJar.absolutePath,
            "--request",
            requestFile.absolutePath,
        )
            .directory(tempDir)
            .start()
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Compiler artifact process timed out")

        val stdout = process.inputStream.bufferedReader().readText()
        val stderr = process.errorStream.bufferedReader().readText()
        val response = Json.parseToJsonElement(stdout).jsonObject
        return CompilerRun(
            exitCode = process.exitValue(),
            stdout = stdout,
            stderr = stderr,
            response = response,
        )
    }

    private fun compilerJar(): File =
        File(
            checkNotNull(System.getProperty("sqlitenow.compilerJar")) {
                "sqlitenow.compilerJar system property must point at the runnable compiler jar"
            }
        ).also { compilerJar ->
            assertTrue(compilerJar.isFile, "Expected compiler jar at ${compilerJar.absolutePath}")
        }

    private fun compilerVersion(): String =
        JarFile(compilerJar()).use { jar ->
            checkNotNull(jar.manifest.mainAttributes.getValue("Implementation-Version")) {
                "Compiler jar manifest must include Implementation-Version."
            }
        }

    private fun JsonObject.stringArray(name: String): List<String> =
        getValue(name).jsonArray.map { it.jsonPrimitive.content }

    private fun swiftProductRequest(
        contractVersion: Int = 2,
        databaseName: String,
        sqlDirectory: File,
        compilerOutputDirectory: File,
        swiftOutputDirectory: File?,
        swiftModuleName: String?,
        runtime: String,
        runtimeModuleName: String? = null,
        legacyBackend: String? = null,
        includeLegacyOversqlite: Boolean = false,
    ): JsonObject =
        buildJsonObject {
            put("contractVersion", contractVersion)
            put("generationMode", "swiftProductSource")
            put("databaseName", databaseName)
            put("sqlDirectory", sqlDirectory.absolutePath)
            put("metadataPackageName", "dev.test.${databaseName.lowercase()}.metadata")
            put("compilerOutputDirectory", compilerOutputDirectory.absolutePath)
            if (swiftOutputDirectory != null) {
                put("swiftOutputDirectory", swiftOutputDirectory.absolutePath)
            }
            if (swiftModuleName != null) {
                put("swiftModuleName", swiftModuleName)
            }
            put("runtime", runtime)
            if (runtimeModuleName != null) {
                put("runtimeModuleName", runtimeModuleName)
            }
            put("schemaDatabaseFile", JsonNull)
            put("debug", false)
            if (legacyBackend != null) {
                put("backend", legacyBackend)
            }
            if (includeLegacyOversqlite) {
                put("oversqlite", true)
            }
        }

    private fun swiftProductPackageRequest(
        contractVersion: Int = 2,
        databaseName: String,
        sqlDirectory: File,
        compilerOutputDirectory: File,
        swiftPackageName: String?,
        swiftTargetName: String?,
        runtime: String,
        runtimeXcframeworkDirectory: File?,
        runtimeArtifact: JsonObject? = null,
        swiftOutputDirectory: File? = null,
        swiftPackageOutputDirectory: File? = null,
        runtimeModuleName: String? = null,
        requestedAppleTargets: List<String>? = null,
        sqliteNowVersion: String? = "1.2.3",
        legacyBackend: String? = null,
        includeLegacyOversqlite: Boolean = false,
    ): JsonObject =
        buildJsonObject {
            put("contractVersion", contractVersion)
            put("generationMode", "swiftProductPackage")
            put("databaseName", databaseName)
            put("sqlDirectory", sqlDirectory.absolutePath)
            put("metadataPackageName", "dev.test.${databaseName.lowercase()}.metadata")
            put("compilerOutputDirectory", compilerOutputDirectory.absolutePath)
            if (swiftOutputDirectory != null) {
                put("swiftOutputDirectory", swiftOutputDirectory.absolutePath)
            }
            if (swiftPackageOutputDirectory != null) {
                put("swiftPackageOutputDirectory", swiftPackageOutputDirectory.absolutePath)
            }
            if (swiftPackageName != null) {
                put("swiftPackageName", swiftPackageName)
            }
            if (swiftTargetName != null) {
                put("swiftTargetName", swiftTargetName)
            }
            put("runtime", runtime)
            if (runtimeModuleName != null) {
                put("runtimeModuleName", runtimeModuleName)
            }
            if (requestedAppleTargets != null) {
                put(
                    "requestedAppleTargets",
                    buildJsonArray {
                        requestedAppleTargets.forEach { add(JsonPrimitive(it)) }
                    }
                )
            }
            if (runtimeXcframeworkDirectory != null) {
                put("runtimeXcframeworkDirectory", runtimeXcframeworkDirectory.absolutePath)
            }
            if (runtimeArtifact != null) {
                put("runtimeArtifact", runtimeArtifact)
            }
            if (sqliteNowVersion != null) {
                put("sqliteNowVersion", sqliteNowVersion)
            }
            put("schemaDatabaseFile", JsonNull)
            put("debug", false)
            if (legacyBackend != null) {
                put("backend", legacyBackend)
            }
            if (includeLegacyOversqlite) {
                put("oversqlite", true)
            }
        }

    private fun runtimeArtifactRequest(
        path: File,
        checksum: String? = null,
        sqliteNowVersion: String? = null,
        kind: String = "localZip",
    ): JsonObject =
        buildJsonObject {
            put("kind", kind)
            put("path", path.absolutePath)
            if (checksum != null) {
                put("checksum", checksum)
            }
            if (sqliteNowVersion != null) {
                put("sqliteNowVersion", sqliteNowVersion)
            }
        }

    private fun remoteRuntimeArtifactRequest(
        url: String?,
        checksum: String? = null,
        sqliteNowVersion: String? = null,
        path: File? = null,
    ): JsonObject =
        buildJsonObject {
            put("kind", "remoteZip")
            if (url != null) {
                put("url", url)
            }
            if (path != null) {
                put("path", path.absolutePath)
            }
            if (checksum != null) {
                put("checksum", checksum)
            }
            if (sqliteNowVersion != null) {
                put("sqliteNowVersion", sqliteNowVersion)
            }
        }

    private fun createRuntimeXcframework(moduleName: String): File =
        tempDir.resolve("runtime/$moduleName.xcframework").apply {
            mkdirs()
            resolve("Info.plist").writeText("fake runtime")
        }

    private fun createRuntimeZip(moduleName: String, version: String): File =
        tempDir.resolve("runtime/$moduleName-$version.xcframework.zip").apply {
            parentFile.mkdirs()
            writeText("fake zipped runtime for $moduleName $version")
        }

    private fun createLegacyDartSql(databaseName: String): File {
        val sqlDir = tempDir.resolve("sql/$databaseName")
        sqlDir.resolve("schema/person.sql").writeSql(
            """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL
                );
            """.trimIndent()
        )
        sqlDir.resolve("queries/person/selectAll.sql").writeSql(
            """
                -- @@{ queryResult=PersonRow }
                SELECT id, name
                FROM person
                ORDER BY id;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createCoreSwiftProductSql(databaseName: String): File {
        val sqlDir = tempDir.resolve("sql/$databaseName")
        sqlDir.resolve("schema/task.sql").writeSql(
            """
                CREATE TABLE task (
                    id INTEGER PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL
                );
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/selectAll.sql").writeSql(
            """
                -- @@{ queryResult=TaskRow }
                SELECT id, title
                FROM task
                ORDER BY id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/task/insert.sql").writeSql(
            """
                INSERT INTO task (id, title)
                VALUES (:id, :title);
            """.trimIndent()
        )
        return sqlDir
    }

    private fun createSyncSwiftProductSql(databaseName: String): File {
        val sqlDir = tempDir.resolve("sql/$databaseName")
        sqlDir.resolve("schema/docs.sql").writeSql(
            """
                -- @@{ enableSync=true }
                CREATE TABLE docs (
                    doc_id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL
                );
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/selectAll.sql").writeSql(
            """
                -- @@{ queryResult=DocRow }
                SELECT doc_id, title
                FROM docs
                ORDER BY doc_id;
            """.trimIndent()
        )
        sqlDir.resolve("queries/docs/insertOrReplace.sql").writeSql(
            """
                INSERT INTO docs (doc_id, title)
                VALUES (:docId, :title)
                ON CONFLICT(doc_id) DO UPDATE SET title = excluded.title;
            """.trimIndent()
        )
        return sqlDir
    }

    private fun File.writeSql(value: String) {
        parentFile.mkdirs()
        writeText(value)
    }

    private fun javaExecutable(): String =
        File(System.getProperty("java.home"), "bin/java").absolutePath
}
