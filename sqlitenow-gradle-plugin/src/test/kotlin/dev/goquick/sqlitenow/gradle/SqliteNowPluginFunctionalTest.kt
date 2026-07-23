package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.swift.DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS
import dev.goquick.sqlitenow.gradle.swift.SwiftPackageMinimumPlatforms
import dev.goquick.sqlitenow.gradle.swift.SwiftProductRuntimeMode
import dev.goquick.sqlitenow.gradle.swift.swiftPackageGeneratorConfigInputs
import dev.goquick.sqlitenow.gradle.swift.swiftPackageSourceInputDigest
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.io.File
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowPluginFunctionalTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("Obsolete SQLiteNow raw statement source fails to compile without a compatibility shim")
    fun obsoleteSqliteNowRawStatementImportFailsToCompile() {
        val projectDir = tempDir.resolve("obsolete-raw-statement-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            kotlinBody = """
                sourceSets {
                    commonMain.dependencies {
                        implementation("dev.goquick.sqlitenow:core")
                    }
                }
            """.trimIndent(),
        )
        writeFixtureFile(
            projectDir,
            "src/commonMain/kotlin/fixture/ObsoleteRawStatementUsage.kt",
            """
                package fixture

                import dev.goquick.sqlitenow.core.sqlite.SqliteStatement

                fun bindObsoleteStatement(statement: SqliteStatement) {
                    statement.bindLong(1, 27L)
                }
            """.trimIndent(),
        )

        val result = runGradleAndFail(projectDir, "compileKotlinJvm", "--stacktrace")

        assertTrue(
            result.output.contains("Unresolved reference 'SqliteStatement'"),
            "The removed raw type must remain a source-breaking compile failure. Output:\n${result.output}",
        )
    }

    @Test
    @DisplayName("External web consumers cannot import the internal SQLite worker provider")
    fun externalWorkerProviderImportFailsToCompile() {
        val projectDir = tempDir.resolve("internal-worker-provider-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeBuildGradle(
            projectDir,
            """
                plugins {
                    kotlin("multiplatform") version "2.4.0"
                }

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    js {
                        nodejs()
                    }

                    sourceSets {
                        jsMain.dependencies {
                            implementation("dev.goquick.sqlitenow:core")
                        }
                    }
                }
            """.trimIndent(),
        )
        writeFixtureFile(
            projectDir,
            "src/jsMain/kotlin/fixture/InternalWorkerProviderUsage.kt",
            """
                package fixture

                import dev.goquick.sqlitenow.core.worker.SqliteWorkerConnectionProvider

                fun createWorkerProvider() = SqliteWorkerConnectionProvider()
            """.trimIndent(),
        )

        val result = runGradleAndFail(projectDir, "compileKotlinJs", "--stacktrace")

        assertTrue(
            result.output.contains("SqliteWorkerConnectionProvider") &&
                result.output.contains("internal", ignoreCase = true),
            "The internal worker provider must be rejected without diagnostic suppressions. " +
                "Output:\n${result.output}",
        )
    }

    @Test
    @DisplayName("External web consumers compile the public worker factory without remote URLs or SQL.js calls")
    fun externalWebConsumerCompilesWorkerFactory() {
        val projectDir = tempDir.resolve("public-worker-factory-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeBuildGradle(
            projectDir,
            """
                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    js {
                        browser()
                    }
                    wasmJs {
                        browser()
                    }

                    sourceSets {
                        jsMain.dependencies {
                            implementation("dev.goquick.sqlitenow:core")
                        }
                        wasmJsMain.dependencies {
                            implementation("dev.goquick.sqlitenow:core")
                        }
                    }
                }
            """.trimIndent(),
        )
        val usage = """
            package fixture

            import dev.goquick.sqlitenow.core.SqliteConnectionProvider
            import dev.goquick.sqlitenow.core.worker.sqliteWorkerConnectionProvider

            val packagedWorkerProvider: SqliteConnectionProvider =
                sqliteWorkerConnectionProvider()
            val relativeWorkerProvider: SqliteConnectionProvider =
                sqliteWorkerConnectionProvider("./sqlite-3.53.0-build1/worker.mjs")
        """.trimIndent()
        writeFixtureFile(
            projectDir,
            "src/jsMain/kotlin/fixture/PublicWorkerProviderUsage.kt",
            usage,
        )
        writeFixtureFile(
            projectDir,
            "src/wasmJsMain/kotlin/fixture/PublicWorkerProviderUsage.kt",
            usage,
        )
        assertFalse(usage.contains("http://") || usage.contains("https://"))
        assertFalse(usage.contains("sql.js"))

        runGradle(
            projectDir,
            "compileKotlinJs",
            "compileKotlinWasmJs",
            "--stacktrace",
        )
    }

    @Test
    @DisplayName("Plugin generates and compiles migration code for a real project fixture")
    fun pluginGeneratesAndCompilesMigrationCode() {
        val repoRoot = resolveRepoRoot()
        val projectDir = tempDir.resolve("fixture-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            kotlinBody = """
                sourceSets {
                    commonMain.dependencies {
                        implementation("dev.goquick.sqlitenow:core")
                    }
                }
            """.trimIndent(),
            trailingBody = """

                sqliteNow {
                    databases {
                        create("FixtureDatabase") {
                            packageName = "fixture.db"
                            debug = false
                        }
                    }
                }
            """.trimIndent(),
        )

        File(projectDir, "src/commonMain/kotlin/fixture/FakeUsage.kt").apply {
            parentFile.mkdirs()
            writeText(
                """
                    package fixture

                    import fixture.db.FixtureDatabase
                    import fixture.db.PersonQuery
                    import fixture.db.VersionBasedDatabaseMigrations

                    fun createGeneratedDatabase(): FixtureDatabase {
                        val params = PersonQuery.SelectById.Params(id = 1L)
                        check(params.id == 1L)
                        return FixtureDatabase(
                            dbName = ":memory:",
                            migration = VersionBasedDatabaseMigrations(),
                        )
                    }
                """.trimIndent()
            )
        }

        writeSqlFixture(
            projectDir = projectDir,
            dbName = "FixtureDatabase",
            queryName = "selectById",
            querySql = """
                SELECT *
                FROM person
                WHERE id = :id;
            """.trimIndent(),
        )

        val result = runGradle(projectDir, "compileKotlinJvm", "--stacktrace")
        assertTrue(
            result.output.contains(":generateFixtureDatabase"),
            "compileKotlinJvm should depend on the stable generated-source task name"
        )

        val generatedFile = projectDir.resolve(
            "build/generated/sqlitenow/code/FixtureDatabase/fixture/db/VersionBasedDatabaseMigrations.kt"
        )
        assertTrue(generatedFile.exists(), "Generated migration file should exist")
        assertTrue(
            projectDir.resolve(
                "build/generated/sqlitenow/code/FixtureDatabase/fixture/db/FixtureDatabase.kt"
            ).exists(),
            "Generated database source should use the stable SQLiteNow output directory"
        )
        assertTrue(
            generatedFile.readText().contains("private suspend fun migrateToVersion1"),
            "Generated migration helper should be suspend in fixture build"
        )
    }

    @Test
    @DisplayName("Plugin owns local Swift package tasks for a single database")
    fun pluginOwnsLocalSwiftPackageTasksForSingleDatabase() {
        val projectDir = tempDir.resolve("swift-package-project").toFile().apply { mkdirs() }
        val runtimeDir = projectDir.resolve("runtime/SQLiteNowCoreRuntime.xcframework").apply {
            mkdirs()
            resolve("Info.plist").writeText("fake runtime")
        }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            kotlinBody = """
                sourceSets {
                    commonMain.dependencies {
                        implementation("dev.goquick.sqlitenow:core")
                    }
                }
            """.trimIndent(),
            trailingBody = """
                sqliteNow {
                    databases {
                        create("FixtureDatabase") {
                            packageName = "fixture.db"
                            swiftPackage {
                                packageName.set("FixtureDatabaseSQLiteNow")
                                swiftTargetName.set("FixtureDatabaseSQLiteNow")
                                outputDirectory.set(layout.buildDirectory.dir("swift-package/FixtureDatabaseSQLiteNow"))
                                runtimeXcframework.set(layout.projectDirectory.dir("runtime/SQLiteNowCoreRuntime.xcframework"))
                                forbiddenTokenPatterns.set(listOf("KotlinByteArray"))
                            }
                        }
                    }
                }
            """.trimIndent(),
        )
        File(projectDir, "src/commonMain/kotlin/fixture/FakeUsage.kt").apply {
            parentFile.mkdirs()
            writeText(
                """
                    package fixture

                    import fixture.db.FixtureDatabase
                    import fixture.db.PersonQuery
                    import fixture.db.VersionBasedDatabaseMigrations

                    fun createGeneratedDatabase(): FixtureDatabase {
                        val params = PersonQuery.SelectById.Params(id = 1L)
                        check(params.id == 1L)
                        return FixtureDatabase(
                            dbName = ":memory:",
                            migration = VersionBasedDatabaseMigrations(),
                        )
                    }
                """.trimIndent()
            )
        }
        writeSqlFixture(
            projectDir = projectDir,
            dbName = "FixtureDatabase",
            packageMarker = "data_flow",
            queryName = "selectById",
            querySql = """
                SELECT *
                FROM person
                WHERE id = :id;
            """.trimIndent(),
        )

        val result = runGradle(
            projectDir,
            "packageDebugSwiftPackage",
            "validateDebugSwiftPackageManifest",
            "checkDebugSwiftPackageLeaks",
            "compileKotlinJvm",
            "--stacktrace",
        )

        assertTrue(result.output.contains(":generateFixtureDatabase"))
        assertTrue(result.output.contains(":generateFixtureDatabaseDebugSwiftProductSource"))
        assertTrue(result.output.contains(":packageFixtureDatabaseDebugSwiftPackage"))
        assertTrue(runtimeDir.resolve("Info.plist").isFile)

        val packageDir = projectDir.resolve("build/swift-package/FixtureDatabaseSQLiteNow")
        assertTrue(packageDir.resolve("Package.swift").isFile, "Generated Swift package manifest should exist")
        assertTrue(
            packageDir.resolve("Sources/FixtureDatabaseSQLiteNow/FixtureDatabase.swift").isFile,
            "Generated Swift product source should be copied into the package"
        )
        assertTrue(
            packageDir.resolve("Binaries/SQLiteNowCoreRuntime.xcframework/Info.plist").isFile,
            "Runtime XCFramework should be copied into the package"
        )
        assertTrue(
            projectDir.resolve("build/generated/sqlitenow/code/FixtureDatabase/fixture/db/FixtureDatabase.kt").isFile,
            "Swift package generation must not replace the Kotlin generated source task"
        )

        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(packageDir.resolve(".sqlitenow/package-manifest.json")) as Map<String, Any?>
        assertEquals(3, (manifest["manifestVersion"] as Number).toInt())
        assertEquals("1.0.0", manifest["sqliteNowVersion"])
        assertEquals("1.0.0", manifest["generatorVersion"])
        assertEquals("FixtureDatabase", manifest["databaseName"])
        assertEquals("FixtureDatabaseSQLiteNow", manifest["packageName"])
        assertEquals("FixtureDatabaseSQLiteNow", manifest["swiftTargetName"])
        assertEquals("core", manifest["runtimeMode"])
        assertEquals(listOf("SQLiteNowCoreRuntime"), manifest["runtimeBinaryTargets"])
        assertEquals("localXcframework", manifest["runtimeArtifactKind"])
        assertEquals(listOf("Binaries/SQLiteNowCoreRuntime.xcframework"), manifest["runtimeArtifactPaths"])
        assertEquals(null, manifest["runtimeArtifactChecksum"])
        assertEquals(null, manifest["runtimeArtifactVersion"])
        assertEquals(null, manifest["runtimeArtifactUrl"])
        assertEquals("./gradlew :packageDebugSwiftPackage", manifest["generatedBy"])
        val generatorInputs = (manifest["generatorInputs"] as? List<*>)?.map { it as String }
        assertNotNull(generatorInputs, "Swift package metadata should include generator inputs")
        val sqlFiles = projectDir.resolve("src/commonMain/sql/FixtureDatabase")
            .walkTopDown()
            .filter { it.isFile && it.extension == "sql" }
            .sortedBy { it.relativeTo(projectDir).invariantSeparatorsPath }
            .toList()
        val expectedGeneratorConfigInputs = swiftPackageGeneratorConfigInputs(
            databaseName = "FixtureDatabase",
            swiftTargetName = "FixtureDatabaseSQLiteNow",
            runtimeMode = SwiftProductRuntimeMode.CORE,
            runtimeModuleName = "SQLiteNowCoreRuntime",
            frameworkMode = "dynamic",
            minimumPlatforms = SwiftPackageMinimumPlatforms(),
            requestedAppleTargets = DEFAULT_SWIFT_PACKAGE_APPLE_TARGETS,
            forbiddenTokenPatterns = listOf("KotlinByteArray"),
        )
        assertEquals(
            sqlFiles.map { it.relativeTo(projectDir).invariantSeparatorsPath } + expectedGeneratorConfigInputs,
            generatorInputs,
        )
        assertNotNull(manifest["sourceInputDigest"], "Swift package metadata should include source digest")
        assertEquals(
            swiftPackageSourceInputDigest(projectDir, sqlFiles, expectedGeneratorConfigInputs),
            manifest["sourceInputDigest"],
        )

        @Suppress("UNCHECKED_CAST")
        val swiftProductMetadata = JsonSlurper().parse(
            projectDir.resolve("build/generated/sqlitenow/swift-product-metadata/FixtureDatabaseSQLiteNow/metadata.json")
        ) as Map<String, Any?>
        val generatedSwiftFiles = (swiftProductMetadata["generatedSwiftFiles"] as? List<*>)?.map { it as String }
        assertNotNull(generatedSwiftFiles, "Compiler Swift product metadata should include generated Swift files")
        assertTrue("FixtureDatabase.swift" in generatedSwiftFiles)
        assertEquals(emptyList<Any?>(), swiftProductMetadata["syncTables"])
    }

    @Test
    @DisplayName("Plugin packages remote Swift runtime artifacts through the unified runtime artifact DSL")
    fun pluginPackagesRemoteSwiftRuntimeArtifactsThroughUnifiedDsl() {
        val projectDir = tempDir.resolve("swift-remote-runtime-project").toFile().apply { mkdirs() }
        val runtimeUrl = "https://example.com/releases/SQLiteNowCoreRuntime-1.0.0.xcframework.zip"
        val checksum = "a".repeat(64)

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            trailingBody = """
                sqliteNow {
                    databases {
                        create("FixtureDatabase") {
                            packageName = "fixture.db"
                            swiftPackage {
                                packageName.set("FixtureDatabaseSQLiteNow")
                                swiftTargetName.set("FixtureDatabaseSQLiteNow")
                                outputDirectory.set(layout.buildDirectory.dir("swift-package/FixtureDatabaseSQLiteNow"))
                                runtime.set("core")
                                runtimeArtifact.remoteZip("$runtimeUrl")
                                runtimeArtifact.checksum.set("$checksum")
                                runtimeArtifact.sqliteNowVersion.set("1.0.0")
                                forbiddenTokenPatterns.set(listOf("KotlinByteArray"))
                            }
                        }
                    }
                }
            """.trimIndent(),
        )
        writeSqlFixture(
            projectDir = projectDir,
            dbName = "FixtureDatabase",
            packageMarker = "remote_runtime",
        )

        runGradle(
            projectDir,
            "packageDebugSwiftPackage",
            "validateDebugSwiftPackageManifest",
            "--stacktrace",
        )

        val packageDir = projectDir.resolve("build/swift-package/FixtureDatabaseSQLiteNow")
        val packageSwift = packageDir.resolve("Package.swift").readText()
        assertTrue(packageSwift.contains("""url: "$runtimeUrl""""))
        assertTrue(packageSwift.contains("""checksum: "$checksum""""))
        assertFalse(packageDir.resolve("Binaries").exists(), "Remote runtime artifacts must not be copied locally")

        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(packageDir.resolve(".sqlitenow/package-manifest.json")) as Map<String, Any?>
        assertEquals("remoteZip", manifest["runtimeArtifactKind"])
        assertEquals(emptyList<String>(), manifest["runtimeArtifactPaths"])
        assertEquals(checksum, manifest["runtimeArtifactChecksum"])
        assertEquals("1.0.0", manifest["runtimeArtifactVersion"])
        assertEquals(runtimeUrl, manifest["runtimeArtifactUrl"])
    }

    @Test
    @DisplayName("Plugin uses compiler Swift metadata for sync package tables")
    fun pluginUsesCompilerSwiftMetadataForSyncPackageTables() {
        val projectDir = tempDir.resolve("swift-sync-metadata-project").toFile().apply { mkdirs() }
        val runtimeUrl = "https://example.com/releases/SQLiteNowSyncRuntime-1.0.0.xcframework.zip"
        val checksum = "b".repeat(64)

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            kotlinBody = """
                sourceSets {
                    commonMain.dependencies {
                        implementation("dev.goquick.sqlitenow:oversqlite")
                    }
                }
            """.trimIndent(),
            trailingBody = """
                sqliteNow {
                    databases {
                        create("SyncFixtureDatabase") {
                            packageName = "fixture.sync.db"
                            oversqlite = true
                            swiftPackage {
                                packageName.set("SyncFixtureDatabaseSQLiteNow")
                                swiftTargetName.set("SyncFixtureDatabaseSQLiteNow")
                                outputDirectory.set(layout.buildDirectory.dir("swift-package/SyncFixtureDatabaseSQLiteNow"))
                                runtimeArtifact.remoteZip("$runtimeUrl")
                                runtimeArtifact.checksum.set("$checksum")
                                runtimeArtifact.sqliteNowVersion.set("1.0.0")
                                forbiddenTokenPatterns.set(listOf("KotlinByteArray"))
                            }
                        }
                    }
                }
            """.trimIndent(),
        )
        writeSyncSqlFixture(projectDir, dbName = "SyncFixtureDatabase")

        runGradle(
            projectDir,
            "packageDebugSwiftPackage",
            "validateDebugSwiftPackageManifest",
            "--stacktrace",
        )

        val packageDir = projectDir.resolve("build/swift-package/SyncFixtureDatabaseSQLiteNow")
        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(packageDir.resolve(".sqlitenow/package-manifest.json")) as Map<String, Any?>
        val syncTables = manifest["syncTables"] as? List<*>
        assertNotNull(syncTables, "Sync package manifest should include sync table metadata")
        assertEquals(
            listOf(mapOf("tableName" to "docs", "syncKeyColumnName" to "doc_id")),
            syncTables,
        )

        @Suppress("UNCHECKED_CAST")
        val swiftProductMetadata = JsonSlurper().parse(
            projectDir.resolve("build/generated/sqlitenow/swift-product-metadata/SyncFixtureDatabaseSQLiteNow/metadata.json")
        ) as Map<String, Any?>
        assertEquals(syncTables, swiftProductMetadata["syncTables"])
    }

    @Test
    @DisplayName("Plugin extracts manifest-owned worker resources for real JS and wasmJs targets")
    fun pluginExtractsWorkerResourcesForRealWebTargets() {
        val projectDir = tempDir.resolve("real-web-project").toFile().apply { mkdirs() }
        val fakeKlib = projectDir.resolve("libs/sqlitenow-real.klib").also {
            writeFakeKlib(it, marker = "real-web")
        }

        writeSettingsGradle(projectDir)
        writeBuildGradle(
            projectDir,
            """
                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    js {
                        browser()
                    }
                    wasmJs {
                        browser()
                    }

                    sourceSets {
                        commonMain.dependencies {
                            implementation(files("${fakeKlib.toPath().invariantSeparatorsPathString}"))
                        }
                    }
                }
            """.trimIndent(),
        )

        runGradle(
            projectDir,
            "jsProcessResources",
            "wasmJsProcessResources",
            "--stacktrace",
        )

        assertExtractedResources(
            projectDir.resolve("build/processedResources/js/main"),
            marker = "real-web",
        )
        assertExtractedResources(
            projectDir.resolve("build/processedResources/wasmJs/main"),
            marker = "real-web",
        )

        val staleVersionedAsset = projectDir.resolve(
            "build/processedResources/js/main/" +
                "sqlitenow-worker-v1/sqlite-3.52.0-build1/stale-worker.mjs"
        )
        staleVersionedAsset.parentFile.mkdirs()
        staleVersionedAsset.writeText("stale")
        val staleLegacyAssets = listOf(
            "sqlitenow-sqljs.js",
            "sqlitenow-indexeddb.js",
            "sql-wasm.wasm",
        ).map { relative ->
            projectDir.resolve("build/processedResources/wasmJs/main/$relative").also {
                it.writeText("stale")
            }
        }

        runGradle(
            projectDir,
            "jsProcessResources",
            "wasmJsProcessResources",
            "--rerun-tasks",
            "--stacktrace",
        )

        assertFalse(
            staleVersionedAsset.exists(),
            "A resource rerun must remove the previous versioned worker namespace",
        )
        staleLegacyAssets.forEach { asset ->
            assertFalse(
                asset.exists(),
                "A resource rerun must remove obsolete ${asset.name}",
            )
        }
        assertExtractedResources(
            projectDir.resolve("build/processedResources/js/main"),
            marker = "real-web",
        )
        assertExtractedResources(
            projectDir.resolve("build/processedResources/wasmJs/main"),
            marker = "real-web",
        )
    }

    @Test
    @DisplayName("Plugin falls back to wasmJsCompileClasspath when the preferred classpath is absent")
    fun pluginFallsBackToLegacyWasmClasspath() {
        val projectDir = tempDir.resolve("fallback-wasm-project").toFile().apply { mkdirs() }
        val fakeKlib = projectDir.resolve("libs/sqlitenow-fallback.klib").also {
            writeFakeKlib(it, marker = "fallback-wasm")
        }

        writeSettingsGradle(projectDir)
        writeBuildGradle(
            projectDir,
            """
                import org.gradle.language.jvm.tasks.ProcessResources

                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    jvm()
                }

                configurations.create("wasmJsCompileClasspath")

                dependencies {
                    add("wasmJsCompileClasspath", files("${fakeKlib.toPath().invariantSeparatorsPathString}"))
                }

                tasks.register<ProcessResources>("wasmJsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("custom-wasm-resources").get().asFile
                }
            """.trimIndent(),
        )

        runGradle(projectDir, "wasmJsProcessResources", "--stacktrace")

        assertExtractedResources(
            projectDir.resolve("build/custom-wasm-resources"),
            marker = "fallback-wasm",
        )
    }

    @Test
    @DisplayName("Plugin recognizes SQLiteNow project dependencies even when the klib file name does not contain sqlitenow")
    fun pluginRecognizesSqliteNowProjectDependencies() {
        val projectDir = tempDir.resolve("project-dependency-wasm").toFile().apply { mkdirs() }
        val fakeCoreDir = projectDir.resolve("library-core").apply { mkdirs() }
        val fakeKlib = fakeCoreDir.resolve("libs/totally-unrelated.klib").also {
            writeFakeKlib(it, marker = "project-dependency")
        }

        writeMultiProjectSettingsGradle(projectDir)

        writeBuildGradle(
            fakeCoreDir,
            """
                group = "dev.goquick.sqlitenow"
                version = "1.0.0"

                configurations.create("sqlitenowWeb")

                artifacts {
                    add("sqlitenowWeb", file("${fakeKlib.toPath().invariantSeparatorsPathString}"))
                }
            """.trimIndent(),
        )

        val appDir = projectDir.resolve("app").apply { mkdirs() }
        writeBuildGradle(
            appDir,
            """
                import org.gradle.language.jvm.tasks.ProcessResources

                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    jvm()
                }

                configurations.create("wasmJsMainCompileClasspath")
                configurations.create("jsMainCompileClasspath")

                dependencies {
                    add(
                        "wasmJsMainCompileClasspath",
                        project(mapOf("path" to ":library-core", "configuration" to "sqlitenowWeb"))
                    )
                    add(
                        "jsMainCompileClasspath",
                        project(mapOf("path" to ":library-core", "configuration" to "sqlitenowWeb"))
                    )
                }

                tasks.register<ProcessResources>("wasmJsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("custom-wasm-resources").get().asFile
                }
                tasks.register<ProcessResources>("jsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("custom-js-resources").get().asFile
                }
            """.trimIndent(),
        )

        runGradle(
            projectDir,
            ":app:jsProcessResources",
            ":app:wasmJsProcessResources",
            "--stacktrace",
        )

        assertExtractedResources(
            appDir.resolve("build/custom-js-resources"),
            marker = "project-dependency",
        )
        assertExtractedResources(
            appDir.resolve("build/custom-wasm-resources"),
            marker = "project-dependency",
        )
    }

    @Test
    @DisplayName("Plugin extracts worker resources from a published SQLiteNow Core klib")
    fun pluginRecognizesPublishedCoreKlib() {
        val projectDir = tempDir.resolve("published-core-klib").toFile().apply { mkdirs() }
        val repositoryDir = projectDir.resolve("repository")
        writePublishedCoreKlib(
            repositoryDir = repositoryDir,
            version = "1.0.0",
            marker = "published-core",
        )

        writeSettingsGradle(projectDir)
        writeBuildGradle(
            projectDir,
            """
                import org.gradle.language.jvm.tasks.ProcessResources

                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                repositories {
                    maven {
                        url = uri("${repositoryDir.toPath().invariantSeparatorsPathString}")
                    }
                }

                kotlin {
                    jvm()
                }

                configurations.create("jsMainCompileClasspath")

                dependencies {
                    add(
                        "jsMainCompileClasspath",
                        "dev.goquick.sqlitenow:core:1.0.0@klib"
                    )
                }

                tasks.register<ProcessResources>("jsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("published-js-resources").get().asFile
                }
            """.trimIndent(),
        )

        runGradle(projectDir, "jsProcessResources", "--stacktrace")

        assertExtractedResources(
            projectDir.resolve("build/published-js-resources"),
            marker = "published-core",
        )
    }

    @Test
    @DisplayName("Plugin rejects ambiguous worker manifests instead of selecting one by order")
    fun pluginRejectsAmbiguousWorkerManifestKlibs() {
        val projectDir = tempDir.resolve("ambiguous-worker-klibs").toFile().apply { mkdirs() }
        val firstKlib = projectDir.resolve("libs/first.klib").also {
            writeFakeKlib(it, marker = "first")
        }
        val secondKlib = projectDir.resolve("libs/second.klib").also {
            writeFakeKlib(it, marker = "second")
        }

        writeSettingsGradle(projectDir)
        writeBuildGradle(
            projectDir,
            """
                import org.gradle.language.jvm.tasks.ProcessResources

                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    jvm()
                }

                configurations.create("jsMainCompileClasspath")

                dependencies {
                    add(
                        "jsMainCompileClasspath",
                        files(
                            "${firstKlib.toPath().invariantSeparatorsPathString}",
                            "${secondKlib.toPath().invariantSeparatorsPathString}",
                        )
                    )
                }

                tasks.register<ProcessResources>("jsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("ambiguous-js-resources").get().asFile
                }
            """.trimIndent(),
        )

        val result = runGradleAndFail(projectDir, "jsProcessResources", "--stacktrace")

        assertTrue(
            result.output.contains("Ambiguous SQLiteNow Core worker resources"),
            "Ambiguous manifest-owned klibs must fail deterministically. Output:\n${result.output}",
        )
    }

    @Test
    @DisplayName("Plugin keeps generated outputs isolated across multiple databases and regeneration")
    fun pluginKeepsGeneratedOutputsIsolatedAcrossDatabases() {
        val projectDir = tempDir.resolve("multi-db-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir)
        writeJvmBuildGradle(
            projectDir = projectDir,
            trailingBody = """
                sqliteNow {
                    databases {
                        create("AlphaDatabase") {
                            packageName = "fixture.alpha.db"
                        }
                        create("BetaDatabase") {
                            packageName = "fixture.beta.db"
                        }
                    }
                }
            """.trimIndent(),
        )

        writeSqlFixture(projectDir, dbName = "AlphaDatabase", packageMarker = "alpha")
        writeSqlFixture(projectDir, dbName = "BetaDatabase", packageMarker = "beta")

        val firstRun = runGradle(projectDir, "generateAlphaDatabase", "generateBetaDatabase", "--stacktrace")
        assertTrue(firstRun.output.contains(":generateAlphaDatabase"))
        assertTrue(firstRun.output.contains(":generateBetaDatabase"))

        val alphaGeneratedFile = projectDir.resolve(
            "build/generated/sqlitenow/code/AlphaDatabase/fixture/alpha/db/VersionBasedDatabaseMigrations.kt"
        )
        val betaGeneratedFile = projectDir.resolve(
            "build/generated/sqlitenow/code/BetaDatabase/fixture/beta/db/VersionBasedDatabaseMigrations.kt"
        )
        assertTrue(alphaGeneratedFile.exists(), "Alpha database should generate into its own root")
        assertTrue(betaGeneratedFile.exists(), "Beta database should generate into its own root")

        val staleAlphaFile = projectDir.resolve("build/generated/sqlitenow/code/AlphaDatabase/stale.txt").apply {
            parentFile.mkdirs()
            writeText("stale alpha")
        }
        val betaSentinel = projectDir.resolve("build/generated/sqlitenow/code/BetaDatabase/keep.txt").apply {
            parentFile.mkdirs()
            writeText("keep beta")
        }

        runGradle(projectDir, "generateAlphaDatabase", "--rerun-tasks", "--stacktrace")

        assertFalse(staleAlphaFile.exists(), "Regenerating alpha should clear stale files from alpha output only")
        assertTrue(alphaGeneratedFile.exists(), "Alpha output should be regenerated after cleanup")
        assertTrue(betaSentinel.exists(), "Regenerating alpha must not delete beta output")
        assertTrue(betaGeneratedFile.exists(), "Beta generated output should remain intact")
    }

    @Test
    @DisplayName("Plugin fails with an actionable message when the SQL database directory is missing")
    fun pluginFailsWhenSqlDirectoryIsMissing() {
        val projectDir = tempDir.resolve("missing-sql-dir-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir)
        writeJvmBuildGradle(
            projectDir = projectDir,
            trailingBody = """
                sqliteNow {
                    databases {
                        create("MissingDatabase") {
                            packageName = "fixture.missing.db"
                        }
                    }
                }
            """.trimIndent(),
        )

        projectDir.resolve("src/commonMain/sql").mkdirs()
        val result = runGradleAndFail(projectDir, "generateMissingDatabase", "--stacktrace")

        assertTrue(
            result.output.contains("SQL database directory"),
            "Failure output should point to the missing database directory"
        )
        assertTrue(result.output.contains("src/commonMain/sql/MissingDatabase"))
        assertTrue(result.output.contains("not found"))
    }

    @Test
    @DisplayName("Plugin writes debug code paths and recreates the schema database file")
    fun pluginWritesDebugCodeAndRecreatesSchemaDatabase() {
        val projectDir = tempDir.resolve("debug-schema-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir)
        writeJvmBuildGradle(
            projectDir = projectDir,
            trailingBody = """
                sqliteNow {
                    databases {
                        create("DebugDatabase") {
                            packageName = "fixture.debug.db"
                            debug = true
                            schemaDatabaseFile.set(layout.buildDirectory.file("schema/debug.db"))
                        }
                    }
                }
            """.trimIndent(),
        )

        writeSqlFixture(projectDir, dbName = "DebugDatabase", queryName = "selectById", querySql = """
            SELECT *
            FROM person
            WHERE id = :id;
        """.trimIndent())

        runGradle(projectDir, "generateDebugDatabase", "--stacktrace")

        val generatedRoot = projectDir.resolve("build/generated/sqlitenow/code/DebugDatabase")
        val schemaDatabase = projectDir.resolve("build/schema/debug.db")
        assertTrue(schemaDatabase.exists(), "Configured schema database file should be created")
        assertSqliteHeader(schemaDatabase)
        assertGeneratedTreeContains(generatedRoot, "withContextAndTrace")
        assertGeneratedTreeContains(generatedRoot, "sqliteNowLogger.d")

        schemaDatabase.writeText("stale-content")
        runGradle(projectDir, "generateDebugDatabase", "--stacktrace")
        assertSqliteHeader(schemaDatabase)
    }

    @Test
    @DisplayName("Plugin compiles a fixture that combines nested collections, mapTo, and adapter-backed columns")
    fun pluginCompilesComplexGeneratorCompositionFixture() {
        val projectDir = tempDir.resolve("complex-generator-project").toFile().apply { mkdirs() }

        writeSettingsGradle(projectDir, includeRepoBuild = true)
        writeJvmBuildGradle(
            projectDir = projectDir,
            kotlinBody = """
                sourceSets {
                    commonMain.dependencies {
                        implementation("dev.goquick.sqlitenow:core")
                    }
                }
            """.trimIndent(),
            trailingBody = """

                sqliteNow {
                    databases {
                        create("FixtureDatabase") {
                            packageName = "fixture.db"
                        }
                    }
                }
            """.trimIndent(),
        )

        File(projectDir, "src/commonMain/kotlin/fixture/db/SupportTypes.kt").apply {
            parentFile.mkdirs()
            writeText(
                """
                    package fixture.db

                    data class BirthDate(val raw: String)
                """.trimIndent()
            )
        }
        File(projectDir, "src/commonMain/kotlin/fixture/model/PeopleSnapshot.kt").apply {
            parentFile.mkdirs()
            writeText(
                """
                    package fixture.model

                    data class PeopleSnapshot(
                        val personId: Int,
                        val birthDate: fixture.db.BirthDate?,
                        val addresses: List<fixture.db.PersonAddressItem>,
                        val comments: List<fixture.db.CommentItem>,
                        val categories: List<fixture.db.CategoryItem>,
                    )
                """.trimIndent()
            )
        }

        writeComplexCompositionFixture(projectDir)

        runGradle(projectDir, "compileKotlinJvm", "--stacktrace")

        val generatedSummary = projectDir.resolve(
            "build/generated/sqlitenow/code/FixtureDatabase/fixture/db/PersonQuery_SelectSnapshots.kt"
        )
        assertTrue(generatedSummary.exists(), "Complex fixture query file should exist")
    }

    private fun writeSettingsGradle(projectDir: File, includeRepoBuild: Boolean = false) {
        val repoRoot = resolveRepoRoot()
        val pluginBuild = repoRoot.resolve("sqlitenow-gradle-plugin")
        val includeRepoBuildBlock = if (includeRepoBuild) {
            """

                includeBuild("${repoRoot.invariantSeparatorsPathString}") {
                    dependencySubstitution {
                        substitute(module("dev.goquick.sqlitenow:core")).using(project(":library-core"))
                        substitute(module("dev.goquick.sqlitenow:oversqlite")).using(project(":library-oversqlite"))
                    }
                }
            """.trimIndent()
        } else {
            ""
        }

        File(projectDir, "settings.gradle.kts").writeText(
            """
                pluginManagement {
                    repositories {
                        google()
                        mavenCentral()
                        gradlePluginPortal()
                    }
                    includeBuild("${pluginBuild.invariantSeparatorsPathString}")
                }

                dependencyResolutionManagement {
                    repositories {
                        google()
                        mavenCentral()
                    }
                }
                $includeRepoBuildBlock

                rootProject.name = "${projectDir.name}"
            """.trimIndent()
        )
    }

    private fun writeMultiProjectSettingsGradle(projectDir: File) {
        val repoRoot = resolveRepoRoot()
        val pluginBuild = repoRoot.resolve("sqlitenow-gradle-plugin")

        File(projectDir, "settings.gradle.kts").writeText(
            """
                pluginManagement {
                    repositories {
                        google()
                        mavenCentral()
                        gradlePluginPortal()
                    }
                    includeBuild("${pluginBuild.invariantSeparatorsPathString}")
                }

                dependencyResolutionManagement {
                    repositories {
                        google()
                        mavenCentral()
                    }
                }

                rootProject.name = "project-dependency-wasm"
                include(":app")
                include(":library-core")
            """.trimIndent()
        )
    }

    private fun writeJvmBuildGradle(
        projectDir: File,
        kotlinBody: String = "",
        trailingBody: String = "",
    ) {
        writeBuildGradle(
            projectDir,
            """
                import org.jetbrains.kotlin.gradle.dsl.JvmTarget
                import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

                plugins {
                    kotlin("multiplatform") version "2.4.0"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
                    jvm()
                    jvmToolchain(17)

                    compilerOptions {
                        languageVersion.set(KotlinVersion.KOTLIN_2_4)
                    }

                    $kotlinBody
                }

                tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
                    compilerOptions {
                        jvmTarget.set(JvmTarget.JVM_17)
                    }
                }

                $trailingBody
            """.trimIndent(),
        )
    }

    private fun writeBuildGradle(projectDir: File, text: String) {
        writeFixtureFile(projectDir, "build.gradle.kts", text)
    }

    private fun writeFixtureFile(root: File, relativePath: String, text: String) {
        File(root, relativePath).apply {
            parentFile.mkdirs()
            writeText(text)
        }
    }

    private fun writeSqlFixture(
        projectDir: File,
        dbName: String,
        packageMarker: String = dbName.lowercase(),
        queryName: String = "selectAll",
        querySql: String = """
            SELECT *
            FROM person
            ORDER BY id;
        """.trimIndent(),
    ) {
        val dbRoot = projectDir.resolve("src/commonMain/sql/$dbName")
        writeFixtureFile(
            dbRoot,
            "schema/person.sql",
            """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    ${packageMarker}_email TEXT
                );
            """.trimIndent(),
        )
        writeFixtureFile(dbRoot, "queries/person/$queryName.sql", querySql)
        writeFixtureFile(dbRoot, "migration/0001.sql", "ALTER TABLE person ADD COLUMN migrated_$packageMarker TEXT;")
    }

    private fun writeSyncSqlFixture(
        projectDir: File,
        dbName: String,
    ) {
        val dbRoot = projectDir.resolve("src/commonMain/sql/$dbName")
        writeFixtureFile(
            dbRoot,
            "schema/docs.sql",
            """
                -- @@{ enableSync=true, syncKeyColumnName=doc_id }
                CREATE TABLE docs (
                    doc_id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "queries/docs/selectAll.sql",
            """
                -- @@{ queryResult=DocRow }
                SELECT doc_id, title
                FROM docs
                ORDER BY doc_id;
            """.trimIndent(),
        )
    }

    private fun writeComplexCompositionFixture(projectDir: File) {
        val dbRoot = projectDir.resolve("src/commonMain/sql/FixtureDatabase")

        writeFixtureFile(
            dbRoot,
            "schema/person.sql",
            """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    /* @@{ field=birth_date, adapter=custom, propertyType=BirthDate } */
                    birth_date TEXT
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "schema/person_address.sql",
            """
                CREATE TABLE person_address (
                    id INTEGER PRIMARY KEY NOT NULL,
                    person_id INTEGER NOT NULL,
                    city TEXT,
                    FOREIGN KEY (person_id) REFERENCES person(id)
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "schema/comment.sql",
            """
                CREATE TABLE comment (
                    id INTEGER PRIMARY KEY NOT NULL,
                    person_id INTEGER NOT NULL,
                    comment TEXT,
                    FOREIGN KEY (person_id) REFERENCES person(id)
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "schema/category.sql",
            """
                CREATE TABLE category (
                    id INTEGER PRIMARY KEY NOT NULL,
                    name TEXT
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "schema/person_category.sql",
            """
                CREATE TABLE person_category (
                    id INTEGER PRIMARY KEY NOT NULL,
                    person_id INTEGER NOT NULL,
                    category_id INTEGER NOT NULL,
                    FOREIGN KEY (person_id) REFERENCES person(id),
                    FOREIGN KEY (category_id) REFERENCES category(id)
                );
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "queries/person/selectSnapshots.sql",
            """
                /* @@{ queryResult=PeopleSnapshotRow, mapTo=fixture.model.PeopleSnapshot, collectionKey=person_id } */
                SELECT
                    p.id AS person_id,
                    p.birth_date,
                    a.id AS address__id,
                    a.person_id AS address__person_id,
                    a.city AS address__city,
                    c.id AS comment__id,
                    c.person_id AS comment__person_id,
                    c.comment AS comment__comment,
                    cat.id AS category__id,
                    cat.name AS category__name

                  /* @@{ dynamicField=addresses,
                         mappingType=collection,
                         propertyType=List<PersonAddressItem>,
                         sourceTable=a,
                         collectionKey=address__id,
                         aliasPrefix=address__,
                         notNull=true } */

                  /* @@{ dynamicField=comments,
                         mappingType=collection,
                         propertyType=List<CommentItem>,
                         sourceTable=c,
                         collectionKey=comment__id,
                         aliasPrefix=comment__,
                         notNull=true } */

                  /* @@{ dynamicField=categories,
                         mappingType=collection,
                         propertyType=List<CategoryItem>,
                         sourceTable=cat,
                         collectionKey=category__id,
                         aliasPrefix=category__,
                         notNull=true } */
                FROM person p
                LEFT JOIN person_address a ON p.id = a.person_id
                LEFT JOIN comment c ON p.id = c.person_id
                LEFT JOIN person_category pc ON p.id = pc.person_id
                LEFT JOIN category cat ON pc.category_id = cat.id
                WHERE p.id = :personId
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "queries/personAddress/selectAll.sql",
            """
                -- @@{ queryResult=PersonAddressItem }
                SELECT
                    a.id,
                    a.person_id,
                    a.city
                FROM person_address a
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "queries/comment/selectAll.sql",
            """
                -- @@{ queryResult=CommentItem }
                SELECT
                    c.id,
                    c.person_id,
                    c.comment
                FROM comment c
            """.trimIndent(),
        )
        writeFixtureFile(
            dbRoot,
            "queries/category/selectAll.sql",
            """
                -- @@{ queryResult=CategoryItem }
                SELECT
                    c.id,
                    c.name
                FROM category c
            """.trimIndent(),
        )
    }

    private fun gradleRunner(projectDir: File, vararg arguments: String): GradleRunner =
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments(
                *(arguments.toList() + "-Dorg.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=1g").toTypedArray()
            )
            .forwardOutput()

    private fun runGradle(projectDir: File, vararg arguments: String): BuildResult =
        gradleRunner(projectDir, *arguments).build()

    private fun runGradleAndFail(projectDir: File, vararg arguments: String): BuildResult =
        gradleRunner(projectDir, *arguments).buildAndFail()

    private fun writeFakeKlib(file: File, marker: String) {
        file.parentFile.mkdirs()
        ZipOutputStream(file.outputStream().buffered()).use { zip ->
            writeZipEntry(zip, "sqlitenow-sqljs.js", "sqljs-$marker")
            writeZipEntry(zip, "sqlitenow-indexeddb.js", "indexeddb-$marker")
            writeZipEntry(zip, "sql-wasm.wasm", "wasm-$marker")
            val assets = fakeWorkerAssets(marker)
            val manifest = mapOf(
                "schema" to "sqlitenow-worker-asset-manifest-v1",
                "resourceNamespace" to "sqlitenow-worker-v1",
                "cacheVersion" to "sqlite-3.53.0-build1",
                "assets" to assets.map { (path, value) ->
                    mapOf("path" to path, "marker" to value)
                },
            )
            writeZipEntry(
                zip,
                "sqlitenow-worker-v1/asset-manifest.json",
                JsonOutput.prettyPrint(JsonOutput.toJson(manifest)),
            )
            assets.forEach { (path, value) ->
                writeZipEntry(zip, "sqlitenow-worker-v1/$path", value)
            }
        }
    }

    private fun fakeWorkerAssets(marker: String): Map<String, String> = linkedMapOf(
        "client.mjs" to "client-$marker",
        "sqlite-3.53.0-build1/worker.mjs" to "worker-$marker",
        "sqlite-3.53.0-build1/vendor/index.mjs" to "browser-$marker",
        "sqlite-3.53.0-build1/vendor/node.mjs" to "node-$marker",
        "sqlite-3.53.0-build1/vendor/sqlite3.wasm" to "sqlite-wasm-$marker",
        "licenses/sqlite-wasm-Apache-2.0.txt" to "license-$marker",
    )

    private fun writePublishedCoreKlib(
        repositoryDir: File,
        version: String,
        marker: String,
    ) {
        val moduleDir = repositoryDir.resolve("dev/goquick/sqlitenow/core/$version")
        val artifact = moduleDir.resolve("core-$version.klib")
        writeFakeKlib(artifact, marker)
        moduleDir.resolve("core-$version.pom").writeText(
            """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <modelVersion>4.0.0</modelVersion>
                  <groupId>dev.goquick.sqlitenow</groupId>
                  <artifactId>core</artifactId>
                  <version>$version</version>
                  <packaging>klib</packaging>
                </project>
            """.trimIndent(),
        )
    }

    private fun writeZipEntry(zip: ZipOutputStream, name: String, value: String) {
        zip.putNextEntry(ZipEntry(name))
        zip.write(value.toByteArray())
        zip.closeEntry()
    }

    private fun assertExtractedResources(
        root: File,
        marker: String,
    ) {
        val namespaceRoot = root.resolve("sqlitenow-worker-v1")
        val manifestFile = namespaceRoot.resolve("asset-manifest.json")
        assertTrue(manifestFile.isFile, "Authored worker asset manifest should be extracted")
        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(manifestFile) as Map<String, Any?>
        @Suppress("UNCHECKED_CAST")
        val assets = manifest["assets"] as List<Map<String, String>>
        val expectedAssets = fakeWorkerAssets(marker)
        assertEquals(expectedAssets.keys, assets.map { it.getValue("path") }.toSet())
        assets.forEach { asset ->
            val path = asset.getValue("path")
            val file = namespaceRoot.resolve(path)
            assertTrue(file.isFile, "Manifest asset should be extracted: $path")
            assertEquals(asset.getValue("marker"), file.readText(), path)
        }

        val sqlJsFiles = mapOf(
            "sqlitenow-sqljs.js" to "sqljs-$marker",
            "sqlitenow-indexeddb.js" to "indexeddb-$marker",
            "sql-wasm.wasm" to "wasm-$marker",
        )
        sqlJsFiles.forEach { (path, _) ->
            val file = root.resolve(path)
            assertFalse(file.exists(), "$path must not be extracted from Core klibs")
        }
        assertFalse(root.resolve("sqlitenow-sqlite-worker-client.mjs").exists())
        assertFalse(root.resolve("sqlitenow-sqlite-worker.mjs").exists())
        assertFalse(root.resolve("sqlitenow-sqlite-worker").exists())
    }

    private fun assertGeneratedTreeContains(root: File, expectedSnippet: String) {
        val match = root.walkTopDown()
            .filter { it.isFile && it.extension == "kt" }
            .firstOrNull { it.readText().contains(expectedSnippet) }
        assertNotNull(match, "Generated Kotlin tree should contain '$expectedSnippet'")
    }

    private fun assertSqliteHeader(file: File) {
        val header = file.inputStream().use { input ->
            ByteArray(16).also { input.read(it) }
        }
        assertEquals("SQLite format 3\u0000", header.toString(Charsets.US_ASCII))
    }

    private fun resolveRepoRoot(): Path {
        val cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize()
        return if (cwd.resolve("library").toFile().exists() && cwd.resolve("sqlitenow-gradle-plugin").toFile().exists()) {
            cwd
        } else {
            cwd.parent
        }
    }
}
