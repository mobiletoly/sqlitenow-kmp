package dev.goquick.sqlitenow.gradle

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
                        implementation("dev.goquick.sqlitenow:core:0.7.0-SNAPSHOT")
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

                    class FakeUsage
                """.trimIndent()
            )
        }

        writeSqlFixture(projectDir, dbName = "FixtureDatabase")

        runGradle(projectDir, "compileKotlinJvm", "--stacktrace")

        val generatedFile = projectDir.resolve(
            "build/generated/sqlitenow/code/FixtureDatabase/fixture/db/VersionBasedDatabaseMigrations.kt"
        )
        assertTrue(generatedFile.exists(), "Generated migration file should exist")
        assertTrue(
            generatedFile.readText().contains("private suspend fun migrateToVersion1"),
            "Generated migration helper should be suspend in fixture build"
        )
    }

    @Test
    @DisplayName("Plugin extracts SQLiteNow wasm resources for a real wasmJs target")
    fun pluginExtractsWasmResourcesForRealWasmTarget() {
        val projectDir = tempDir.resolve("real-wasm-project").toFile().apply { mkdirs() }
        val fakeKlib = projectDir.resolve("libs/sqlitenow-real.klib").also {
            writeFakeKlib(it, marker = "real-wasm")
        }

        writeSettingsGradle(projectDir)
        writeBuildGradle(
            projectDir,
            """
                plugins {
                    kotlin("multiplatform") version "2.3.20"
                    id("dev.goquick.sqlitenow")
                }

                group = "fixture"
                version = "1.0.0"

                repositories {
                    google()
                    mavenCentral()
                }

                kotlin {
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

        runGradle(projectDir, "wasmJsProcessResources", "--stacktrace")

        assertExtractedResources(projectDir.resolve("build"), marker = "real-wasm")
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
                    kotlin("multiplatform") version "2.3.20"
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

        assertExtractedResources(projectDir.resolve("build/custom-wasm-resources"), marker = "fallback-wasm")
    }

    @Test
    @DisplayName("Plugin recognizes SQLiteNow project dependencies even when the klib file name does not contain sqlitenow")
    fun pluginRecognizesSqliteNowProjectDependencies() {
        val projectDir = tempDir.resolve("project-dependency-wasm").toFile().apply { mkdirs() }
        val fakeCoreDir = projectDir.resolve("fakecore").apply { mkdirs() }
        val fakeKlib = fakeCoreDir.resolve("libs/totally-unrelated.klib").also {
            writeFakeKlib(it, marker = "project-dependency")
        }

        writeMultiProjectSettingsGradle(projectDir)

        writeBuildGradle(
            fakeCoreDir,
            """
                group = "dev.goquick.sqlitenow"
                version = "1.0.0"

                configurations.create("sqlitenowWasm")

                artifacts {
                    add("sqlitenowWasm", file("${fakeKlib.toPath().invariantSeparatorsPathString}"))
                }
            """.trimIndent(),
        )

        val appDir = projectDir.resolve("app").apply { mkdirs() }
        writeBuildGradle(
            appDir,
            """
                import org.gradle.language.jvm.tasks.ProcessResources

                plugins {
                    kotlin("multiplatform") version "2.3.20"
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

                dependencies {
                    add(
                        "wasmJsMainCompileClasspath",
                        project(mapOf("path" to ":fakecore", "configuration" to "sqlitenowWasm"))
                    )
                }

                tasks.register<ProcessResources>("wasmJsProcessResources") {
                    destinationDir = layout.buildDirectory.dir("custom-wasm-resources").get().asFile
                }
            """.trimIndent(),
        )

        runGradle(projectDir, ":app:wasmJsProcessResources", "--stacktrace")

        assertExtractedResources(appDir.resolve("build/custom-wasm-resources"), marker = "project-dependency")
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
                        implementation("dev.goquick.sqlitenow:core:0.7.0-SNAPSHOT")
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
        val generatedText = generatedSummary.readText()
        assertTrue(generatedText.contains("peopleSnapshotRowMapper"))
        assertTrue(generatedText.contains("sqlValueToBirthDate"))
        assertTrue(generatedText.contains("executeAsOne"))
    }

    private fun writeSettingsGradle(projectDir: File, includeRepoBuild: Boolean = false) {
        val repoRoot = resolveRepoRoot()
        val pluginBuild = repoRoot.resolve("sqlitenow-gradle-plugin")
        val includeRepoBuildBlock = if (includeRepoBuild) {
            """

                includeBuild("${repoRoot.invariantSeparatorsPathString}") {
                    dependencySubstitution {
                        substitute(module("dev.goquick.sqlitenow:core")).using(project(":library"))
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
                include(":fakecore")
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
                    kotlin("multiplatform") version "2.3.20"
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
                        languageVersion.set(KotlinVersion.KOTLIN_2_3)
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
        File(projectDir, "build.gradle.kts").writeText(text)
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
        File(dbRoot, "schema/person.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        ${packageMarker}_email TEXT
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/person/$queryName.sql").apply {
            parentFile.mkdirs()
            writeText(querySql)
        }
        File(dbRoot, "migration/0001.sql").apply {
            parentFile.mkdirs()
            writeText("ALTER TABLE person ADD COLUMN migrated_$packageMarker TEXT;")
        }
    }

    private fun writeComplexCompositionFixture(projectDir: File) {
        val dbRoot = projectDir.resolve("src/commonMain/sql/FixtureDatabase")

        File(dbRoot, "schema/person.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        /* @@{ field=birth_date, adapter=custom, propertyType=BirthDate } */
                        birth_date TEXT
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "schema/person_address.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person_address (
                        id INTEGER PRIMARY KEY NOT NULL,
                        person_id INTEGER NOT NULL,
                        city TEXT,
                        FOREIGN KEY (person_id) REFERENCES person(id)
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "schema/comment.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE comment (
                        id INTEGER PRIMARY KEY NOT NULL,
                        person_id INTEGER NOT NULL,
                        comment TEXT,
                        FOREIGN KEY (person_id) REFERENCES person(id)
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "schema/category.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE category (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "schema/person_category.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    CREATE TABLE person_category (
                        id INTEGER PRIMARY KEY NOT NULL,
                        person_id INTEGER NOT NULL,
                        category_id INTEGER NOT NULL,
                        FOREIGN KEY (person_id) REFERENCES person(id),
                        FOREIGN KEY (category_id) REFERENCES category(id)
                    );
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/person/selectSnapshots.sql").apply {
            parentFile.mkdirs()
            writeText(
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
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/personAddress/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    -- @@{ queryResult=PersonAddressItem }
                    SELECT
                        a.id,
                        a.person_id,
                        a.city
                    FROM person_address a
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/comment/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    -- @@{ queryResult=CommentItem }
                    SELECT
                        c.id,
                        c.person_id,
                        c.comment
                    FROM comment c
                """.trimIndent()
            )
        }
        File(dbRoot, "queries/category/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                    -- @@{ queryResult=CategoryItem }
                    SELECT
                        c.id,
                        c.name
                    FROM category c
                """.trimIndent()
            )
        }
    }

    private fun runGradle(projectDir: File, vararg arguments: String): BuildResult =
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments(*arguments)
            .forwardOutput()
            .build()

    private fun runGradleAndFail(projectDir: File, vararg arguments: String): BuildResult =
        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments(*arguments)
            .forwardOutput()
            .buildAndFail()

    private fun writeFakeKlib(file: File, marker: String) {
        file.parentFile.mkdirs()
        ZipOutputStream(file.outputStream().buffered()).use { zip ->
            writeZipEntry(zip, "sqlitenow-sqljs.js", "sqljs-$marker")
            writeZipEntry(zip, "sqlitenow-indexeddb.js", "indexeddb-$marker")
            writeZipEntry(zip, "sql-wasm.wasm", "wasm-$marker")
        }
    }

    private fun writeZipEntry(zip: ZipOutputStream, name: String, value: String) {
        zip.putNextEntry(ZipEntry(name))
        zip.write(value.toByteArray())
        zip.closeEntry()
    }

    private fun assertExtractedResources(root: File, marker: String) {
        val sqlJs = root.walkTopDown().firstOrNull { it.name == "sqlitenow-sqljs.js" }
        val indexedDb = root.walkTopDown().firstOrNull { it.name == "sqlitenow-indexeddb.js" }
        val wasm = root.walkTopDown().firstOrNull { it.name == "sql-wasm.wasm" }

        assertNotNull(sqlJs, "sqlitenow-sqljs.js should be extracted")
        assertNotNull(indexedDb, "sqlitenow-indexeddb.js should be extracted")
        assertNotNull(wasm, "sql-wasm.wasm should be extracted")
        assertEquals("sqljs-$marker", sqlJs.readText())
        assertEquals("indexeddb-$marker", indexedDb.readText())
        assertEquals("wasm-$marker", wasm.readText())
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
