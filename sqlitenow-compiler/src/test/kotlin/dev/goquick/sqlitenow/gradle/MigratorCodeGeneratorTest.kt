package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.database.MigratorCodeGenerator
import dev.goquick.sqlitenow.gradle.database.MigrationInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SQLBatchInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertTrue

class MigratorCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var outputDir: File

    @BeforeEach
    fun setup() {
        outputDir = File(tempDir.toFile(), "output")
        outputDir.mkdir()
    }

    @TestFactory
    @DisplayName("MigratorCodeGenerator generation scenarios")
    fun generationScenarios(): List<DynamicTest> {
        val createTableSql = """
            -- This is a test table
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );

            -- This is another test table
            CREATE TABLE another_table (
                id INTEGER PRIMARY KEY NOT NULL,
                test_table_id INTEGER NOT NULL,
                value TEXT NOT NULL,
                FOREIGN KEY (test_table_id) REFERENCES test_table(id) ON DELETE CASCADE
            );
        """.trimIndent()

        val usersWithEmailSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL
            );
        """.trimIndent()

        val adminUserBatchSql = """
            INSERT INTO users (id, name, email) VALUES (1, 'Admin', 'admin@example.com');
            CREATE INDEX idx_users_email ON users(email);
        """.trimIndent()

        val usersSql = """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );
        """.trimIndent()

        return listOf(
            MigrationGenerationCase(
                displayName = "generates base migration class and SQL batches",
                schemaFiles = mapOf("init.sql" to createTableSql),
                batchFiles = mapOf("init.sql" to createTableSql),
                migrationFiles = mapOf("0001.sql" to "CREATE TABLE migration_test (id INTEGER);"),
                expectedSnippets = listOf(
                    "class VersionBasedDatabaseMigrations" to "File should contain the class declaration",
                    "override suspend fun applyMigration" to "File should contain the applyMigration function",
                    "private suspend fun executeAllSql" to "File should contain the executeAllSql function",
                    "migrateToVersion1" to "File should contain migration function for version 1",
                    "CREATE TABLE test_table" to "File should contain the CREATE TABLE statement",
                    "CREATE TABLE another_table" to "File should contain the second CREATE TABLE statement",
                    "CREATE TABLE migration_test" to "File should contain the migration statement",
                ),
            ),
            MigrationGenerationCase(
                displayName = "combines schema init and migration inspectors",
                schemaFiles = mapOf("schema.sql" to usersWithEmailSql),
                batchFiles = mapOf("batch.sql" to adminUserBatchSql),
                migrationFiles = mapOf("0001.sql" to "CREATE TABLE migration_test (id INTEGER);"),
                expectedSnippets = listOf(
                    "CREATE TABLE users" to "Should contain schema statement",
                    "INSERT INTO users" to "Should contain batch statement",
                    "CREATE INDEX idx_users_email" to "Should contain batch index statement",
                    "CREATE TABLE migration_test" to "Should contain migration statement",
                    "Execute schema statements first" to "Should mention schema statements in comments",
                    "Execute init statements" to "Should mention init statements in comments",
                    "if (currentVersion == -1)" to "Should contain initial setup logic",
                ),
                expectedVersionFlow = listOf(1),
                expectedLatestVersion = 1,
            ),
            MigrationGenerationCase(
                displayName = "generates incremental checks for multiple migration versions",
                schemaFiles = mapOf("schema.sql" to usersSql),
                batchFiles = mapOf("batch.sql" to "INSERT INTO users (id, name) VALUES (1, 'Admin');"),
                migrationFiles = mapOf(
                    "0001.sql" to "ALTER TABLE users ADD COLUMN email TEXT;",
                    "0003.sql" to "CREATE INDEX idx_users_email ON users(email);",
                    "0005.sql" to "ALTER TABLE users ADD COLUMN created_at TIMESTAMP;",
                ),
                expectedVersionFlow = listOf(1, 3, 5),
                expectedLatestVersion = 5,
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val fileContent = generatedMigrationCode(
                    schemaFiles = case.schemaFiles,
                    batchFiles = case.batchFiles,
                    migrationFiles = case.migrationFiles,
                ).content

                assertGeneratedContentContains(fileContent, *case.expectedSnippets.toTypedArray())
                assertMigrationVersionFlow(fileContent, case.expectedVersionFlow, case.expectedLatestVersion)
            }
        }
    }

    @Test
    @DisplayName("Test MigratorCodeGenerator preserves percent signs in SQL literals")
    fun testGenerateCodeWithPercentSignsInSql() {
        val schemaSql = """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            );
        """.trimIndent()

        val fileContent = generatedMigrationCode(
            schemaFiles = mapOf("schema.sql" to schemaSql),
            migrationFiles = mapOf(
                "0001.sql" to "ALTER TABLE events ADD COLUMN updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));"
            ),
        ).content

        assertTrue(
            fileContent.contains("strftime('%Y-%m-%dT%H:%M:%SZ', 'now')"),
            "Generated code should preserve strftime format strings with percent signs"
        )
    }

    @Test
    @DisplayName("Generated migration helpers are suspend and compile with migration files present")
    fun generatedMigrationHelpersAreSuspendAndCompile() {
        val generated = generatedMigrationCode(
            schemaFiles = mapOf(
                "schema.sql" to """
                    CREATE TABLE users (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent()
            ),
            migrationFiles = mapOf("0001.sql" to "ALTER TABLE users ADD COLUMN email TEXT;"),
        )

        assertTrue(
            generated.content.contains("private suspend fun migrateToVersion1"),
            "Generated migration helper should be suspend"
        )

        compileGeneratedMigrator(generated.file)
    }

    private fun generatedMigrationCode(
        schemaFiles: Map<String, String>,
        batchFiles: Map<String, String> = emptyMap(),
        migrationFiles: Map<String, String>,
    ): GeneratedMigrationCode {
        val schemaDir = writeSqlDirectory("schema", schemaFiles)
        val batchDir = writeSqlDirectory("batch", batchFiles)
        val migrationDir = writeSqlDirectory("migrations", migrationFiles)

        MigratorCodeGenerator(
            schemaInspector = SchemaInspector(schemaDirectory = schemaDir),
            sqlBatchInspector = SQLBatchInspector(sqlDirectory = batchDir, mandatory = false),
            migrationInspector = MigrationInspector(sqlDirectory = migrationDir),
            packageName = "com.example.db",
            outputDir = outputDir,
        ).generateCode()

        val outputFile = File(outputDir, "com/example/db/VersionBasedDatabaseMigrations.kt")
        assertTrue(outputFile.exists(), "Output file should be created")
        return GeneratedMigrationCode(file = outputFile, content = outputFile.readText())
    }

    private fun writeSqlDirectory(name: String, files: Map<String, String>): File =
        File(tempDir.toFile(), name).apply {
            mkdir()
            files.forEach { (fileName, sql) -> File(this, fileName).writeText(sql) }
        }

    private fun assertGeneratedContentContains(fileContent: String, vararg expectations: Pair<String, String>) {
        expectations.forEach { (snippet, message) ->
            assertTrue(fileContent.contains(snippet), message)
        }
    }

    private fun assertMigrationVersionFlow(fileContent: String, versions: List<Int>, latestVersion: Int?) {
        versions.forEach { version ->
            assertGeneratedContentContains(
                fileContent,
                "migrateToVersion$version" to "Should contain migration function for version $version",
                "if (currentVersion < $version)" to "Should contain check for version $version",
                "migrateToVersion$version(conn)" to "Should call migration function for version $version",
            )
        }

        if (latestVersion != null) {
            assertTrue(
                fileContent.contains("return@withContext $latestVersion") || fileContent.contains("    $latestVersion"),
                "Should return latest version $latestVersion",
            )
        }
    }

    private data class MigrationGenerationCase(
        val displayName: String,
        val schemaFiles: Map<String, String>,
        val batchFiles: Map<String, String> = emptyMap(),
        val migrationFiles: Map<String, String>,
        val expectedSnippets: List<Pair<String, String>> = emptyList(),
        val expectedVersionFlow: List<Int> = emptyList(),
        val expectedLatestVersion: Int? = null,
    )

    private data class GeneratedMigrationCode(val file: File, val content: String)

    private fun compileGeneratedMigrator(generatedFile: File) {
        val projectDir = tempDir.resolve("compile-project").toFile().apply { mkdirs() }
        File(projectDir, "settings.gradle.kts").writeText(
            """
                pluginManagement {
                    repositories {
                        mavenCentral()
                        gradlePluginPortal()
                    }
                }
                rootProject.name = "migration-compile-check"
            """.trimIndent()
        )
        File(projectDir, "build.gradle.kts").writeText(
            """
                import org.jetbrains.kotlin.gradle.dsl.JvmTarget
                import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

                plugins {
                    kotlin("jvm") version "2.3.20"
                }

                repositories {
                    mavenCentral()
                }

                kotlin {
                    jvmToolchain(17)
                    compilerOptions {
                        jvmTarget.set(JvmTarget.JVM_17)
                        languageVersion.set(KotlinVersion.KOTLIN_2_3)
                    }
                }
            """.trimIndent()
        )

        val coreDir = File(projectDir, "src/main/kotlin/dev/goquick/sqlitenow/core").apply { mkdirs() }
        File(coreDir, "DatabaseMigrations.kt").writeText(
            """
                package dev.goquick.sqlitenow.core

                interface DatabaseMigrations {
                    suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int
                }
            """.trimIndent()
        )
        File(coreDir, "SafeSQLiteConnection.kt").writeText(
            """
                package dev.goquick.sqlitenow.core

                class SafeSQLiteConnection {
                    suspend fun execSQL(sql: String) = Unit

                    suspend fun <T> withExclusiveAccess(block: suspend () -> T): T = block()
                }
            """.trimIndent()
        )

        val destination = File(projectDir, "src/main/kotlin/com/example/db/VersionBasedDatabaseMigrations.kt")
        destination.parentFile.mkdirs()
        destination.writeText(generatedFile.readText())

        GradleRunner.create()
            .withProjectDir(projectDir)
            .withArguments("compileKotlin", "--stacktrace")
            .forwardOutput()
            .build()
    }
}
