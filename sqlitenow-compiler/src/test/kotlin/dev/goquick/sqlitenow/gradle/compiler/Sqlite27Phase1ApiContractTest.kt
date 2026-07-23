package dev.goquick.sqlitenow.gradle.compiler

import dev.goquick.sqlitenow.gradle.swift.SqliteNowSwiftExportConfig
import java.io.File
import java.util.stream.Stream
import kotlin.test.assertTrue
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir

class Sqlite27Phase1ApiContractTest {
    @TempDir
    lateinit var tempDir: File

    @TestFactory
    fun phase2GeneratedKotlinAndPreservedSwiftContracts(): Stream<DynamicTest> {
        val outputs = generateContractOutputs()
        return contractScenarios().map { scenario ->
            DynamicTest.dynamicTest(scenario.name) {
                val output = outputs.getValue(scenario.output)
                assertTrue(
                    output.contains(scenario.expected),
                    "${scenario.name}: expected generated output to contain:\n${scenario.expected}",
                )
            }
        }.stream()
    }

    private fun generateContractOutputs(): Map<ContractOutput, String> {
        val sqlDirectory = tempDir.resolve("sql/Phase1ContractDatabase")
        writeSqlFixture(sqlDirectory)
        val kotlinOutput = tempDir.resolve("generated-kotlin")
        val swiftOutput = tempDir.resolve(
            "generated-swift/Phase1ContractDatabaseSQLiteNow",
        )

        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "Phase1ContractDatabase",
                sqlDirectory = sqlDirectory,
                packageName = "dev.test.phase1.db",
                outputDirectory = kotlinOutput,
                oversqlite = true,
                swiftExport = SqliteNowSwiftExportConfig(
                    swiftOutputDirectory = swiftOutput,
                    swiftModuleName = "Phase1ContractDatabaseSQLiteNow",
                    frameworkModuleName = "Phase1ContractDatabaseKmp",
                    bridgePackageName = "dev.test.phase1.swiftbridge",
                ),
            ),
        )

        return mapOf(
            ContractOutput.DATABASE_KOTLIN to
                kotlinOutput.resolve("dev/test/phase1/db/Phase1ContractDatabase.kt").readText(),
            ContractOutput.SELECT_KOTLIN to
                kotlinOutput.resolve("dev/test/phase1/db/NoteQuery_SelectAll.kt").readText(),
            ContractOutput.MIGRATION_KOTLIN to
                kotlinOutput.resolve("dev/test/phase1/db/VersionBasedDatabaseMigrations.kt").readText(),
            ContractOutput.SWIFT_BRIDGE_KOTLIN to
                kotlinOutput.resolve(
                    "dev/test/phase1/swiftbridge/Phase1ContractDatabaseBridge.kt",
                ).readText(),
            ContractOutput.SWIFT_OVERLAY to
                swiftOutput.resolve("Phase1ContractDatabase.swift").readText(),
        )
    }

    private fun contractScenarios(): List<ContractScenario> = listOf(
        ContractScenario(
            name = "generated Core database preserves SqliteNowDatabase constructor",
            output = ContractOutput.DATABASE_KOTLIN,
            expected = ") : SqliteNowDatabase(dbName = dbName, migration = migration, debug = debug)",
        ),
        ContractScenario(
            name = "generated Core select runner preserves asList asOne asOneOrNull and flow",
            output = ContractOutput.DATABASE_KOTLIN,
            expected = "public val selectAll: SelectRunners<NoteRow>",
        ),
        ContractScenario(
            name = "generated Core public bind helper uses accepted AndroidX statement type",
            output = ContractOutput.SELECT_KOTLIN,
            expected = "public fun NoteQuery.SelectAll.bindStatementParams(statement: SQLiteStatement)",
        ),
        ContractScenario(
            name = "generated Core public reader uses accepted AndroidX statement type",
            output = ContractOutput.SELECT_KOTLIN,
            expected = "readStatementResult(statement: SQLiteStatement): NoteRow",
        ),
        ContractScenario(
            name = "generated Core imports AndroidX async step",
            output = ContractOutput.SELECT_KOTLIN,
            expected = "import androidx.sqlite.async.step",
        ),
        ContractScenario(
            name = "generated Core execution preserves SafeSQLiteConnection suspend runner",
            output = ContractOutput.SELECT_KOTLIN,
            expected = "executeAsList(conn: SafeSQLiteConnection): List<NoteRow>",
        ),
        ContractScenario(
            name = "generated migration preserves DatabaseMigrations contract",
            output = ContractOutput.MIGRATION_KOTLIN,
            expected = "override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int",
        ),
        ContractScenario(
            name = "generated Oversqlite config factory preserves public call shape",
            output = ContractOutput.DATABASE_KOTLIN,
            expected = "public fun buildOversqliteConfig(",
        ),
        ContractScenario(
            name = "generated Oversqlite client factory preserves public call shape",
            output = ContractOutput.DATABASE_KOTLIN,
            expected = "public fun newOversqliteClient(",
        ),
        ContractScenario(
            name = "generated Oversqlite client factory preserves SafeSQLiteConnection construction boundary",
            output = ContractOutput.DATABASE_KOTLIN,
            expected = "DefaultOversqliteClient(db = this.connection(), config = cfg, http = httpClient,",
        ),
        ContractScenario(
            name = "generated Swift Core bridge preserves suspend list signature",
            output = ContractOutput.SWIFT_BRIDGE_KOTLIN,
            expected = "public suspend fun list(): List<AppNoteRow>",
        ),
        ContractScenario(
            name = "generated Swift Oversqlite bridge preserves generated factory call",
            output = ContractOutput.SWIFT_BRIDGE_KOTLIN,
            expected = "database.newOversqliteClient(",
        ),
        ContractScenario(
            name = "generated Swift Core overlay preserves async query",
            output = ContractOutput.SWIFT_OVERLAY,
            expected = "public func selectAll() -> SQLiteNowSelectQuery<NoteRow>",
        ),
        ContractScenario(
            name = "generated Swift Oversqlite overlay preserves sync client factory",
            output = ContractOutput.SWIFT_OVERLAY,
            expected = "public func makeSyncClient(",
        ),
        ContractScenario(
            name = "generated Swift Oversqlite overlay preserves attach signature",
            output = ContractOutput.SWIFT_OVERLAY,
            expected = "public func attach(userId: String) async throws -> SQLiteNowAttachResult",
        ),
    )

    private fun writeSqlFixture(sqlDirectory: File) {
        sqlDirectory.resolve("schema/note.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                -- @@{enableSync=true, syncKeyColumnName=id}
                CREATE TABLE note (
                    id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT
                );
                """.trimIndent(),
            )
        }
        sqlDirectory.resolve("queries/note/selectAll.sql").apply {
            parentFile.mkdirs()
            writeText(
                """
                -- @@{ queryResult=NoteRow }
                SELECT id,
                       title,
                       body
                FROM note
                ORDER BY id;
                """.trimIndent(),
            )
        }
        sqlDirectory.resolve("queries/note/upsert.sql").writeText(
            """
            INSERT INTO note (id, title, body)
            VALUES (:id, :title, :body)
            ON CONFLICT(id) DO UPDATE SET title = :title, body = :body;
            """.trimIndent(),
        )
    }

    private data class ContractScenario(
        val name: String,
        val output: ContractOutput,
        val expected: String,
    )

    private enum class ContractOutput {
        DATABASE_KOTLIN,
        SELECT_KOTLIN,
        MIGRATION_KOTLIN,
        SWIFT_BRIDGE_KOTLIN,
        SWIFT_OVERLAY,
    }
}
