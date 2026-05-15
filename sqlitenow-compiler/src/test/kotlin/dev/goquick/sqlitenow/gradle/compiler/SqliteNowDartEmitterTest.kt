package dev.goquick.sqlitenow.gradle.compiler

import java.io.File
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class SqliteNowDartEmitterTest {

    @TempDir
    lateinit var tempDir: File

    @Test
    fun compilerGeneratesDartVerticalSlice() {
        val sqlDir = createDartFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated")
        val schemaDatabaseFile = tempDir.resolve("dart-schema.db")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "DartDb",
                sqlDirectory = sqlDir,
                packageName = "ignored.for.dart",
                outputDirectory = outputDir,
                schemaDatabaseFile = schemaDatabaseFile,
                backend = SqliteNowCompilerBackend.DART,
            )
        )

        assertEquals(listOf("dart_db.dart"), result.generatedFiles.map { it.name })
        val code = outputDir.resolve("dart_db.dart").readText()
        assertTrue(code.contains("final class DartDb"))
        assertTrue(code.contains("final class PersonRow"))
        assertTrue(code.contains("final class PersonInsertReturningResult"))
        assertTrue(code.contains("final class DartDbAdapters"))
        assertTrue(code.contains("required this.personStatusToSql"))
        assertTrue(code.contains("required this.sqlValueToPersonStatus"))
        assertTrue(code.contains("SelectRunner<PersonRow> selectAll()"))
        assertTrue(code.contains("SelectRunner<PersonRow> selectById(PersonSelectByIdParams params)"))
        assertTrue(code.contains("Future<void> insertOne(PersonInsertOneParams params)"))
        assertTrue(code.contains("Future<List<PersonInsertReturningResult>> insertReturning"))
        assertTrue(code.contains("Future<void> updateName(PersonUpdateNameParams params)"))
        assertTrue(code.contains("Future<void> deleteById(PersonDeleteByIdParams params)"))
        assertTrue(code.contains("SqliteNowMigrationStep.fresh(2"))
        assertTrue(code.contains("CREATE INDEX idx_person_name ON person(name)"))
        assertTrue(code.contains("final List<String> names;"))
        assertTrue(code.contains("final List<String> statuses;"))
        assertTrue(code.contains("name IN (SELECT value FROM json_each(?))"))
        assertTrue(code.contains("parameters: [params.names, ]"))
        assertTrue(code.contains("parameters: [params.statuses.map((value) => _db.adapters.personStatusToSql(value)).toList(), ]"))
        assertTrue(code.contains("affectedTables: const <String>{'active_person', 'person'}"))
        assertTrue(code.contains("affectedTables: const <String>{'person', 'person_note'}"))

        val repeatedParams = code
            .substringAfter("final class PersonSelectRepeatedIdParams")
            .substringBefore("\n}\n")
        assertEquals(1, Regex("final int id;").findAll(repeatedParams).count())
        assertTrue(code.contains("parameters: [params.id, params.id, ]"))

        assertSchemaDatabaseContains(
            schemaDatabaseFile,
            listOf("active_person", "person", "person_note"),
        )
    }

    @Test
    fun compilerGeneratesDartOversqliteMetadataAndHttpClientHelper() {
        val sqlDir = createDartOversqliteFixtureSql(tempDir)
        val outputDir = tempDir.resolve("generated-oversqlite")

        val result = compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = "SyncDartDb",
                sqlDirectory = sqlDir,
                packageName = "ignored.for.dart",
                outputDirectory = outputDir,
                oversqlite = true,
                backend = SqliteNowCompilerBackend.DART,
            )
        )

        assertEquals(listOf("sync_dart_db.dart"), result.generatedFiles.map { it.name })
        val code = outputDir.resolve("sync_dart_db.dart").readText()
        assertTrue(code.contains("import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';"))
        assertTrue(code.contains("static const List<SyncTable> syncTables = <SyncTable>["))
        assertTrue(code.contains("SyncTable(tableName: 'users', syncKeyColumnName: 'uuid'),"))
        assertTrue(code.contains("SyncTable(tableName: 'blob_docs', syncKeyColumnName: 'id'),"))
        val syncTablesBlock = code
            .substringAfter("static const List<SyncTable> syncTables")
            .substringBefore("];")
        assertTrue(!syncTablesBlock.contains("regular_table"))
        assertTrue(code.contains("OversqliteConfig buildOversqliteConfig({"))
        assertTrue(code.contains("syncTables: syncTables,"))
        assertTrue(code.contains("DefaultOversqliteClient newOversqliteClient({"))
        assertTrue(code.contains("required OversqliteHttpClient httpClient,"))
        assertTrue(code.contains("return DefaultOversqliteClient("))
        assertTrue(code.contains("database: _database,"))
        assertTrue(code.contains("httpClient: httpClient,"))
    }

    @Test
    fun dartGenerationFailsClosedWhenEnableSyncUsesOversqliteFalse() {
        assertDartGenerationFails<IllegalStateException>(
            databaseName = "SyncDartDb",
            sqlDir = createDartOversqliteFixtureSql(tempDir),
            outputDir = tempDir.resolve("generated-no-oversqlite"),
            oversqlite = false,
            expectedMessage = "enableSync=true, but oversqlite=false",
        )
    }

    @Test
    fun dartOversqliteMetadataRejectsUnsupportedSyncKey() {
        assertDartGenerationFails<IllegalArgumentException>(
            databaseName = "SyncDartDb",
            sqlDir = createDartInvalidSyncKeyFixtureSql(tempDir),
            outputDir = tempDir.resolve("generated-invalid-sync"),
            oversqlite = true,
            expectedMessage = "TEXT PRIMARY KEY or BLOB PRIMARY KEY",
        )
    }

    @Test
    fun dartOversqliteMetadataRequiresSyncEnabledTable() {
        assertDartGenerationFails<IllegalArgumentException>(
            databaseName = "DartDb",
            sqlDir = createDartFixtureSql(tempDir),
            outputDir = tempDir.resolve("generated-empty-sync"),
            oversqlite = true,
            expectedMessage = "no tables are annotated with enableSync=true",
        )
    }
}

private inline fun <reified T : Throwable> assertDartGenerationFails(
    databaseName: String,
    sqlDir: File,
    outputDir: File,
    oversqlite: Boolean,
    expectedMessage: String,
) {
    val error = assertFailsWith<T> {
        compileSqliteNowDatabase(
            SqliteNowCompilerInput(
                databaseName = databaseName,
                sqlDirectory = sqlDir,
                packageName = "ignored.for.dart",
                outputDirectory = outputDir,
                oversqlite = oversqlite,
                backend = SqliteNowCompilerBackend.DART,
            )
        )
    }

    assertTrue(error.message?.contains(expectedMessage) == true)
}

private fun assertSchemaDatabaseContains(schemaDatabaseFile: File, names: List<String>) {
    assertTrue(
        schemaDatabaseFile.isFile,
        "Expected schema database at ${schemaDatabaseFile.absolutePath}"
    )
    DriverManager.getConnection("jdbc:sqlite:${schemaDatabaseFile.absolutePath}").use { conn ->
        conn.createStatement().use { statement ->
            val actualNames = mutableListOf<String>()
            statement.executeQuery(
                """
                    SELECT name
                    FROM sqlite_master
                    WHERE type IN ('table', 'view')
                    ORDER BY name;
                """.trimIndent()
            ).use { rows ->
                while (rows.next()) {
                    actualNames += rows.getString("name")
                }
            }
            assertEquals(names, actualNames)
        }
    }
}

private fun createDartFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/DartDb")
    sqlDir.resolve("schema/person.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ cascadeNotify = { delete = [person_note] } }
                CREATE TABLE person (
                  id INTEGER PRIMARY KEY NOT NULL,
                  name TEXT NOT NULL,
                  -- @@{ field=status, adapter=custom, propertyType=String }
                  status TEXT NOT NULL,
                  score REAL,
                  avatar BLOB
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("schema/person_note.sql").writeText(
        """
            CREATE TABLE person_note (
              id INTEGER PRIMARY KEY NOT NULL,
              person_id INTEGER NOT NULL,
              body TEXT NOT NULL,
              FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE
            );
        """.trimIndent()
    )
    sqlDir.resolve("schema/active_person.sql").writeText(
        """
            CREATE VIEW active_person AS
            SELECT id, name, status, score, avatar
            FROM person
            WHERE status = 'active';
        """.trimIndent()
    )
    sqlDir.resolve("migration/0002_add_person_index.sql").apply {
        parentFile.mkdirs()
        writeText("CREATE INDEX idx_person_name ON person(name);")
    }
    sqlDir.resolve("queries/person/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{ queryResult=PersonRow }
                SELECT id, name, status, score, avatar
                FROM person
                ORDER BY id;
            """.trimIndent()
        )
    }
    sqlDir.resolve("queries/person/selectActive.sql").writeText(
        """
            -- @@{ queryResult=PersonRow }
            SELECT id, name, status, score, avatar
            FROM active_person
            ORDER BY id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/selectById.sql").writeText(
        """
            -- @@{ queryResult=PersonRow }
            SELECT id, name, status, score, avatar
            FROM person
            WHERE id = :id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/selectByNames.sql").writeText(
        """
            -- @@{ queryResult=PersonRow }
            SELECT p.id, p.name, p.status, p.score, p.avatar
            FROM person p
            WHERE p.name IN :names
            ORDER BY p.id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/selectByStatuses.sql").writeText(
        """
            -- @@{ queryResult=PersonRow }
            SELECT p.id, p.name, p.status, p.score, p.avatar
            FROM person p
            WHERE p.status IN :statuses
            ORDER BY p.id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/selectRepeatedId.sql").writeText(
        """
            -- @@{ queryResult=PersonRow }
            SELECT p.id, p.name, p.status, p.score, p.avatar
            FROM person p
            WHERE p.id = :id OR p.id = :id
            ORDER BY p.id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/insertOne.sql").writeText(
        """
            INSERT INTO person(id, name, status, score, avatar)
            VALUES (:id, :name, :status, :score, :avatar);
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/insertReturning.sql").writeText(
        """
            INSERT INTO person(id, name, status, score, avatar)
            VALUES (:id, :name, :status, :score, :avatar)
            RETURNING id, name, status, score, avatar;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/updateName.sql").writeText(
        """
            UPDATE person
            SET name = :name
            WHERE id = :id;
        """.trimIndent()
    )
    sqlDir.resolve("queries/person/deleteById.sql").writeText(
        """
            DELETE FROM person
            WHERE id = :id;
        """.trimIndent()
    )
    return sqlDir
}

private fun createDartOversqliteFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/SyncDartDb")
    sqlDir.resolve("schema/users.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{enableSync=true, syncKeyColumnName=uuid}
                CREATE TABLE users (
                  uuid TEXT PRIMARY KEY NOT NULL,
                  name TEXT NOT NULL
                );
            """.trimIndent()
        )
    }
    sqlDir.resolve("schema/blob_docs.sql").writeText(
        """
            -- @@{enableSync=true}
            CREATE TABLE blob_docs (
              id BLOB PRIMARY KEY NOT NULL,
              title TEXT NOT NULL
            );
        """.trimIndent()
    )
    sqlDir.resolve("schema/regular_table.sql").writeText(
        """
            CREATE TABLE regular_table (
              id INTEGER PRIMARY KEY NOT NULL,
              note TEXT NOT NULL
            );
        """.trimIndent()
    )
    sqlDir.resolve("queries/users/selectAll.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                SELECT uuid, name
                FROM users
                ORDER BY uuid;
            """.trimIndent()
        )
    }
    return sqlDir
}

private fun createDartInvalidSyncKeyFixtureSql(tempDir: File): File {
    val sqlDir = tempDir.resolve("sql/InvalidSyncDartDb")
    sqlDir.resolve("schema/orders.sql").apply {
        parentFile.mkdirs()
        writeText(
            """
                -- @@{enableSync=true}
                CREATE TABLE orders (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  quantity INTEGER NOT NULL DEFAULT 1
                );
            """.trimIndent()
        )
    }
    return sqlDir
}
