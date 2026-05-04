package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import java.io.File
import java.nio.file.Path
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

class StatementProcessingHelperTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `process query file rejects multiple statements in a single file`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")

            val queriesDir = tempDir.resolve("queries/person").toFile().apply { mkdirs() }
            val queryFile = File(queriesDir, "invalid.sql").apply {
                writeText(
                    """
                        SELECT * FROM person;
                        SELECT name FROM person;
                    """.trimIndent()
                )
            }

            val helper = StatementProcessingHelper(connection)
            val error = assertThrows<RuntimeException> {
                helper.processQueryFile(queryFile)
            }

            assertTrue(error.message!!.contains("Only one SQL statement per file is supported"))
        }
    }

    @Test
    fun `process query file rejects empty SQL files`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")

            val queriesDir = tempDir.resolve("queries/person").toFile().apply { mkdirs() }
            val queryFile = File(queriesDir, "empty.sql").apply {
                writeText("-- just a comment")
            }

            val helper = StatementProcessingHelper(connection)
            val error = assertThrows<RuntimeException> {
                helper.processQueryFile(queryFile)
            }

            assertTrue(error.message!!.contains("No SQL statements found"))
        }
    }

    @Test
    fun `process queries directory keeps namespace and file ordering deterministic and accepts uppercase SQL extensions`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().execute("CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT)")
            connection.createStatement().execute("CREATE TABLE address(id INTEGER PRIMARY KEY, city TEXT)")

            val queriesDir = tempDir.resolve("queries").toFile().apply { mkdirs() }
            File(queriesDir, "zeta").mkdirs()
            File(queriesDir, "alpha").mkdirs()

            File(queriesDir, "zeta/second.SQL").writeText("SELECT id FROM person;")
            File(queriesDir, "zeta/first.sql").writeText("SELECT name FROM person;")
            File(queriesDir, "alpha/b.sql").writeText("SELECT city FROM address;")
            File(queriesDir, "alpha/a.SQL").writeText("SELECT id FROM address;")

            val helper = StatementProcessingHelper(connection)
            val result = helper.processQueriesDirectory(queriesDir)

            assertEquals(listOf("alpha", "zeta"), result.keys.toList())
            assertEquals(listOf("a", "b"), result.getValue("alpha").map { it.name })
            assertEquals(listOf("first", "second"), result.getValue("zeta").map { it.name })
        }
    }

    @Test
    fun `process queries directory inherits dynamic fields from nested views with alias path rewriting`() {
        val schemaDir = tempDir.resolve("schema").toFile().apply { mkdirs() }
        val queriesDir = tempDir.resolve("queries/person").toFile().apply { mkdirs() }

        File(schemaDir, "schema.sql").writeText(
            """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    category_id INTEGER NOT NULL
                );

                CREATE TABLE category (
                    id INTEGER PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL
                );

                CREATE VIEW category_to_join AS
                SELECT
                    c.id AS category__id,
                    c.title AS category__title
                FROM category c;

                CREATE VIEW person_with_category_view AS
                SELECT
                    p.id AS person__id,
                    cat.category__id,
                    cat.category__title

                    /* @@{ dynamicField=categoryDoc,
                           mappingType=perRow,
                           propertyType=CategoryDoc,
                           sourceTable=cat,
                           aliasPrefix=category__,
                           notNull=true } */

                FROM person p
                LEFT JOIN category_to_join cat ON cat.category__id = p.category_id;
            """.trimIndent()
        )

        File(queriesDir, "selectAll.sql").writeText(
            """
                SELECT
                    pwc.person__id,
                    pwc.category__id,
                    pwc.category__title
                FROM person_with_category_view pwc
                ORDER BY pwc.person__id;
            """.trimIndent()
        )

        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            val inspector = SchemaInspector(schemaDir)
            val tables = inspector.getCreateTableStatements(connection)
            val views = inspector.getCreateViewStatements(connection)
            val helper = StatementProcessingHelper(connection, FieldAnnotationResolver(tables, views))

            val result = helper.processQueriesDirectory(tempDir.resolve("queries").toFile())
            val select = result.getValue("person").single() as AnnotatedSelectStatement
            val inheritedDynamicField = select.fields.firstOrNull { it.src.fieldName == "categoryDoc" }

            assertNotNull(inheritedDynamicField)
            assertEquals("pwc", inheritedDynamicField.annotations.sourceTable)
            assertEquals(listOf("pwc", "cat"), inheritedDynamicField.aliasPath)
        }
    }
}
