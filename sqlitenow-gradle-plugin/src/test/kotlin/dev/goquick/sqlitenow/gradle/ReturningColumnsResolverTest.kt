package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import java.io.File
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.update.Update
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReturningColumnsResolverTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `resolve columns supports update returning specific columns`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            val schemaDir = writeSchemaFixture()
            val generatorContext = createGeneratorContext(schemaDir, connection)
            val statement = annotatedUpdateStatement(
                connection,
                """
                    UPDATE person
                    SET created_at = :createdAt
                    RETURNING doc_id
                """.trimIndent(),
            )

            val columns = ReturningColumnsResolver.resolveColumns(generatorContext, statement)
            val fields = ReturningColumnsResolver.createSelectLikeFields(generatorContext, statement)

            assertEquals(listOf("doc_id"), columns.map { it.src.name })
            assertEquals(1, fields.size)
            assertEquals("documentId", fields.single().annotations.propertyName)
        }
    }

    @Test
    fun `create select like fields supports delete returning star`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            val schemaDir = writeSchemaFixture()
            val generatorContext = createGeneratorContext(schemaDir, connection)
            val statement = annotatedDeleteStatement(
                connection,
                """
                    DELETE FROM person
                    WHERE id = :personId
                    RETURNING *
                """.trimIndent(),
            )

            val fields = ReturningColumnsResolver.createSelectLikeFields(generatorContext, statement)

            assertEquals(listOf("id", "doc_id", "created_at"), fields.map { it.src.fieldName })
            assertEquals("documentId", fields.first { it.src.fieldName == "doc_id" }.annotations.propertyName)
        }
    }

    @Test
    fun `update returning alias remains unsupported with clear message`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().execute(
                "CREATE TABLE person(id INTEGER PRIMARY KEY, doc_id TEXT NOT NULL, created_at TEXT)"
            )

            val error = assertThrows<IllegalArgumentException> {
                UpdateStatement.parse(
                    CCJSqlParserUtil.parse(
                        "UPDATE person SET created_at = :createdAt RETURNING doc_id AS alias_doc_id"
                    ) as Update,
                    connection,
                )
            }

            assertTrue(error.message!!.contains("RETURNING clause with aliases is currently not supported"))
        }
    }

    @Test
    fun `delete returning expression remains unsupported with clear message`() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().execute(
                "CREATE TABLE person(id INTEGER PRIMARY KEY, doc_id TEXT NOT NULL, created_at TEXT)"
            )

            val error = assertThrows<IllegalArgumentException> {
                DeleteStatement.parse(
                    CCJSqlParserUtil.parse(
                        "DELETE FROM person WHERE id = :personId RETURNING upper(doc_id)"
                    ) as Delete,
                    connection,
                )
            }

            assertTrue(error.message!!.contains("RETURNING clause with expressions is currently not supported"))
        }
    }

    private fun writeSchemaFixture(): File {
        val schemaDir = tempDir.resolve("schema").toFile().apply { mkdirs() }
        File(schemaDir, "person.sql").writeText(
            """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY NOT NULL,
                    -- @@{field=doc_id, propertyName=documentId}
                    doc_id TEXT NOT NULL,
                    created_at TEXT
                );
            """.trimIndent()
        )
        return schemaDir
    }

    private fun createGeneratorContext(schemaDir: File, connection: Connection): GeneratorContext {
        val inspector = SchemaInspector(schemaDir)
        return GeneratorContext(
            packageName = "fixture.db",
            outputDir = tempDir.resolve("generated").toFile(),
            createTableStatements = inspector.getCreateTableStatements(connection),
            createViewStatements = inspector.getCreateViewStatements(connection),
            nsWithStatements = emptyMap(),
        )
    }

    private fun annotatedUpdateStatement(connection: Connection, sql: String): AnnotatedExecuteStatement =
        AnnotatedExecuteStatement(
            name = "UpdatePerson",
            src = UpdateStatement.parse(CCJSqlParserUtil.parse(sql) as Update, connection),
            annotations = defaultStatementAnnotations(),
        )

    private fun annotatedDeleteStatement(connection: Connection, sql: String): AnnotatedExecuteStatement =
        AnnotatedExecuteStatement(
            name = "DeletePerson",
            src = DeleteStatement.parse(CCJSqlParserUtil.parse(sql) as Delete, connection),
            annotations = defaultStatementAnnotations(),
        )

    private fun defaultStatementAnnotations() = StatementAnnotationOverrides(
        name = null,
        propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
        queryResult = null,
        collectionKey = null,
    )
}
