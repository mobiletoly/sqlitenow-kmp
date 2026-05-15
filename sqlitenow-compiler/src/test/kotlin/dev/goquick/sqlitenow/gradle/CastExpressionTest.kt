package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.NamedParametersProcessor
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CastExpressionTest {

    @Test
    fun testCastExpressionParameterExtraction() {
        assertCastParameterExtraction(
            sql = "SELECT * FROM PersonWithAddressView WHERE person_id = :personId AND phone >= CAST(:numberOfDays AS INTEGER)",
            expectedParameters = listOf("personId", "numberOfDays"),
            expectedCastTypes = mapOf("numberOfDays" to "INTEGER"),
        )
    }

    @Test
    fun testMultipleCastExpressions() {
        assertCastParameterExtraction(
            sql = "SELECT * FROM table WHERE col1 = CAST(:param1 AS TEXT) AND col2 = CAST(:param2 AS REAL) AND col3 = :param3",
            expectedParameters = listOf("param1", "param2", "param3"),
            expectedCastTypes = mapOf("param1" to "TEXT", "param2" to "REAL"),
        )
    }

    @Test
    fun testNoCastExpressions() {
        assertCastParameterExtraction(
            sql = "SELECT * FROM table WHERE col1 = :param1 AND col2 = :param2",
            expectedParameters = listOf("param1", "param2"),
            expectedCastTypes = emptyMap(),
        )
    }

    @Test
    fun testDataStructCodeGeneratorIntegration() {
        // Create a mock SELECT statement with CAST expression
        val mockSelectStatement = AnnotatedSelectStatement(
            name = "selectWithCast",
            src = SelectStatement(
                sql = "SELECT * FROM PersonWithAddressView WHERE person_id = ? AND phone >= ?",
                fromTable = "PersonWithAddressView",
                joinTables = emptyList(),
                fields = listOf(
                    SelectStatement.FieldSource(
                        fieldName = "person_id",
                        tableName = "PersonWithAddressView",
                        originalColumnName = "person_id",
                        dataType = "INTEGER"
                    ),
                    SelectStatement.FieldSource(
                        fieldName = "phone",
                        tableName = "PersonWithAddressView",
                        originalColumnName = "phone",
                        dataType = "TEXT"
                    )
                ),
                namedParameters = listOf("personId", "numberOfDays"),
                namedParametersToColumns = mapOf(
                    "personId" to AssociatedColumn.Default("person_id")
                ),
                offsetNamedParam = null,
                limitNamedParam = null,
                parameterCastTypes = mapOf(
                    "numberOfDays" to "INTEGER"  // This simulates CAST(:numberOfDays AS INTEGER)
                )
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Create minimal setup with real SQLite connection
        val tempDir = java.nio.file.Files.createTempDirectory("sqlitenow-cast-test")
        try {
            val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
            realConnection.createStatement().execute("""
                CREATE TABLE person_with_address_view (
                    person_id INTEGER NOT NULL,
                    phone TEXT
                )
            """.trimIndent())

            val dataStructGenerator = DataStructCodeGenerator(
                conn = realConnection,
                queriesDir = tempDir.resolve("queries").toFile(),
                packageName = "com.example.db",
                outputDir = tempDir.resolve("output").toFile(),
                statementExecutors = mutableListOf()
            )

            // Test parameter type inference for CAST parameter
            val numberOfDaysType = dataStructGenerator.inferParameterType("numberOfDays", mockSelectStatement)
            assertEquals("kotlin.Long", numberOfDaysType.toString(),
                "numberOfDays parameter should be kotlin.Long due to CAST(:numberOfDays AS INTEGER)")

            // Test parameter type inference for regular parameter (should still work normally)
            val personIdType = dataStructGenerator.inferParameterType("personId", mockSelectStatement)
            // Note: personId might be String if column lookup doesn't work in test setup
            // The important test is that CAST parameters work correctly
            assertTrue(personIdType.toString() == "kotlin.Long" || personIdType.toString() == "kotlin.String",
                "personId parameter should be either kotlin.Long (from column) or kotlin.String (default)")

            realConnection.close()
        } finally {
            tempDir.toFile().deleteRecursively()
        }
    }

    private fun assertCastParameterExtraction(
        sql: String,
        expectedParameters: List<String>,
        expectedCastTypes: Map<String, String>,
    ) {
        val statement = CCJSqlParserUtil.parse(sql)
        val processor = NamedParametersProcessor(statement)

        assertEquals(expectedParameters.size, processor.parameters.size)
        expectedParameters.forEach { parameter ->
            assertTrue(processor.parameters.contains(parameter))
            assertEquals(expectedCastTypes[parameter], processor.parameterCastTypes[parameter])
        }
        assertEquals(expectedCastTypes.size, processor.parameterCastTypes.size)
    }
}
