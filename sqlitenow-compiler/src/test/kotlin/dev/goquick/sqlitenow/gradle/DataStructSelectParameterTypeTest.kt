package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.generator.query.QueryCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.DeleteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.UpdateStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.DeferredStatementExecutor
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.sql.Connection


class DataStructSelectParameterTypeTest : DataStructParameterTestSupport() {
    @Test
    @DisplayName("Test SELECT WHERE clause parameters use correct types from schema annotations")
    fun testSelectWhereParametersUseSchemaTypes() {
        val fixture = createParameterTypeFixture(
            queryFileName = "selectByDateRange.sql",
            querySql = "SELECT * FROM Person WHERE birth_date >= :start AND birth_date <= :end;",
            createTableSql = """
            CREATE TABLE person (
                birth_date TEXT
            )
            """,
            tableColumns = listOf(
                ParameterTableColumn(
                    name = "birth_date",
                    dataType = "TEXT",
                    notNull = false,
                    propertyType = "kotlinx.datetime.LocalDate",
                    adapter = true,
                )
            ),
            selectName = "selectByDateRange",
            selectSql = "SELECT * FROM Person WHERE birth_date >= ? AND birth_date <= ?",
            selectFields = listOf(
                ParameterSelectField(
                    fieldName = "birth_date",
                    dataType = "TEXT",
                    adapter = true,
                )
            ),
            namedParameters = listOf("start", "end"),
            namedParametersToColumns = mapOf(
                "start" to AssociatedColumn.Default("birth_date"),
                "end" to AssociatedColumn.Default("birth_date"),
            ),
        )
        try {
            listOf("start", "end").forEach { parameterName ->
                val parameterType = fixture.generator.inferParameterType(parameterName, fixture.selectStatement)
                assertEquals(
                    "kotlinx.datetime.LocalDate?",
                    parameterType.toString(),
                    "$parameterName parameter should map to nullable LocalDate type"
                )
            }
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test Collection parameter types for IN clause parameters")
    fun testCollectionParameterTypesForInClause() {
        listOf(
            CollectionParameterTypeCase(
                queryFileName = "selectByInClause.sql",
                querySql = "SELECT * FROM Person WHERE last_name IN (:setOfLastNames) AND age IN (:setOfAges);",
                createTableSql = """
                CREATE TABLE person (
                    last_name TEXT NOT NULL,
                    age INTEGER
                )
                """,
                tableColumns = listOf(
                    ParameterTableColumn(name = "last_name", dataType = "TEXT", notNull = true),
                    ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = false),
                ),
                selectName = "selectByInClause",
                selectSql = "SELECT * FROM Person WHERE last_name IN (?) AND age IN (?)",
                selectFields = listOf(
                    ParameterSelectField(fieldName = "last_name", dataType = "TEXT"),
                    ParameterSelectField(fieldName = "age", dataType = "INTEGER"),
                ),
                namedParameters = listOf("setOfLastNames", "setOfAges"),
                namedParametersToColumns = mapOf(
                    "setOfLastNames" to AssociatedColumn.Collection("last_name"),
                    "setOfAges" to AssociatedColumn.Collection("age"),
                ),
                expectations = listOf(
                    ParameterTypeExpectation(
                        parameterName = "setOfLastNames",
                        expectedType = "kotlin.collections.Collection<kotlin.String>",
                        message = "setOfLastNames parameter should map to Collection<String> for IN clause",
                    ),
                    ParameterTypeExpectation(
                        parameterName = "setOfAges",
                        expectedType = "kotlin.collections.Collection<kotlin.Long?>",
                        message = "setOfAges parameter should map to Collection<Long?> for IN clause",
                    )
                ),
            ),
            CollectionParameterTypeCase(
                queryFileName = "selectByDateRange.sql",
                querySql = "SELECT * FROM Person WHERE birth_date IN (:setOfBirthDates);",
                createTableSql = """
                CREATE TABLE person (
                    birth_date TEXT
                )
                """,
                tableColumns = listOf(
                    ParameterTableColumn(
                        name = "birth_date",
                        dataType = "TEXT",
                        notNull = false,
                        propertyType = "kotlinx.datetime.LocalDate",
                    )
                ),
                selectName = "selectByDateRange",
                selectSql = "SELECT * FROM Person WHERE birth_date IN (?)",
                selectFields = listOf(
                    ParameterSelectField(fieldName = "birth_date", dataType = "TEXT")
                ),
                namedParameters = listOf("setOfBirthDates"),
                namedParametersToColumns = mapOf(
                    "setOfBirthDates" to AssociatedColumn.Collection("birth_date")
                ),
                expectations = listOf(
                    ParameterTypeExpectation(
                        parameterName = "setOfBirthDates",
                        expectedType = "kotlin.collections.Collection<kotlinx.datetime.LocalDate?>",
                        message = "setOfBirthDates parameter should map to Collection<LocalDate?> for IN clause with custom property type",
                    )
                ),
            )
        ).forEach { case ->
            val fixture = createParameterTypeFixture(
                queryFileName = case.queryFileName,
                querySql = case.querySql,
                createTableSql = case.createTableSql,
                tableColumns = case.tableColumns,
                selectName = case.selectName,
                selectSql = case.selectSql,
                selectFields = case.selectFields,
                namedParameters = case.namedParameters,
                namedParametersToColumns = case.namedParametersToColumns,
            )
            try {
                case.expectations.forEach { expectation ->
                    val parameterType = fixture.generator.inferParameterType(
                        expectation.parameterName,
                        fixture.selectStatement,
                    )
                    assertEquals(expectation.expectedType, parameterType.toString(), expectation.message)
                }
            } finally {
                fixture.cleanup()
            }
        }
    }

    @Test
    @DisplayName("Test LIMIT and OFFSET parameters generate Long type regardless of parameter names")
    fun testLimitOffsetParametersGenerateLongType() {
        // Create a mock SELECT statement with LIMIT and OFFSET parameters
        val mockSelectStatement = AnnotatedSelectStatement(
            name = "selectWithPagination",
            src = SelectStatement(
                sql = "SELECT * FROM users LIMIT :pageSize OFFSET :skipRows",
                fromTable = "users",
                joinTables = emptyList(),
                fields = listOf(
                    SelectStatement.FieldSource(
                        fieldName = "id",
                        tableName = "users",
                        originalColumnName = "id",
                        dataType = "INTEGER"
                    ),
                    SelectStatement.FieldSource(
                        fieldName = "name",
                        tableName = "users",
                        originalColumnName = "name",
                        dataType = "TEXT"
                    )
                ),
                namedParameters = listOf("pageSize", "skipRows"),
                namedParametersToColumns = emptyMap(), // LIMIT/OFFSET params don't map to columns
                offsetNamedParam = "skipRows", // This parameter is used in OFFSET clause
                limitNamedParam = "pageSize"   // This parameter is used in LIMIT clause
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            fields = listOf(
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "id",
                        tableName = "users",
                        originalColumnName = "id",
                        dataType = "INTEGER"
                    ),
                    annotations = FieldAnnotationOverrides(
                        propertyName = null,
                        propertyType = null,
                        notNull = null,
                        adapter = false
                    )
                ),
                AnnotatedSelectStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "name",
                        tableName = "users",
                        originalColumnName = "name",
                        dataType = "TEXT"
                    ),
                    annotations = FieldAnnotationOverrides(
                        propertyName = null,
                        propertyType = null,
                        notNull = null,
                        adapter = false
                    )
                )
            )
        )

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT)")

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = tempDir.resolve("queries").toFile(),
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = tempDir.resolve("output").toFile()
        )

        // Test parameter type inference for LIMIT parameter
        val limitParamType = dataStructGenerator.inferParameterType("pageSize", mockSelectStatement)
        assertEquals(
            "kotlin.Long", limitParamType.toString(),
            "pageSize parameter (LIMIT) should be kotlin.Long type"
        )

        // Test parameter type inference for OFFSET parameter
        val offsetParamType = dataStructGenerator.inferParameterType("skipRows", mockSelectStatement)
        assertEquals(
            "kotlin.Long", offsetParamType.toString(),
            "skipRows parameter (OFFSET) should be kotlin.Long type"
        )

        // Test parameter type inference for regular parameter (should still be String if no column found)
        val regularParamType = dataStructGenerator.inferParameterType("someOtherParam", mockSelectStatement)
        assertEquals(
            "kotlin.String", regularParamType.toString(),
            "Regular parameters should still default to kotlin.String type"
        )

        realConnection.close()
    }

}
