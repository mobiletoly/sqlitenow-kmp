package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.generator.query.QueryCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DataStructInsertParameterTypeTest : DataStructParameterTestSupport() {
    @TestFactory
    fun insertParameterTypeInference(): List<DynamicTest> = listOf(
        InsertParameterInferenceCase(
            displayName = "column names map camelCase parameters to table column types",
            querySql = "INSERT INTO Person (first_name, created_at) VALUES (:firstName, :createdAt);",
            createTableSql = """
                CREATE TABLE person (
                    created_at INTEGER NOT NULL,
                    first_name VARCHAR(255) NOT NULL
                )
            """,
            tableColumns = listOf(
                ParameterTableColumn(name = "created_at", dataType = "INTEGER", notNull = true),
                ParameterTableColumn(name = "first_name", dataType = "VARCHAR(255)", notNull = true),
            ),
            insertSql = "INSERT INTO person (first_name, created_at) VALUES (?, ?)",
            namedParameters = listOf("firstName", "createdAt"),
            parameterToColumnNames = mapOf(
                "firstName" to "first_name",
                "createdAt" to "created_at",
            ),
            expectations = listOf(
                ParameterTypeExpectation(
                    parameterName = "createdAt",
                    expectedType = "kotlin.Long",
                    message = "createdAt parameter should map to Long type from created_at INTEGER column",
                ),
                ParameterTypeExpectation(
                    parameterName = "firstName",
                    expectedType = "kotlin.String",
                    message = "firstName parameter should map to String type from first_name VARCHAR column",
                ),
            ),
        ),
        InsertParameterInferenceCase(
            displayName = "adapter annotation supplies the parameter property type",
            querySql = "INSERT INTO Person (created_at) VALUES (:createdAt);",
            createTableSql = """
                CREATE TABLE person (
                    created_at INTEGER NOT NULL
                )
            """,
            tableColumns = listOf(
                ParameterTableColumn(
                    name = "created_at",
                    dataType = "INTEGER",
                    notNull = true,
                    propertyType = "kotlinx.datetime.LocalDateTime",
                    adapter = true,
                )
            ),
            insertSql = "INSERT INTO person (created_at) VALUES (?)",
            namedParameters = listOf("createdAt"),
            parameterToColumnNames = mapOf("createdAt" to "created_at"),
            expectations = listOf(
                ParameterTypeExpectation(
                    parameterName = "createdAt",
                    expectedType = "kotlinx.datetime.LocalDateTime",
                    message = "createdAt parameter should map to LocalDateTime type due to propertyType annotation",
                )
            ),
        ),
        InsertParameterInferenceCase(
            displayName = "mixed parameters and literals infer parameter types from target columns",
            querySql = """
                INSERT INTO Person (email, first_name, last_name, age, phone, birth_date, created_at, notes)
                VALUES (:email, :firstName, :lastName, 1234, :phone, :birthDate, :myCreatedAt, :notes);
            """,
            createTableSql = """
                CREATE TABLE person (
                    email TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age INTEGER NOT NULL DEFAULT 0,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT NOT NULL DEFAULT current_timestamp,
                    notes TEXT
                )
            """,
            tableColumns = listOf(
                ParameterTableColumn(name = "email", dataType = "TEXT", notNull = true),
                ParameterTableColumn(name = "first_name", dataType = "TEXT", notNull = true),
                ParameterTableColumn(name = "last_name", dataType = "TEXT", notNull = true),
                ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = true),
                ParameterTableColumn(name = "phone", dataType = "TEXT", notNull = false),
                ParameterTableColumn(
                    name = "birth_date",
                    dataType = "TEXT",
                    notNull = false,
                    propertyType = "kotlinx.datetime.LocalDate",
                ),
                ParameterTableColumn(
                    name = "created_at",
                    dataType = "TEXT",
                    notNull = true,
                    propertyType = "kotlinx.datetime.LocalDateTime",
                ),
                ParameterTableColumn(name = "notes", dataType = "TEXT", notNull = false),
            ),
            insertSql = """
                INSERT INTO Person (email, first_name, last_name, age, phone, birth_date, created_at, notes)
                VALUES (?, ?, ?, 1234, ?, ?, ?, ?)
            """,
            namedParameters = listOf("email", "firstName", "lastName", "phone", "birthDate", "myCreatedAt", "notes"),
            parameterToColumnNames = mapOf(
                "email" to "email",
                "firstName" to "first_name",
                "lastName" to "last_name",
                "age" to "age",
                "phone" to "phone",
                "birthDate" to "birth_date",
                "myCreatedAt" to "created_at",
                "notes" to "notes",
            ),
            expectations = listOf(
                ParameterTypeExpectation("email", "kotlin.String", "email parameter should map to non-nullable String"),
                ParameterTypeExpectation("phone", "kotlin.String?", "phone parameter should map to nullable String"),
                ParameterTypeExpectation("birthDate", "kotlinx.datetime.LocalDate?", "birthDate parameter should map to nullable LocalDate"),
                ParameterTypeExpectation("firstName", "kotlin.String", "firstName parameter should map to non-nullable String"),
                ParameterTypeExpectation("notes", "kotlin.String?", "notes parameter should map to nullable String"),
            ),
        ),
        InsertParameterInferenceCase(
            displayName = "literal values do not shift parameter-to-column mapping",
            querySql = """
                INSERT INTO Person (email, first_name, last_name, age, phone, birth_date, created_at, notes)
                VALUES (:email, :firstName, :lastName, 1234, '000-555-777', :birthDate, :myCreatedAt, :notes);
            """,
            createTableSql = """
                CREATE TABLE person (
                    email TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT,
                    age INTEGER NOT NULL DEFAULT 0,
                    phone TEXT,
                    birth_date TEXT,
                    created_at TEXT NOT NULL DEFAULT current_timestamp,
                    notes TEXT
                )
            """,
            tableColumns = listOf(
                ParameterTableColumn(name = "email", dataType = "TEXT", notNull = true),
                ParameterTableColumn(name = "first_name", dataType = "TEXT", notNull = true),
                ParameterTableColumn(name = "last_name", dataType = "TEXT", notNull = false),
                ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = true),
                ParameterTableColumn(name = "phone", dataType = "TEXT", notNull = false),
                ParameterTableColumn(
                    name = "birth_date",
                    dataType = "TEXT",
                    notNull = false,
                    propertyType = "kotlinx.datetime.LocalDate",
                ),
                ParameterTableColumn(
                    name = "created_at",
                    dataType = "TEXT",
                    notNull = true,
                    propertyType = "kotlinx.datetime.LocalDateTime",
                ),
                ParameterTableColumn(
                    name = "notes",
                    dataType = "TEXT",
                    notNull = false,
                    propertyType = "PersonNote",
                ),
            ),
            insertSql = """
                INSERT INTO Person (email, first_name, last_name, age, phone, birth_date, created_at, notes)
                VALUES (?, ?, ?, 1234, '000-555-777', ?, ?, ?)
            """,
            namedParameters = listOf("mySelectAge", "email", "firstName", "lastName", "birthDate", "myCreatedAt", "notes"),
            parameterToColumnNames = mapOf(
                "email" to "email",
                "firstName" to "first_name",
                "lastName" to "last_name",
                "birthDate" to "birth_date",
                "myCreatedAt" to "created_at",
                "notes" to "notes",
            ),
            expectations = listOf(
                ParameterTypeExpectation("email", "kotlin.String", "email should map to String"),
                ParameterTypeExpectation("firstName", "kotlin.String", "firstName should map to String"),
                ParameterTypeExpectation("lastName", "kotlin.String?", "lastName should map to String?"),
                ParameterTypeExpectation("birthDate", "kotlinx.datetime.LocalDate?", "birthDate should map to LocalDate?"),
                ParameterTypeExpectation("myCreatedAt", "kotlinx.datetime.LocalDateTime", "myCreatedAt should map to LocalDateTime"),
                ParameterTypeExpectation("notes", "com.example.db.PersonNote?", "notes should map to com.example.db.PersonNote?"),
            ),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val fixture = createInsertFixture(
                querySql = case.querySql,
                createTableSql = case.createTableSql,
                tableColumns = case.tableColumns,
                insertSql = case.insertSql,
                namedParameters = case.namedParameters,
                parameterToColumnNames = case.parameterToColumnNames,
            )
            try {
                case.expectations.forEach { expectation ->
                    val actualType = fixture.generator.inferParameterType(expectation.parameterName, fixture.statement)
                    assertEquals(expectation.expectedType, actualType.toString(), expectation.message)
                }
            } finally {
                fixture.cleanup()
            }
        }
    }

    @Test
    @DisplayName("Test INSERT statement with nullable adapter annotation generates correct null-checking logic")
    fun testInsertWithNullableAdapterAnnotation() {
        val fixture = createInsertFixture(
            querySql = "INSERT INTO Person (created_at) VALUES (:createdAt);",
            createTableSql = """
            CREATE TABLE person (
                created_at INTEGER NOT NULL
            )
            """,
            tableColumns = listOf(
                ParameterTableColumn(
                    name = "created_at",
                    dataType = "INTEGER",
                    notNull = false,
                    propertyType = "kotlinx.datetime.LocalDateTime",
                    adapter = true,
                )
            ),
            insertSql = "INSERT INTO Person (created_at) VALUES (?)",
            namedParameters = listOf("createdAt"),
            parameterToColumnNames = mapOf("createdAt" to "created_at"),
        )
        try {
            val createdAtType = fixture.generator.inferParameterType("createdAt", fixture.statement)
            assertEquals(
                "kotlinx.datetime.LocalDateTime?",
                createdAtType.toString(),
                "createdAt parameter should map to nullable LocalDateTime type due to propertyType annotation and nullable column"
            )
            val columnLookup = ColumnLookup(fixture.createTableStatements, createViewStatements = emptyList())
            val isNullable = columnLookup.isParameterNullable(fixture.statement, "createdAt")
            assertTrue(isNullable, "created_at column should be detected as nullable")
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test INSERT statement with nullable adapter keeps SQL binding non-null and generates adapter")
    fun testInsertWithNullableAdapterOnNotNullColumn() {
        val fixture = createNullableLastNameInsertFixture()
        try {
            val columnLookup = ColumnLookup(fixture.createTableStatements, createViewStatements = emptyList())
            val isNullable = columnLookup.isParameterNullable(fixture.statement, "lastName")
            assertFalse(
                isNullable,
                "last_name column should remain non-null for SQL binding even when @@{notNull=false} overrides Kotlin property nullability"
            )
            val queryGenerator = QueryCodeGenerator(
                generatorContext = fixture.generator.generatorContext,
                dataStructCodeGenerator = fixture.generator
            )
            queryGenerator.invokeExecuteQueryFun(
                namespace = "person",
                statement = fixture.statement,
                functionName = "execute",
            )
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test adapter parameter naming convention for INSERT and SELECT statements")
    fun testAdapterParameterNamingConvention() {
        val insertFixture = createCreatedAtInsertAdapterFixture()
        val selectFixture = createCreatedAtSelectAdapterFixture()
        try {
            val insertQueryGenerator = QueryCodeGenerator(
                generatorContext = insertFixture.generator.generatorContext,
                dataStructCodeGenerator = insertFixture.generator
            )
            val selectQueryGenerator = QueryCodeGenerator(
                generatorContext = selectFixture.generator.generatorContext,
                dataStructCodeGenerator = selectFixture.generator
            )

            val insertFunction = insertQueryGenerator.invokeExecuteQueryFun(
                namespace = "person",
                statement = insertFixture.statement,
                functionName = "execute",
            )
            val selectFunction = selectQueryGenerator.invokeSelectQueryFun(
                namespace = "person",
                statement = selectFixture.selectStatement,
                functionName = "executeAsList",
            )

            assertTrue(
                insertFunction.parameters.any { it.name == "createdAtToSqlValue" },
                "INSERT adapter parameter should use {propertyName}ToSqlValue naming"
            )
            assertTrue(
                selectFunction.parameters.any { it.name == "sqlValueToCreatedAt" },
                "SELECT adapter parameter should use sqlValueTo{PropertyName} naming"
            )
        } finally {
            insertFixture.cleanup()
            selectFixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test adapter deduplication for repeated SELECT parameters")
    fun testAdapterDeduplicationForRepeatedSelectParameters() {
        val fixture = createDuplicateBirthDateSelectAdapterFixture()
        try {
            val queryGenerator = QueryCodeGenerator(
                generatorContext = fixture.generator.generatorContext,
                dataStructCodeGenerator = fixture.generator
            )
            val selectFunction = queryGenerator.invokeSelectQueryFun(
                namespace = "person",
                statement = fixture.selectStatement,
                functionName = "executeAsList",
            )

            assertEquals(
                1,
                selectFunction.parameters.count { it.name == "birthDateToSqlValue" },
                "Repeated birth_date placeholders should share one adapter parameter"
            )
            assertEquals(
                1,
                Regex("""bindStatementParams\(statement, params, birthDateToSqlValue\)""")
                    .findAll(selectFunction.body.toString())
                    .count(),
                "Repeated birth_date placeholders should pass one adapter into bindStatementParams"
            )
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test INSERT statement generates correct null-checking logic for nullable parameters")
    fun testInsertStatementGeneratesNullCheckingLogic() {
        val fixture = createMixedNullableInsertFixture()
        try {
            val queryGenerator = QueryCodeGenerator(
                generatorContext = fixture.generator.generatorContext,
                dataStructCodeGenerator = fixture.generator
            )
            val function = queryGenerator.invokeExecuteQueryFun(
                namespace = "person",
                statement = fixture.statement,
                functionName = "execute",
            )

            val generatedCode = function.toString()
            assertTrue(
                generatedCode.contains("val sql = PersonQuery.Add.SQL"),
                "Execute function should use SQL constant"
            )
            assertTrue(
                generatedCode.contains("val statement = conn.prepare(sql)"),
                "Execute function should prepare statement"
            )
            assertTrue(
                generatedCode.contains("PersonQuery.Add.bindStatementParams("),
                "Execute function should call bindStatementParams"
            )
            assertTrue(
                generatedCode.contains("statement.use { statement ->"),
                "Execute function should use the prepared statement"
            )
            assertTrue(
                generatedCode.contains("statement.step()"),
                "Execute function should call step() on the prepared statement"
            )
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test INSERT statement with WITH clause does NOT include WITH clause fields as parameters")
    fun testInsertStatementWithWithClauseFields() {
        val withSelectStatement = SelectStatement(
            sql = "SELECT first_name, last_name FROM Person WHERE age > 18",
            fromTable = "person",
            joinTables = emptyList(),
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "first_name",
                    tableName = "person",
                    originalColumnName = "first_name",
                    dataType = "TEXT"
                ),
                SelectStatement.FieldSource(
                    fieldName = "last_name",
                    tableName = "person",
                    originalColumnName = "last_name",
                    dataType = "TEXT"
                )
            ),
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null
        )
        val fixture = createInsertFixture(
            querySql = "WITH adults AS (SELECT first_name, last_name FROM Person WHERE age > 18) INSERT INTO Person (first_name, last_name, created_at) VALUES (:firstName, :lastName, :createdAt);",
            createTableSql = """
                CREATE TABLE person (
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    age INTEGER,
                    created_at TEXT NOT NULL
                )
            """,
            tableColumns = withClausePersonColumns(),
            insertSql = "WITH adults AS (SELECT first_name, last_name FROM Person WHERE age > 18) INSERT INTO Person (first_name, last_name, created_at) VALUES (?, ?, ?)",
            namedParameters = listOf("firstName", "lastName", "createdAt"),
            parameterToColumnNames = mapOf(
                "firstName" to "first_name",
                "lastName" to "last_name",
                "createdAt" to "created_at",
            ),
            withSelectStatements = listOf(withSelectStatement),
        )
        try {
            val createdAtType = fixture.generator.inferParameterType("createdAt", fixture.statement)
            assertEquals(
                "kotlinx.datetime.LocalDateTime", createdAtType.toString(),
                "createdAt parameter should map to LocalDateTime from propertyType annotation"
            )
        } finally {
            fixture.cleanup()
        }
    }

    @Test
    @DisplayName("Test INSERT statement with WITH clause properly extracts named parameters and uses SELECT logic")
    fun testInsertStatementWithWithClauseParameterExtraction() {
        val withSelectStatement = SelectStatement(
            sql = "SELECT id FROM Person WHERE age = :mySelectAge LIMIT 1",
            fromTable = "person",
            joinTables = emptyList(),
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                )
            ),
            namedParameters = listOf("mySelectAge"),
            namedParametersToColumns = mapOf(
                "mySelectAge" to AssociatedColumn.Default("age")
            ),
            offsetNamedParam = null,
            limitNamedParam = null
        )
        val fixture = createInsertFixture(
            querySql = "WITH tmp AS (SELECT id FROM Person WHERE age = :mySelectAge LIMIT 1) INSERT INTO Person (first_name, last_name, created_at) VALUES (:firstName, :lastName, :createdAt);",
            createTableSql = """
                CREATE TABLE person (
                    id INTEGER PRIMARY KEY,
                    age INTEGER NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """,
            tableColumns = withClausePersonColumns(includeId = true, ageNotNull = true),
            insertSql = "WITH tmp AS (SELECT id FROM Person WHERE age = ? LIMIT 1) INSERT INTO Person (first_name, last_name, created_at) VALUES (?, ?, ?)",
            namedParameters = listOf("firstName", "lastName", "createdAt"),
            parameterToColumnNames = mapOf(
                "firstName" to "first_name",
                "lastName" to "last_name",
                "createdAt" to "created_at",
            ),
            withSelectStatements = listOf(withSelectStatement),
        )
        try {
            val mySelectAgeType = fixture.generator.inferParameterType("mySelectAge", fixture.statement)
            assertEquals(
                "kotlin.Long", mySelectAgeType.toString(),
                "mySelectAge parameter should map to Long (INTEGER type from age column)"
            )

            val firstNameType = fixture.generator.inferParameterType("firstName", fixture.statement)
            assertEquals(
                "kotlin.String", firstNameType.toString(),
                "firstName parameter should map to non-nullable String"
            )

            val createdAtType = fixture.generator.inferParameterType("createdAt", fixture.statement)
            assertEquals(
                "kotlinx.datetime.LocalDateTime", createdAtType.toString(),
                "createdAt parameter should map to LocalDateTime from propertyType annotation"
            )

            val allParams = fixture.statement.src.namedParameters + withSelectStatement.namedParameters
            assertEquals(
                setOf("firstName", "lastName", "createdAt", "mySelectAge"),
                allParams.toSet(),
                "Should include both INSERT parameters and WITH clause parameters"
            )
        } finally {
            fixture.cleanup()
        }
    }

    private fun withClausePersonColumns(
        includeId: Boolean = false,
        ageNotNull: Boolean = false,
    ): List<ParameterTableColumn> = buildList {
        if (includeId) {
            add(ParameterTableColumn(name = "id", dataType = "INTEGER", notNull = true))
        }
        add(ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = ageNotNull))
        add(ParameterTableColumn(name = "first_name", dataType = "TEXT", notNull = true))
        add(ParameterTableColumn(name = "last_name", dataType = "TEXT", notNull = true))
        add(
            ParameterTableColumn(
                name = "created_at",
                dataType = "TEXT",
                notNull = true,
                propertyType = "kotlinx.datetime.LocalDateTime",
            )
        )
    }

    private data class InsertParameterInferenceCase(
        val displayName: String,
        val querySql: String,
        val createTableSql: String,
        val tableColumns: List<ParameterTableColumn>,
        val insertSql: String,
        val namedParameters: List<String>,
        val parameterToColumnNames: Map<String, String>,
        val expectations: List<ParameterTypeExpectation>,
    )

}
