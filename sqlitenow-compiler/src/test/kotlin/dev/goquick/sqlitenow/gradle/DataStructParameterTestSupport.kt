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


abstract class DataStructParameterTestSupport {
    protected fun createParameterTypeFixture(
        queryFileName: String,
        querySql: String,
        createTableSql: String,
        tableColumns: List<ParameterTableColumn>,
        selectName: String,
        selectSql: String,
        selectFields: List<ParameterSelectField>,
        namedParameters: List<String>,
        namedParametersToColumns: Map<String, AssociatedColumn>,
    ): ParameterTypeFixture {
        val fixtureDir = Files.createTempDirectory("test-sql")
        val queriesDir = fixtureDir.resolve("queries").resolve("person").toFile()
        queriesDir.mkdirs()
        File(queriesDir, queryFileName).writeText(querySql)

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute(createTableSql.trimIndent())

        val createTableStatements = listOf(
            annotatedCreateTable(
                tableName = "person",
                columns = tableColumns.map(::annotatedParameterTableColumn),
            )
        )
        val generator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = createTableStatements,
            packageName = "com.example.db",
            outputDir = fixtureDir.resolve("output").toFile()
        )
        val fieldSources = selectFields.map { field ->
            fieldSource(
                fieldName = field.fieldName,
                tableName = "person",
                originalColumnName = field.originalColumnName,
                dataType = field.dataType,
            )
        }
        val statement = AnnotatedSelectStatement(
            name = selectName,
            src = selectStatementWithParameters(
                sql = selectSql,
                fromTable = "person",
                fields = fieldSources,
                namedParameters = namedParameters,
                namedParametersToColumns = namedParametersToColumns,
            ),
            annotations = statementAnnotations(),
            fields = selectFields.map { field ->
                regularField(
                    fieldName = field.fieldName,
                    tableName = "person",
                    originalColumnName = field.originalColumnName,
                    dataType = field.dataType,
                    adapter = field.adapter,
                )
            },
        )
        return ParameterTypeFixture(
            tempDir = fixtureDir,
            conn = conn,
            generator = generator,
            selectStatement = statement,
        )
    }

    protected fun createInsertFixture(
        queryFileName: String = "add.sql",
        querySql: String,
        createTableSql: String,
        tableColumns: List<ParameterTableColumn>,
        insertSql: String,
        namedParameters: List<String>,
        parameterToColumnNames: Map<String, String>,
        withSelectStatements: List<SelectStatement> = emptyList(),
    ): InsertFixture {
        val fixtureDir = Files.createTempDirectory("test-sql")
        val queriesDir = fixtureDir.resolve("queries").resolve("person").toFile()
        queriesDir.mkdirs()
        File(queriesDir, queryFileName).writeText(querySql)

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute(createTableSql.trimIndent())

        val createTableStatements = listOf(
            annotatedCreateTable(
                tableName = "person",
                columns = tableColumns.map(::annotatedParameterTableColumn),
            )
        )
        val generator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = createTableStatements,
            packageName = "com.example.db",
            outputDir = fixtureDir.resolve("output").toFile()
        )
        val statement = AnnotatedExecuteStatement(
            name = "add",
            src = InsertStatement(
                sql = insertSql,
                table = "person",
                namedParameters = namedParameters,
                columnNamesAssociatedWithNamedParameters = parameterToColumnNames,
                withSelectStatements = withSelectStatements,
                parameterCastTypes = emptyMap(),
                hasReturningClause = false,
                returningColumns = emptyList(),
            ),
            annotations = statementAnnotations(),
        )
        return InsertFixture(
            tempDir = fixtureDir,
            conn = conn,
            createTableStatements = createTableStatements,
            generator = generator,
            statement = statement,
        )
    }

    protected fun createDeleteFixture(
        queryFileName: String,
        querySql: String,
        createTableSql: String,
        tableColumns: List<ParameterTableColumn>,
        deleteSql: String,
        namedParameters: List<String>,
        namedParametersToColumns: Map<String, AssociatedColumn>,
        withSelectStatements: List<SelectStatement> = emptyList(),
    ): DeleteFixture {
        val fixtureDir = Files.createTempDirectory("test-sql")
        val queriesDir = fixtureDir.resolve("queries").resolve("person").toFile()
        queriesDir.mkdirs()
        File(queriesDir, queryFileName).writeText(querySql)

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute(createTableSql.trimIndent())

        val createTableStatements = listOf(
            annotatedCreateTable(
                tableName = "person",
                columns = tableColumns.map(::annotatedParameterTableColumn),
            )
        )
        val generator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir.parentFile,
            createTableStatements = createTableStatements,
            packageName = "com.example.db",
            outputDir = fixtureDir.resolve("output").toFile()
        )
        val statement = AnnotatedExecuteStatement(
            name = queryFileName.removeSuffix(".sql"),
            src = DeleteStatement(
                sql = deleteSql,
                table = "person",
                namedParameters = namedParameters,
                namedParametersToColumns = namedParametersToColumns,
                withSelectStatements = withSelectStatements,
            ),
            annotations = statementAnnotations(),
        )
        return DeleteFixture(
            tempDir = fixtureDir,
            conn = conn,
            createTableStatements = createTableStatements,
            generator = generator,
            statement = statement,
        )
    }

    protected fun createNullableLastNameInsertFixture(): InsertFixture = createInsertFixture(
        querySql = "INSERT INTO Person (last_name) VALUES (:lastName);",
        createTableSql = """
        CREATE TABLE person (
            last_name TEXT NOT NULL
        )
        """,
        tableColumns = listOf(
            ParameterTableColumn(
                name = "last_name",
                dataType = "TEXT",
                notNull = true,
                propertyType = "String",
                adapter = true,
                notNullOverride = false,
            )
        ),
        insertSql = "INSERT INTO person (last_name) VALUES (?)",
        namedParameters = listOf("lastName"),
        parameterToColumnNames = mapOf("lastName" to "last_name"),
    )

    protected fun createCreatedAtInsertAdapterFixture(): InsertFixture = createInsertFixture(
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
    )

    protected fun createCreatedAtSelectAdapterFixture(): ParameterTypeFixture = createParameterTypeFixture(
        queryFileName = "selectAll.sql",
        querySql = "SELECT created_at FROM Person",
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
        selectName = "selectAll",
        selectSql = "SELECT created_at FROM Person",
        selectFields = listOf(
            ParameterSelectField(
                fieldName = "created_at",
                dataType = "INTEGER",
                adapter = true,
            )
        ),
        namedParameters = emptyList(),
        namedParametersToColumns = emptyMap(),
    )

    protected fun createDuplicateBirthDateSelectAdapterFixture(): ParameterTypeFixture = createParameterTypeFixture(
        queryFileName = "selectOlderThan.sql",
        querySql = "SELECT birth_date FROM Person WHERE birth_date < :birth_date AND birth_date < :birth_date AND age < :age;",
        createTableSql = """
        CREATE TABLE person (
            birth_date TEXT,
            age INTEGER
        )
        """,
        tableColumns = listOf(
            ParameterTableColumn(
                name = "birth_date",
                dataType = "TEXT",
                notNull = false,
                propertyType = "kotlinx.datetime.LocalDate",
                adapter = true,
            ),
            ParameterTableColumn(name = "age", dataType = "INTEGER", notNull = false),
        ),
        selectName = "selectOlderThan",
        selectSql = "SELECT * FROM Person WHERE birth_date < ? AND birth_date < ? AND age < ?",
        selectFields = listOf(
            ParameterSelectField(
                fieldName = "birth_date",
                dataType = "TEXT",
                adapter = true,
            )
        ),
        namedParameters = listOf("birth_date", "birth_date", "age"),
        namedParametersToColumns = mapOf(
            "birth_date" to AssociatedColumn.Default("birth_date"),
            "age" to AssociatedColumn.Default("age"),
        ),
    )

    protected fun createMixedNullableInsertFixture(): InsertFixture = createInsertFixture(
        querySql = "INSERT INTO Person (email, phone, birth_date) VALUES (:email, :phone, :birthDate);",
        createTableSql = """
        CREATE TABLE person (
            email TEXT NOT NULL,
            phone TEXT,
            birth_date TEXT
        )
        """,
        tableColumns = listOf(
            ParameterTableColumn(name = "email", dataType = "TEXT", notNull = true),
            ParameterTableColumn(name = "phone", dataType = "TEXT", notNull = false),
            ParameterTableColumn(
                name = "birth_date",
                dataType = "TEXT",
                notNull = false,
                propertyType = "kotlinx.datetime.LocalDate",
            ),
        ),
        insertSql = "INSERT INTO Person (email, phone, birth_date) VALUES (?, ?, ?)",
        namedParameters = listOf("email", "phone", "birthDate"),
        parameterToColumnNames = mapOf(
            "email" to "email",
            "phone" to "phone",
            "birthDate" to "birth_date",
        ),
    )

    protected fun annotatedParameterTableColumn(
        column: ParameterTableColumn,
    ): AnnotatedCreateTableStatement.Column {
        val annotations = buildMap<String, Any?> {
            column.notNullOverride?.let { put(AnnotationConstants.NOT_NULL, it) }
            column.propertyType?.let { put(AnnotationConstants.PROPERTY_TYPE, it) }
            if (column.adapter) {
                put(AnnotationConstants.ADAPTER, AnnotationConstants.ADAPTER_CUSTOM)
            }
        }
        return AnnotatedCreateTableStatement.Column(
            src = CreateTableStatement.Column(
                name = column.name,
                dataType = column.dataType,
                notNull = column.notNull,
                primaryKey = false,
                autoIncrement = false,
                unique = false,
            ),
            annotations = annotations,
        )
    }

    protected interface TempSqlFixture {
        val tempDir: Path
        val conn: Connection

        fun cleanup() {
            conn.close()
            tempDir.toFile().deleteRecursively()
        }
    }

    protected data class ParameterTypeFixture(
        override val tempDir: Path,
        override val conn: Connection,
        val generator: DataStructCodeGenerator,
        val selectStatement: AnnotatedSelectStatement,
    ) : TempSqlFixture

    protected data class InsertFixture(
        override val tempDir: Path,
        override val conn: Connection,
        val createTableStatements: List<AnnotatedCreateTableStatement>,
        val generator: DataStructCodeGenerator,
        val statement: AnnotatedExecuteStatement,
    ) : TempSqlFixture

    protected data class DeleteFixture(
        override val tempDir: Path,
        override val conn: Connection,
        val createTableStatements: List<AnnotatedCreateTableStatement>,
        val generator: DataStructCodeGenerator,
        val statement: AnnotatedExecuteStatement,
    ) : TempSqlFixture

    protected data class ParameterTableColumn(
        val name: String,
        val dataType: String,
        val notNull: Boolean,
        val propertyType: String? = null,
        val adapter: Boolean = false,
        val notNullOverride: Boolean? = null,
    )

    protected data class ParameterSelectField(
        val fieldName: String,
        val dataType: String,
        val originalColumnName: String = fieldName,
        val adapter: Boolean = false,
    )

    protected data class CollectionParameterTypeCase(
        val queryFileName: String,
        val querySql: String,
        val createTableSql: String,
        val tableColumns: List<ParameterTableColumn>,
        val selectName: String,
        val selectSql: String,
        val selectFields: List<ParameterSelectField>,
        val namedParameters: List<String>,
        val namedParametersToColumns: Map<String, AssociatedColumn>,
        val expectations: List<ParameterTypeExpectation>,
    )

    protected data class ParameterTypeExpectation(
        val parameterName: String,
        val expectedType: String,
        val message: String,
    )

    @TempDir
    lateinit var tempDir: Path


}

internal fun QueryCodeGenerator.invokeExecuteQueryFun(
    namespace: String,
    statement: AnnotatedExecuteStatement,
    functionName: String,
): com.squareup.kotlinpoet.FunSpec = invokeQueryFun(
    namespace = namespace,
    statement = statement,
    functionName = functionName,
    generatorMethodName = "generateExecuteQueryFunction",
    statementClass = AnnotatedExecuteStatement::class.java,
)

internal fun QueryCodeGenerator.invokeSelectQueryFun(
    namespace: String,
    statement: AnnotatedSelectStatement,
    functionName: String,
): com.squareup.kotlinpoet.FunSpec = invokeQueryFun(
    namespace = namespace,
    statement = statement,
    functionName = functionName,
    generatorMethodName = "generateSelectQueryFunction",
    statementClass = AnnotatedSelectStatement::class.java,
)

private fun QueryCodeGenerator.invokeQueryFun(
    namespace: String,
    statement: AnnotatedStatement,
    functionName: String,
    generatorMethodName: String,
    statementClass: Class<out AnnotatedStatement>,
): com.squareup.kotlinpoet.FunSpec {
    val field = javaClass.getDeclaredField("queryExecuteEmitter").apply { isAccessible = true }
    val emitter = field.get(this)
    val method = emitter.javaClass.getDeclaredMethod(
        generatorMethodName,
        String::class.java,
        statementClass,
        String::class.java,
    ).apply { isAccessible = true }
    return method.invoke(emitter, namespace, statement, functionName) as com.squareup.kotlinpoet.FunSpec
}
