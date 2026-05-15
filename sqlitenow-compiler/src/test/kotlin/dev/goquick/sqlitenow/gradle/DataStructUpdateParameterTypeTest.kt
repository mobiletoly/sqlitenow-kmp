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


class DataStructUpdateParameterTypeTest : DataStructParameterTestSupport() {
    @Test
    @DisplayName("Test UPDATE statement parameter type inference")
    fun testUpdateStatementParameterTypeInference() {
        // Create a temporary directory for SQL files
        val tempDir = Files.createTempDirectory("test-sql")
        val queriesDir = tempDir.resolve("queries").resolve("person").toFile()
        queriesDir.mkdirs()

        // Create an UPDATE SQL file
        val updateSqlFile = File(queriesDir, "updatePerson.sql")
        updateSqlFile.writeText("UPDATE Person SET name = :newName, age = :newAge WHERE id = :personId;")

        // Create an in-memory SQLite database for testing
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the Person table
        conn.createStatement().execute(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER
            )
        """.trimIndent()
        )

        // Create mock CREATE TABLE statements
        val createTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "Person",
                src = CreateTableStatement(
                    sql = "",
                    tableName = "person",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "id",
                            dataType = "INTEGER",
                            notNull = true,
                            primaryKey = true,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "name",
                            dataType = "TEXT",
                            notNull = true,  // NOT NULL
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "age",
                            dataType = "INTEGER",
                            notNull = false,  // NULL allowed
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        )
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = null,
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    queryResult = null,
                    collectionKey = null
                ),
                columns = listOf(
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "id",
                            dataType = "INTEGER",
                            notNull = true,
                            primaryKey = true,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "name",
                            dataType = "TEXT",
                            notNull = true,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "age",
                            dataType = "INTEGER",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
                    )
                )
            )
        )

        // Create the generator
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = createTableStatements,
            packageName = "com.example.db",
            outputDir = tempDir.resolve("output").toFile()
        )

        // Create a mock UPDATE statement
        val mockUpdateStatement = AnnotatedExecuteStatement(
            name = "updatePerson",
            src = UpdateStatement(
                sql = "UPDATE Person SET name = ?, age = ? WHERE id = ?",
                table = "person",
                namedParameters = listOf("newName", "newAge", "personId"),
                namedParametersToColumns = mapOf(
                    "newName" to AssociatedColumn.Default("name"),
                    "newAge" to AssociatedColumn.Default("age"),
                    "personId" to AssociatedColumn.Default("id")
                ),
                namedParametersToColumnNames = mapOf(
                    "newName" to "name",
                    "newAge" to "age"
                    // personId is from WHERE clause, not SET clause
                ),
                withSelectStatements = emptyList(),
                hasReturningClause = false,
                returningColumns = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            )
        )

        // Test parameter type inference for UPDATE statement
        val newNameType = dataStructGenerator.inferParameterType("newName", mockUpdateStatement)
        assertEquals(
            "kotlin.String", newNameType.toString(),
            "newName parameter should be non-nullable String (name column is NOT NULL)"
        )

        val newAgeType = dataStructGenerator.inferParameterType("newAge", mockUpdateStatement)
        assertEquals(
            "kotlin.Long?", newAgeType.toString(),
            "newAge parameter should be nullable Long (age column allows NULL)"
        )

        val personIdType = dataStructGenerator.inferParameterType("personId", mockUpdateStatement)
        assertEquals(
            "kotlin.Long", personIdType.toString(),
            "personId parameter should be non-nullable Long (id column is NOT NULL)"
        )

        // Clean up
        tempDir.toFile().deleteRecursively()
    }

}
