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

// Test helper to create mock executors from AnnotatedCreateTableStatement
class MockCreateTableStatementExecutor(
    private val annotatedStatement: AnnotatedCreateTableStatement
) : DeferredStatementExecutor {
    override fun execute(conn: Connection): AnnotatedStatement {
        // Create the table in the database using the column information from the annotated statement
        val tableName = annotatedStatement.src.tableName
        val columns = annotatedStatement.src.columns.joinToString(", ") { col ->
            val nullConstraint = if (col.notNull) "NOT NULL" else ""
            val primaryKeyConstraint = if (col.primaryKey) "PRIMARY KEY" else ""
            val autoIncrementConstraint = if (col.autoIncrement) "AUTOINCREMENT" else ""
            val uniqueConstraint = if (col.unique) "UNIQUE" else ""

            "${col.name} ${col.dataType} $nullConstraint $primaryKeyConstraint $autoIncrementConstraint $uniqueConstraint".trim()
        }

        val createTableSql = "CREATE TABLE IF NOT EXISTS $tableName ($columns)"

        conn.createStatement().use { stmt ->
            stmt.executeUpdate(createTableSql)
        }

        // Return the exact annotated statement that was passed in
        return annotatedStatement
    }

    override fun reportContext(): String {
        return annotatedStatement.src.sql
    }
}
// Helper function to create DataStructCodeGenerator with mock executors
fun createDataStructCodeGeneratorWithMockExecutors(
    conn: Connection,
    queriesDir: File,
    createTableStatements: List<AnnotatedCreateTableStatement>,
    packageName: String,
    outputDir: File
): DataStructCodeGenerator {
    // Create mock executors that will return our predefined AnnotatedCreateTableStatement objects
    val mockExecutors: MutableList<DeferredStatementExecutor> = createTableStatements.map {
        MockCreateTableStatementExecutor(it)
    }.toMutableList()

    // Execute the mock executors to create tables in the database
    mockExecutors.forEach { it.execute(conn) }

    // Create a DataStructCodeGenerator with provided createTableStatements
    return DataStructCodeGenerator(
        conn = conn,
        queriesDir = queriesDir,
        packageName = packageName,
        outputDir = outputDir,
        statementExecutors = mutableListOf(), // Empty since we provide createTableStatements directly
        providedCreateTableStatements = createTableStatements
    )
}

class DataStructCodeGeneratorTest {

    /**
     * Creates a real DataStructCodeGenerator with actual SQL files and database connection.
     * This eliminates the need for complex mocking.
     *
     * @param useClassName If true, adds 'name' annotation to SQL files
     * @param withParams If true, creates SQL with named parameters
     * @param namespace The namespace directory name (default: "user")
     */
    private fun createRealDataStructGenerator(
        tempDir: Path,
        useClassName: Boolean = true,
        withParams: Boolean = false,
        namespace: String = "user"
    ): DataStructCodeGenerator {
        // Create directory structure
        val queriesDir = tempDir.resolve("queries").toFile()
        queriesDir.mkdirs()
        val namespaceDir = File(queriesDir, namespace)
        namespaceDir.mkdirs()

        // Create real in-memory SQLite database
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        conn.createStatement().execute(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent()
        )

        // Create SQL file content
        val classNameAnnotation = if (useClassName) "-- @@{name=CustomClassName}\n" else ""
        val sqlContent = if (withParams) {
            "${classNameAnnotation}SELECT name FROM users WHERE id = :user_id AND email = :email;"
        } else {
            "${classNameAnnotation}SELECT name FROM users;"
        }

        // Write SQL file
        File(namespaceDir, "testQuery.sql").writeText(sqlContent)

        // Create and return real generator
        return createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = tempDir.resolve("output").toFile()
        )
    }

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("Test generating namespace data structures code")
    fun testGenerateNamespaceDataStructuresCode() {
        // Create real generators for multiple namespaces
        val userGenerator = createRealDataStructGenerator(tempDir, useClassName = true, namespace = "user")
        val productGenerator = createRealDataStructGenerator(tempDir, useClassName = true, namespace = "product")

        // Test that we can generate code for user namespace
        val userFileSpec = userGenerator.generateNamespaceDataStructuresCode(
            namespace = "user",
            packageName = "com.example.db"
        )

        // Test that we can generate code for product namespace
        val productFileSpec = productGenerator.generateNamespaceDataStructuresCode(
            namespace = "product",
            packageName = "com.example.db"
        )

        // Verify that FileSpec.Builders were created successfully
        assertTrue(userFileSpec.build().name == "UserQuery", "Should generate UserQuery file")
        assertTrue(productFileSpec.build().name == "ProductQuery", "Should generate ProductQuery file")

        // Verify the generated code contains expected structures
        val userCode = userFileSpec.build().toString()
        val productCode = productFileSpec.build().toString()

        assertTrue(userCode.contains("object UserQuery"), "User code should contain 'object UserQuery'")
        assertTrue(userCode.contains("object CustomClassName"), "User code should contain query-specific object")
        assertTrue(productCode.contains("object ProductQuery"), "Product code should contain 'object ProductQuery'")
        assertTrue(productCode.contains("object CustomClassName"), "Product code should contain query-specific object")
    }

    @Test
    @DisplayName("Test generating code with className annotation")
    fun testGenerateCodeWithClassName() {
        // Create real generator with className annotation
        val generator = createRealDataStructGenerator(tempDir, useClassName = true)

        // Generate the code
        generator.generateCode()

        // Verify that the files were created
        val userQueriesFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserQuery.kt")
        val resultFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserCustomClassNameResult.kt")

        assertTrue(userQueriesFile.exists(), "UserQuery.kt should be created")
        assertTrue(resultFile.exists(), "UserCustomClassNameResult.kt should be created")

        // Verify query file contents
        val userQueriesContent = userQueriesFile.readText()

        assertTrue(userQueriesContent.contains("object UserQuery"), "UserQuery.kt should contain 'object UserQuery'")
        assertTrue(
            userQueriesContent.contains("object CustomClassName"),
            "UserQuery.kt should contain query-specific object"
        )
        assertTrue(userQueriesContent.contains("const val SQL"), "UserQuery.kt should contain SQL constant")

        // Verify result file contents
        val resultContent = resultFile.readText()
        assertTrue(resultContent.contains("data class UserCustomClassNameResult"), "Result file should contain 'data class UserCustomClassNameResult'")
        assertTrue(resultContent.contains("val name: String"), "Result file should contain 'val name: String'")
    }

    @Test
    @DisplayName("Test generating code without className annotation")
    fun testGenerateCodeWithoutClassName() {
        // Create real generator without className annotation
        val generator = createRealDataStructGenerator(tempDir, useClassName = false)

        // Generate the code
        generator.generateCode()

        // Verify that the files were created
        val userQueriesFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserQuery.kt")
        val resultFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserTestQueryResult.kt")

        assertTrue(userQueriesFile.exists(), "UserQuery.kt should be created")
        assertTrue(resultFile.exists(), "UserTestQueryResult.kt should be created")

        // Verify query file contents
        val userQueriesContent = userQueriesFile.readText()

        assertTrue(userQueriesContent.contains("object UserQuery"), "UserQuery.kt should contain 'object UserQuery'")
        assertTrue(userQueriesContent.contains("object TestQuery"), "UserQuery.kt should contain query-specific object")
        assertTrue(userQueriesContent.contains("const val SQL"), "UserQuery.kt should contain SQL constant")

        // Verify result file contents
        val resultContent = resultFile.readText()
        assertTrue(resultContent.contains("data class UserTestQueryResult"), "Result file should contain 'data class UserTestQueryResult'")
    }

    @Test
    @DisplayName("Test generating and writing code files")
    fun testGenerateCode() {
        // Create real generators for multiple namespaces
        val userGenerator = createRealDataStructGenerator(tempDir, useClassName = true, namespace = "user")
        val productGenerator = createRealDataStructGenerator(tempDir, useClassName = true, namespace = "product")

        // Generate the code for both namespaces
        userGenerator.generateCode()
        productGenerator.generateCode()

        // Verify that the files were created
        val outputDir = tempDir.resolve("output").toFile()
        val userQueriesFile = File(outputDir, "com/example/db/UserQuery.kt")
        val productQueriesFile = File(outputDir, "com/example/db/ProductQuery.kt")

        assertTrue(userQueriesFile.exists(), "UserQuery.kt should be created")
        assertTrue(productQueriesFile.exists(), "ProductQuery.kt should be created")

        // Verify file contents
        val userQueriesContent = userQueriesFile.readText()
        val productQueriesContent = productQueriesFile.readText()

        assertTrue(userQueriesContent.contains("object UserQuery"), "UserQuery.kt should contain 'object UserQuery'")
        assertTrue(
            userQueriesContent.contains("object CustomClassName"),
            "UserQuery.kt should contain query-specific object"
        )
        assertTrue(userQueriesContent.contains("const val SQL"), "UserQuery.kt should contain SQL constant")
        assertTrue(
            productQueriesContent.contains("object ProductQuery"),
            "ProductQuery.kt should contain 'object ProductQuery'"
        )
        assertTrue(
            productQueriesContent.contains("object CustomClassName"),
            "ProductQuery.kt should contain query-specific object"
        )
        assertTrue(productQueriesContent.contains("const val SQL"), "ProductQuery.kt should contain SQL constant")

        // Since we didn't add parameters, the Params data class should not be present
        assertFalse(
            userQueriesContent.contains("data class Params"),
            "UserQuery.kt should not contain 'data class Params'"
        )
        assertFalse(
            productQueriesContent.contains("data class Params"),
            "ProductQuery.kt should not contain 'data class Params'"
        )
    }

    @Test
    @DisplayName("Test generating code with parameters")
    fun testGenerateCodeWithParameters() {
        // Create real generator with parameters
        val generator = createRealDataStructGenerator(tempDir, useClassName = true, withParams = true)

        // Generate the code
        generator.generateCode()

        // Verify that the files were created
        val userQueriesFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserQuery.kt")
        val resultFile = File(tempDir.resolve("output").toFile(), "com/example/db/UserCustomClassNameResult.kt")

        assertTrue(userQueriesFile.exists(), "UserQuery.kt should be created")
        assertTrue(resultFile.exists(), "UserCustomClassNameResult.kt should be created")

        // Verify query file contents
        val userQueriesContent = userQueriesFile.readText()

        assertTrue(userQueriesContent.contains("object UserQuery"), "UserQuery.kt should contain 'object UserQuery'")
        assertTrue(
            userQueriesContent.contains("object CustomClassName"),
            "UserQuery.kt should contain query-specific object"
        )
        assertTrue(userQueriesContent.contains("const val SQL"), "UserQuery.kt should contain SQL constant")
        assertTrue(userQueriesContent.contains("data class Params"), "UserQuery.kt should contain 'data class Params'")

        // Check for parameter properties (defaulting to String until proper type inference is implemented)
        assertTrue(
            userQueriesContent.contains("val userId: String"),
            "UserQuery.kt should contain 'val userId: String'"
        )
        assertTrue(userQueriesContent.contains("val email: String"), "UserQuery.kt should contain 'val email: String'")

        // Verify result file contents
        val resultContent = resultFile.readText()
        assertTrue(resultContent.contains("data class UserCustomClassNameResult"), "Result file should contain 'data class UserCustomClassNameResult'")
    }

    @Test
    @DisplayName("Test shared result data class generation with queryResult annotation")
    fun testSharedResultWithQueryResultAnnotation() {
        // Create minimal create table statements for the test
        val createTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "Person",
                src = CreateTableStatement(
                    sql = "",
                    tableName = "person",
                    columns = listOf(
                        CreateTableStatement.Column("id", "INTEGER", true, true, false, false),
                        CreateTableStatement.Column("name", "TEXT", true, false, false, false)
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = null,
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    queryResult = null,
                    collectionKey = null
),
                columns = emptyList()
            )
        )

        // Create a real in-memory SQLite connection
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the Person table
        conn.createStatement().execute(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """.trimIndent()
        )

        // Create queries directory structure
        val queriesDir = tempDir.resolve("queries").toFile()
        queriesDir.mkdirs()
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL file with queryResult annotation
        val sqlContent = """
            -- @@{queryResult=PersonWithExtras}
            SELECT id, name FROM person;
        """.trimIndent()
        File(personDir, "selectWithExtras.sql").writeText(sqlContent)

        // Create data structure generator using the helper function
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = createTableStatements,
            packageName = "com.example.db",
            outputDir = tempDir.resolve("output").toFile()
        )

        // Generate the code (this will create separate result files)
        dataStructGenerator.generateCode()

        // Verify the separate result file was created
        val resultFile = File(tempDir.resolve("output").toFile(), "com/example/db/PersonWithExtras.kt")
        assertTrue(resultFile.exists(), "PersonWithExtras.kt should be created")

        // Read the result file content
        val resultContent = resultFile.readText()

        // Verify the shared result data class is generated correctly
        assertTrue(
            resultContent.contains("data class PersonWithExtras"),
            "Should generate PersonWithExtras data class"
        )

        // Verify regular fields are included
        assertTrue(
            resultContent.contains("val id: Long"),
            "Should include regular id field"
        )
        assertTrue(
            resultContent.contains("val name: String"),
            "Should include regular name field"
        )
    }
}
