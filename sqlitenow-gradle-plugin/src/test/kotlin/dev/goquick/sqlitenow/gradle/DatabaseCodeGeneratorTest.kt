package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.CreateTableStatement
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class DatabaseCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var queriesDir: File
    private lateinit var outputDir: File

    @BeforeEach
    fun setUp() {
        queriesDir = tempDir.resolve("queries").toFile()
        outputDir = tempDir.resolve("output").toFile()
        queriesDir.mkdirs()
        outputDir.mkdirs()
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator generates database class with adapters")
    fun testGenerateDatabaseClass() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files with field-specific adapter annotations
        File(personDir, "selectWithAdapters.sql").writeText("""
            -- @@name=SelectWithAdapters
            -- @@field=birth_date @@adapter
            SELECT id, name, birth_date
            FROM users
            WHERE birth_date >= :myBirthDateStart;
        """.trimIndent())

        File(personDir, "insertWithAdapters.sql").writeText("""
            -- @@name=InsertWithAdapters
            INSERT INTO users (name, birth_date, notes)
            VALUES (:name, :birthDate, :notes);
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the tables
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                birth_date TEXT,
                notes BLOB
            )
        """.trimIndent())

        // Create CREATE TABLE statements with adapter annotations
        val testCreateTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "CreateUsers",
                src = CreateTableStatement(
                    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, birth_date TEXT, notes BLOB)",
                    tableName = "users",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "id",
                            dataType = "INTEGER",
                            notNull = false,
                            primaryKey = true,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "name",
                            dataType = "TEXT",
                            notNull = true,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "birth_date",
                            dataType = "TEXT",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "notes",
                            dataType = "BLOB",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        )
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = null,
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    sharedResult = null,
                implements = null,
                excludeOverrideFields = null
                ),
                columns = listOf(
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "id",
                            dataType = "INTEGER",
                            notNull = false,
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
                            name = "birth_date",
                            dataType = "TEXT",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = mapOf("adapter" to null)
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "notes",
                            dataType = "BLOB",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = mapOf("adapter" to null)
                    )
                )
            )
        )

        // Use existing test helper to create DataStructCodeGenerator
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = testCreateTableStatements,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Get the parsed statements from the generator
        val nsWithStatements = dataStructGenerator.nsWithStatements
        val createTableStatements = dataStructGenerator.createTableStatements

        // Create DatabaseCodeGenerator
        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = createTableStatements,
            createViewStatements = dataStructGenerator.createViewStatements,
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "TestDatabase"
        )

        // Generate the database class
        databaseGenerator.generateDatabaseClass()

        // Debug: Check what files were created
        println("=== Files in output directory ===")
        outputDir.walkTopDown().forEach { file ->
            if (file.isFile()) {
                println("File: ${file.relativeTo(outputDir)}")
            }
        }

        // Verify that the database file was created
        val databaseFile = File(outputDir, "com/example/db/TestDatabase.kt")
        if (!databaseFile.exists()) {
            println("TestDatabase.kt file not found. Looking for any .kt files...")
            outputDir.walkTopDown().filter { it.extension == "kt" }.forEach { file ->
                println("Found .kt file: ${file.absolutePath}")
                println("Content preview: ${file.readText().take(200)}...")
            }
        }
        assertTrue(databaseFile.exists(), "TestDatabase.kt file should be created")

        // Read the file content
        val fileContent = databaseFile.readText()

        // Verify basic structure
        assertTrue(fileContent.contains("class TestDatabase"), "Should contain TestDatabase class")
        assertTrue(fileContent.contains(": SqliteNowDatabase"), "Should extend SqliteNowDatabase")
        
        // Verify constructor parameters for adapter wrappers
        assertTrue(fileContent.contains("personAdapters: PersonAdapters"), "Should contain personAdapters parameter")

        // Verify router class
        assertTrue(fileContent.contains("class PersonRouter"), "Should contain PersonRouter class")
        assertTrue(fileContent.contains("val person: PersonRouter"), "Should contain person router property")

        // Verify router methods - now using SelectRunners pattern
        assertTrue(fileContent.contains("val selectWithAdapters"), "Should contain selectWithAdapters SelectRunners property")
        assertTrue(fileContent.contains("SelectRunners<"), "Should contain SelectRunners interface usage")
        assertTrue(fileContent.contains("override suspend fun asList()"), "Should contain asList() method in SelectRunners")
        assertTrue(fileContent.contains("override suspend fun asOne()"), "Should contain asOne() method in SelectRunners")
        assertTrue(fileContent.contains("override suspend fun asOneOrNull()"), "Should contain asOneOrNull() method in SelectRunners")
        assertTrue(fileContent.contains("override fun asFlow()"), "Should contain asFlow() method in SelectRunners")
        assertTrue(fileContent.contains("val insertWithAdapters"), "Should contain insertWithAdapters ExecuteRunners property")

        // Verify adapter wrapper classes
        assertTrue(fileContent.contains("data class PersonAdapters"), "Should contain PersonAdapters data class")

        // Verify adapter parameter passing through wrapper
        assertTrue(fileContent.contains("ref.personAdapters."), "Should pass adapter parameters through wrapper")
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator handles namespaces without adapters")
    fun testNamespacesWithoutAdapters() {
        // Create test SQL files: one namespace with adapters, one without
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()
        val utilsDir = File(queriesDir, "utils")
        utilsDir.mkdirs()

        // Person namespace without adapters (to test the fix)
        File(personDir, "selectWithoutAdapters.sql").writeText("""
            -- @@name=SelectWithoutAdapters
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        // Utils namespace without adapters
        File(utilsDir, "simpleQuery.sql").writeText("""
            -- @@name=SimpleQuery
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT, birth_date TEXT)")

        val stmtProcessingHelper = StatementProcessingHelper(realConnection, FieldAnnotationResolver(emptyList(), emptyList()))
        val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir)

        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = emptyList(),
            createViewStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "TestDatabase"
        )

        databaseGenerator.generateDatabaseClass()

        val databaseFile = File(outputDir, "com/example/db/TestDatabase.kt")
        assertTrue(databaseFile.exists(), "TestDatabase.kt file should be created")

        val fileContent = databaseFile.readText()

        // Debug: Print the generated content to understand what's happening
        println("Generated database file content:")
        println(fileContent)

        // Should NOT have PersonAdapters data class (no adapters in our test SQL)
        assertFalse(fileContent.contains("public data class PersonAdapters("),
                   "Should NOT generate PersonAdapters data class when no adapters are used")

        // Should NOT have UtilsAdapters data class (no adapters)
        assertFalse(fileContent.contains("public data class UtilsAdapters("),
                   "Should NOT generate UtilsAdapters data class for namespace without adapters")

        // Should NOT have constructor parameter for PersonAdapters or UtilsAdapters
        assertFalse(fileContent.contains("private val personAdapters: PersonAdapters"),
                   "Should NOT have constructor parameter for PersonAdapters when no adapters")
        assertFalse(fileContent.contains("private val utilsAdapters: UtilsAdapters"),
                   "Should NOT have constructor parameter for UtilsAdapters")

        // Should have both router properties
        assertTrue(fileContent.contains("public val person: PersonRouter"),
                  "Should have person router property")
        assertTrue(fileContent.contains("public val utils: UtilsRouter"),
                  "Should have utils router property")

        // Router methods should not reference adapters
        assertFalse(fileContent.contains("ref.personAdapters."),
                   "Person router methods should not reference adapters when none exist")
        assertFalse(fileContent.contains("ref.utilsAdapters."),
                   "Utils router methods should not reference adapters")

        realConnection.close()

        println("✅ Namespaces without adapters test passed!")
        println("  - PersonAdapters data class NOT generated (no adapters)")
        println("  - UtilsAdapters data class NOT generated (no adapters)")
        println("  - Constructor parameters correct (no adapter parameters)")
        println("  - Router methods work without adapter references")
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator generates SelectRunners objects instead of individual methods")
    fun testSelectRunnersGeneration() {
        // Create test SQL files with SELECT statements
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // SELECT statement without parameters
        File(personDir, "selectAll.sql").writeText("""
            -- @@name=SelectAll
            SELECT id, name FROM users;
        """.trimIndent())

        // SELECT statement with parameters
        File(personDir, "selectById.sql").writeText("""
            -- @@name=SelectById
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT)")

        val stmtProcessingHelper = StatementProcessingHelper(realConnection, FieldAnnotationResolver(emptyList(), emptyList()))
        val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir)

        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = emptyList(),
            createViewStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "TestDatabase"
        )

        databaseGenerator.generateDatabaseClass()

        val databaseFile = File(outputDir, "com/example/db/TestDatabase.kt")
        assertTrue(databaseFile.exists(), "TestDatabase.kt file should be created")

        val fileContent = databaseFile.readText()

        // Should NOT have individual methods like selectAllAsList, selectAllAsOne, etc.
        assertFalse(fileContent.contains("suspend fun selectAllAsList"),
                   "Should NOT generate individual selectAllAsList method")
        assertFalse(fileContent.contains("suspend fun selectAllAsOne"),
                   "Should NOT generate individual selectAllAsOne method")
        assertFalse(fileContent.contains("suspend fun selectAllAsOneOrNull"),
                   "Should NOT generate individual selectAllAsOneOrNull method")
        assertFalse(fileContent.contains("fun selectAllFlow"),
                   "Should NOT generate individual selectAllFlow method")

        // Should have SelectRunners properties
        assertTrue(fileContent.contains("val selectAll"),
                  "Should have selectAll property")
        assertTrue(fileContent.contains("val selectById"),
                  "Should have selectById property")

        // Should have SelectRunners object expressions
        assertTrue(fileContent.contains("object : SelectRunners<"),
                  "Should contain SelectRunners object expressions")
        assertTrue(fileContent.contains("override suspend fun asList()"),
                  "Should contain asList() method implementation")
        assertTrue(fileContent.contains("override suspend fun asOne()"),
                  "Should contain asOne() method implementation")
        assertTrue(fileContent.contains("override suspend fun asOneOrNull()"),
                  "Should contain asOneOrNull() method implementation")
        assertTrue(fileContent.contains("override fun asFlow()"),
                  "Should contain asFlow() method implementation")

        // Should have function type for parameterized queries
        assertTrue(fileContent.contains("{ params -> object : SelectRunners<"),
                  "Should have lambda function for parameterized queries")

        realConnection.close()

        println("✅ SelectRunners generation test passed!")
        println("  - Individual methods NOT generated")
        println("  - SelectRunners properties generated")
        println("  - Object expressions with correct interface implementation")
        println("  - Function types for parameterized queries")
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator generates ExecuteRunners objects instead of individual execute methods")
    fun testExecuteRunnersGeneration() {
        // Create test SQL files with EXECUTE statements
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // INSERT statement without parameters
        File(personDir, "addSimple.sql").writeText("""
            -- @@name=AddSimple
            INSERT INTO users (name) VALUES ('test');
        """.trimIndent())

        // INSERT statement with parameters
        File(personDir, "addWithParams.sql").writeText("""
            -- @@name=AddWithParams
            INSERT INTO users (name, email) VALUES (:name, :email);
        """.trimIndent())

        // UPDATE statement with parameters
        File(personDir, "updateById.sql").writeText("""
            -- @@name=UpdateById
            UPDATE users SET name = :name WHERE id = :id;
        """.trimIndent())

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")

        val stmtProcessingHelper = StatementProcessingHelper(realConnection, FieldAnnotationResolver(emptyList(), emptyList()))
        val nsWithStatements = stmtProcessingHelper.processQueriesDirectory(queriesDir)

        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = emptyList(),
            createViewStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "TestDatabase"
        )

        databaseGenerator.generateDatabaseClass()

        val databaseFile = File(outputDir, "com/example/db/TestDatabase.kt")
        assertTrue(databaseFile.exists(), "TestDatabase.kt file should be created")

        val fileContent = databaseFile.readText()

        // Should NOT have individual execute methods like addSimpleExecute, addWithParamsExecute, etc.
        assertFalse(fileContent.contains("suspend fun addSimple("),
                   "Should NOT generate individual addSimple method")
        assertFalse(fileContent.contains("suspend fun addWithParams("),
                   "Should NOT generate individual addWithParams method")
        assertFalse(fileContent.contains("suspend fun updateById("),
                   "Should NOT generate individual updateById method")

        // Should have ExecuteRunners properties
        assertTrue(fileContent.contains("val addSimple"),
                  "Should have addSimple property")
        assertTrue(fileContent.contains("val addWithParams"),
                  "Should have addWithParams property")
        assertTrue(fileContent.contains("val updateById"),
                  "Should have updateById property")

        // Should have ExecuteRunners object expressions
        assertTrue(fileContent.contains("object : ExecuteRunners"),
                  "Should contain ExecuteRunners object expressions")
        assertTrue(fileContent.contains("override suspend fun execute()"),
                  "Should contain execute() method implementation")

        // Should have function type for parameterized queries
        assertTrue(fileContent.contains("{ params -> object : ExecuteRunners"),
                  "Should have lambda function for parameterized execute queries")

        // Should have table change notifications
        assertTrue(fileContent.contains("ref.notifyTablesChanged"),
                  "Should contain table change notifications")

        realConnection.close()

        println("✅ ExecuteRunners generation test passed!")
        println("  - Individual execute methods NOT generated")
        println("  - ExecuteRunners properties generated")
        println("  - Object expressions with correct interface implementation")
        println("  - Function types for parameterized execute queries")
        println("  - Table change notifications included")
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator deduplicates identical adapters")
    fun testAdapterDeduplication() {
        // Create test SQL files that would generate duplicate adapters
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        File(personDir, "query1.sql").writeText("""
            -- @@name=Query1
            -- @@field=created_at @@adapter
            SELECT id, created_at FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "query2.sql").writeText("""
            -- @@name=Query2
            -- @@field=created_at @@adapter
            SELECT name, created_at FROM users WHERE name = :userName;
        """.trimIndent())

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)")

        // Create CREATE TABLE statements with adapter annotations
        val testCreateTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "CreateUsers",
                src = CreateTableStatement(
                    sql = "CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)",
                    tableName = "users",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "id", dataType = "INTEGER", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "name", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "created_at", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        )
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = null,
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    sharedResult = null,
                implements = null,
                excludeOverrideFields = null
                ),
                columns = listOf(
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "id", dataType = "INTEGER", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "name", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "created_at", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = mapOf("adapter" to null)
                    )
                )
            )
        )

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = testCreateTableStatements,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        val nsWithStatements = dataStructGenerator.nsWithStatements
        val createTableStatements = dataStructGenerator.createTableStatements

        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = createTableStatements,
            createViewStatements = dataStructGenerator.createViewStatements,
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "DeduplicationTestDatabase"
        )

        databaseGenerator.generateDatabaseClass()

        val databaseFile = File(outputDir, "com/example/db/DeduplicationTestDatabase.kt")
        assertTrue(databaseFile.exists(), "DeduplicationTestDatabase.kt file should be created")

        val fileContent = databaseFile.readText()

        // Count occurrences of sqlColumnToCreatedAt - should appear only once in PersonAdapters data class
        val personAdaptersContent = fileContent.substringAfter("public data class PersonAdapters(")
            .substringBefore(")")

        val sqlColumnToCreatedAtCount = personAdaptersContent.split("sqlColumnToCreatedAt").size - 1
        assertEquals(1, sqlColumnToCreatedAtCount,
            "sqlColumnToCreatedAt should appear exactly once in PersonAdapters data class, but found $sqlColumnToCreatedAtCount occurrences")

        // Verify the adapter is present
        assertTrue(fileContent.contains("sqlColumnToCreatedAt"), "Should contain sqlColumnToCreatedAt adapter")
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator with real-world duplicate scenario")
    fun testRealWorldDuplicateScenario() {
        // Create test SQL files that would generate the exact duplicate you're seeing
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create multiple SQL files that reference the same column with adapter
        File(personDir, "selectWithCreatedAt1.sql").writeText("""
            -- @@name=SelectWithCreatedAt1
            -- @@field=created_at @@adapter
            SELECT id, name, created_at FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "selectWithCreatedAt2.sql").writeText("""
            -- @@name=SelectWithCreatedAt2
            -- @@field=created_at @@adapter
            SELECT name, created_at FROM users WHERE name = :userName;
        """.trimIndent())

        // Create minimal setup
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        realConnection.createStatement().execute("CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)")

        // Create CREATE TABLE statements with adapter annotations
        val testCreateTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "CreateUsers",
                src = CreateTableStatement(
                    sql = "CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)",
                    tableName = "users",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "id", dataType = "INTEGER", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "name", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "created_at", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        )
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = null,
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    sharedResult = null,
                implements = null,
                excludeOverrideFields = null
                ),
                columns = listOf(
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "id", dataType = "INTEGER", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "name", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "created_at", dataType = "TEXT", notNull = false,
                            primaryKey = false, autoIncrement = false, unique = false
                        ),
                        annotations = mapOf("adapter" to null)
                    )
                )
            )
        )

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = testCreateTableStatements,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        val nsWithStatements = dataStructGenerator.nsWithStatements
        val createTableStatements = dataStructGenerator.createTableStatements

        val databaseGenerator = DatabaseCodeGenerator(
            nsWithStatements = nsWithStatements,
            createTableStatements = createTableStatements,
            createViewStatements = dataStructGenerator.createViewStatements,
            packageName = "com.example.db",
            outputDir = outputDir,
            databaseClassName = "RealWorldTestDatabase"
        )

        // This should show debug output
        databaseGenerator.generateDatabaseClass()

        val databaseFile = File(outputDir, "com/example/db/RealWorldTestDatabase.kt")
        assertTrue(databaseFile.exists(), "RealWorldTestDatabase.kt file should be created")

        val fileContent = databaseFile.readText()
        println("=== Generated RealWorldTestDatabase.kt content ===")
        println(fileContent)

        // Count occurrences of sqlColumnToCreatedAt in adapter data class
        val adapterDataClassContent = fileContent.substringAfter("public data class ")
            .substringAfter("Adapters(")
            .substringBefore(")")

        val sqlColumnToCreatedAtCount = adapterDataClassContent.split("sqlColumnToCreatedAt").size - 1

        // If we see duplicates, let's understand why
        if (sqlColumnToCreatedAtCount > 1) {
            println("=== DUPLICATE DETECTED ===")
            println("Found $sqlColumnToCreatedAtCount occurrences of sqlColumnToCreatedAt in adapter data class")
            println("Adapter data class content:")
            println(adapterDataClassContent)
        }

        assertEquals(1, sqlColumnToCreatedAtCount,
            "sqlColumnToCreatedAt should appear exactly once in adapter data class, but found $sqlColumnToCreatedAtCount occurrences")
    }
}
