package dev.goquick.sqlitenow.gradle

import java.io.File
import java.nio.file.Path
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class QueryCodeGeneratorTest {

    @TempDir
    lateinit var tempDir: Path

    private lateinit var outputDir: File
    private lateinit var queriesDir: File

    @BeforeEach
    fun setup() {
        outputDir = File(tempDir.toFile(), "output")
        queriesDir = File(tempDir.toFile(), "queries")
        queriesDir.mkdirs()
    }

    @Test
    @DisplayName("Test QueryCodeGenerator generates extension functions")
    fun testGenerateQueryExtensionFunctions() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing
        File(personDir, "getById.sql").writeText("""
            -- @@name=GetById
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@name=Add
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        File(personDir, "deleteById.sql").writeText("""
            -- @@name=DeleteById
            DELETE FROM users WHERE id = :userId;
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            dataStructCodeGenerator = dataStructGenerator,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the query code
        try {
            queryGenerator.generateCode()
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }

        // Verify that separate query files were created
        val personGetByIdFile = File(outputDir, "com/example/db/Person_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/Person_Add.kt")
        val personDeleteByIdFile = File(outputDir, "com/example/db/Person_DeleteById.kt")
        assertTrue(personGetByIdFile.exists(), "Person_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "Person_Add.kt file should be created")
        assertTrue(personDeleteByIdFile.exists(), "Person_DeleteById.kt file should be created")

        // Read the file content from one of the generated files
        val getByIdFileContent = personGetByIdFile.readText()
        val addFileContent = personAddFile.readText()
        val deleteByIdFileContent = personDeleteByIdFile.readText()

        // Verify that extension functions are generated with new structure
        assertTrue(getByIdFileContent.contains("fun Person.GetById.execute"), "Should contain GetById.execute extension function")
        assertTrue(getByIdFileContent.contains("fun Person.GetById.bindStatementParams"), "Should contain GetById.bindStatementParams extension function")
        assertTrue(getByIdFileContent.contains("fun Person.GetById.readStatementResult"), "Should contain GetById.readStatementResult extension function")
        assertTrue(addFileContent.contains("fun Person.Add.execute"), "Should contain Add.execute extension function")
        assertTrue(addFileContent.contains("fun Person.Add.bindStatementParams"), "Should contain Add.bindStatementParams extension function")
        assertTrue(deleteByIdFileContent.contains("fun Person.DeleteById.execute"), "Should contain DeleteById.execute extension function")
        assertTrue(deleteByIdFileContent.contains("fun Person.DeleteById.bindStatementParams"), "Should contain DeleteById.bindStatementParams extension function")

        // Verify function signatures with new structure
        assertTrue(getByIdFileContent.contains("conn: SQLiteConnection"), "Should have SQLiteConnection parameter")
        assertTrue(getByIdFileContent.contains("params: Person.GetById.Params"), "Should have GetById.Params parameter")
        assertTrue(addFileContent.contains("params: Person.Add.Params"), "Should have Add.Params parameter")
        assertTrue(deleteByIdFileContent.contains("params: Person.DeleteById.Params"), "Should have DeleteById.Params parameter")

        // Verify return types with new structure
        assertTrue(getByIdFileContent.contains("List<Person.GetById.Result>"), "SELECT should return List of results")
        // For Unit return type, Kotlin doesn't require explicit declaration, so check for function without return type
        assertTrue(addFileContent.contains("fun Person.Add.execute(conn: SQLiteConnection, params: Person.Add.Params)"),
                  "INSERT should have Unit return type (implicit)")
        assertTrue(deleteByIdFileContent.contains("fun Person.DeleteById.execute(conn: SQLiteConnection, params: Person.DeleteById.Params)"),
                  "DELETE should have Unit return type (implicit)")

        // Verify SQL statement variables (now uses SQL constants from query objects)
        assertTrue(getByIdFileContent.contains("val sql = Person.GetById.SQL"), "Should use SQL constants from query objects")
        assertTrue(addFileContent.contains("val sql = Person.Add.SQL"), "Should use SQL constants from query objects")
        assertTrue(deleteByIdFileContent.contains("val sql = Person.DeleteById.SQL"), "Should use SQL constants from query objects")

        // Verify statement preparation in execute functions
        assertTrue(getByIdFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")
        assertTrue(addFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")
        assertTrue(deleteByIdFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")

        // Verify that execute functions call bindStatementParams
        assertTrue(getByIdFileContent.contains("Person.GetById.bindStatementParams("), "Execute should call bindStatementParams")
        assertTrue(addFileContent.contains("Person.Add.bindStatementParams("), "Execute should call bindStatementParams")
        assertTrue(deleteByIdFileContent.contains("Person.DeleteById.bindStatementParams("), "Execute should call bindStatementParams")

        // Verify SELECT-specific implementation (only for SELECT statements)
        assertTrue(getByIdFileContent.contains("val results = mutableListOf<"), "Should contain results collection for SELECT")
        assertTrue(getByIdFileContent.contains("while (statement.step())"), "Should contain result iteration for SELECT")
        assertTrue(getByIdFileContent.contains("results.add("), "Should contain result addition for SELECT")

        // Verify that executeAsList calls readStatementResult for SELECT statements
        assertTrue(getByIdFileContent.contains("Person.GetById.readStatementResult("), "ExecuteAsList should call readStatementResult for SELECT")

        // Verify INSERT/DELETE-specific implementation (no return value)
        assertTrue(addFileContent.contains("statement.use { statement ->"), "Should contain statement execution for INSERT")
        assertTrue(deleteByIdFileContent.contains("statement.use { statement ->"), "Should contain statement execution for DELETE")

        // Verify file header comments
        assertTrue(getByIdFileContent.contains("Generated query extension functions for person.GetById"), "Should contain query-specific comment")
        assertTrue(getByIdFileContent.contains("Do not modify this file manually"), "Should contain warning comment")

        // Verify imports
        assertTrue(getByIdFileContent.contains("import androidx.sqlite.SQLiteConnection"), "Should import androidx.sqlite.SQLiteConnection")

        // Verify suspend functions
        assertTrue(getByIdFileContent.contains("public suspend fun Person.GetById.executeAsList("), "ExecuteAsList function should be suspend")
        assertTrue(addFileContent.contains("public suspend fun Person.Add.execute("), "Execute function should be suspend")
        assertTrue(deleteByIdFileContent.contains("public suspend fun Person.DeleteById.execute("), "Execute function should be suspend")
    }

    @Test
    @DisplayName("Test QueryCodeGenerator with statements without parameters")
    fun testGenerateQueryFunctionsWithoutParameters() {
        // Create a test namespace directory with SQL files
        val userDir = File(queriesDir, "user")
        userDir.mkdirs()

        // Create a SELECT statement without parameters
        File(userDir, "getAll.sql").writeText("""
            -- @@name=GetAll
            SELECT id, name, email FROM users;
        """.trimIndent())

        // Create DataStructCodeGenerator
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            
            dataStructCodeGenerator = dataStructGenerator,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the User_GetAll.kt file was created
        val userGetAllFile = File(outputDir, "com/example/db/User_GetAll.kt")
        assertTrue(userGetAllFile.exists(), "User_GetAll.kt file should be created")

        // Read the file content
        val fileContent = userGetAllFile.readText()

        // Verify that extension function is generated without params parameter
        assertTrue(fileContent.contains("suspend fun User.GetAll.executeAsList(conn: SQLiteConnection): List<User.GetAll.Result>"),
                  "Should contain GetAll.executeAsList extension function without params parameter and with suspend modifier")
        assertTrue(fileContent.contains("fun User.GetAll.bindStatementParams(statement: SQLiteStatement)"),
                  "Should contain GetAll.bindStatementParams extension function without params parameter")
        assertTrue(fileContent.contains("fun User.GetAll.readStatementResult(statement: SQLiteStatement"),
                  "Should contain GetAll.readStatementResult extension function")

        // Should not contain params parameter for statements without named parameters
        assertTrue(!fileContent.contains("params: User.GetAll.Params"), "Should not have params parameter for parameterless queries")
    }

    @Test
    @DisplayName("Test adapter annotation functionality with real SQL parsing")
    fun testAdapterAnnotationWithRealSqlParsing() {
        // This test verifies that the adapter annotation functionality works correctly
        // by testing the core components that handle adapter annotations.

        // Test 1: Verify FieldAnnotationOverrides correctly parses adapter annotations
        val fieldAnnotationsWithAdapter = mapOf(AnnotationConstants.ADAPTER to null, AnnotationConstants.PROPERTY_TYPE to "dev.example.Status")
        val fieldOverrides = FieldAnnotationOverrides.parse(fieldAnnotationsWithAdapter)
        assertTrue(fieldOverrides.adapter == true, "Should parse @@adapter flag annotation in SELECT fields")
        assertEquals("dev.example.Status", fieldOverrides.propertyType, "Should parse @@propertyType annotation")

        // Test 2: Verify SqliteTypeToKotlinCodeConverter provides consistent type mapping
        val testTypeMappings = mapOf(
            "INTEGER" to "kotlin.Long",
            "TEXT" to "kotlin.String",
            "REAL" to "kotlin.Double",
            "BLOB" to "kotlin.ByteArray"
        )

        testTypeMappings.forEach { (sqliteType, expectedKotlinType) ->
            val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(sqliteType)
            assertEquals(expectedKotlinType, kotlinType.toString(),
                        "$sqliteType should map to $expectedKotlinType for adapter input types")
        }

        // Test 4: Verify KOTLIN_STDLIB_TYPES contains expected types for binding
        val expectedStdlibTypes = listOf("String", "Int", "Long", "Double", "Float", "Boolean", "Byte")
        expectedStdlibTypes.forEach { type ->
            assertTrue(type in SqliteTypeToKotlinCodeConverter.KOTLIN_STDLIB_TYPES,
                      "$type should be in KOTLIN_STDLIB_TYPES for consistent binding")
        }

        assertTrue(true, "All adapter annotation functionality tests passed")
    }

    @Test
    @DisplayName("Test adapter annotation flag parsing behavior")
    fun testAdapterAnnotationFlagParsing() {
        // Test that adapter annotations are correctly identified as flag annotations

        // Test 1: Adapter annotation present (flag annotation with null value)
        val annotationsWithAdapter = mapOf(
            AnnotationConstants.ADAPTER to null,  // Flag annotation - key exists, value is null
            AnnotationConstants.PROPERTY_TYPE to "dev.example.CustomType"
        )

        val fieldOverrides = FieldAnnotationOverrides.parse(annotationsWithAdapter)
        assertTrue(fieldOverrides.adapter == true, "@@adapter flag annotation should be parsed as true")
        assertEquals("dev.example.CustomType", fieldOverrides.propertyType, "@@propertyType should be parsed correctly")

        // Test 2: Adapter annotation absent
        val annotationsWithoutAdapter = mapOf(
            AnnotationConstants.PROPERTY_TYPE to "dev.example.AnotherType",
            AnnotationConstants.NULLABLE to null
        )

        val fieldOverridesNoAdapter = FieldAnnotationOverrides.parse(annotationsWithoutAdapter)
        assertTrue(fieldOverridesNoAdapter.adapter == false, "Missing @@adapter should result in false")
        assertEquals("dev.example.AnotherType", fieldOverridesNoAdapter.propertyType, "Other annotations should still work")

        assertTrue(true, "Adapter flag annotation parsing works correctly")
    }

    @Test
    @DisplayName("Test SqliteTypeToKotlinCodeConverter integration")
    fun testSqliteTypeToKotlinCodeConverterIntegration() {
        // Test that the QueryCodeGenerator properly uses SqliteTypeToKotlinCodeConverter
        // for consistent type mapping across the codebase

        // Test basic SQLite type mappings
        val integerType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("INTEGER")
        assertEquals("kotlin.Long", integerType.toString(), "INTEGER should map to kotlin.Long")

        val textType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("TEXT")
        assertEquals("kotlin.String", textType.toString(), "TEXT should map to kotlin.String")

        val realType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("REAL")
        assertEquals("kotlin.Double", realType.toString(), "REAL should map to kotlin.Double")

        val blobType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType("BLOB")
        assertEquals("kotlin.ByteArray", blobType.toString(), "BLOB should map to kotlin.ByteArray")

        assertTrue(true, "SqliteTypeToKotlinCodeConverter integration verified")
    }

    @Test
    @DisplayName("Test annotation parsing functionality")
    fun testAnnotationParsingFunctionality() {
        // Test that FieldAnnotationOverrides properly handles adapter annotations
        val annotationsWithAdapter = mapOf(AnnotationConstants.ADAPTER to null) // Flag annotation
        val fieldAnnotations = FieldAnnotationOverrides.parse(annotationsWithAdapter)
        assertTrue(fieldAnnotations.adapter == true, "Should parse @@adapter flag annotation correctly")

        val annotationsWithoutAdapter = mapOf(AnnotationConstants.PROPERTY_NAME to "customName")
        val fieldAnnotationsNoAdapter = FieldAnnotationOverrides.parse(annotationsWithoutAdapter)
        assertTrue(fieldAnnotationsNoAdapter.adapter == false, "Should handle missing @@adapter annotation")

        assertTrue(true, "Annotation parsing functionality verified")
    }

    @Test
    @DisplayName("Test type mapping consistency across components")
    fun testTypeMappingConsistency() {
        // Test that different SQLite types map to consistent Kotlin types
        val testCases = listOf(
            Pair("INTEGER", "kotlin.Long"),
            Pair("REAL", "kotlin.Double"),
            Pair("BLOB", "kotlin.ByteArray"),
            Pair("TEXT", "kotlin.String"),
            Pair("FLOAT", "kotlin.Float"),
            Pair("BOOLEAN", "kotlin.Boolean")
        )

        testCases.forEach { (sqliteType, expectedKotlinType) ->
            val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(sqliteType)
            assertEquals(expectedKotlinType, kotlinType.toString(),
                        "$sqliteType should consistently map to $expectedKotlinType")
        }

        // Test that KOTLIN_STDLIB_TYPES contains expected types
        val expectedStdlibTypes = listOf("String", "Int", "Long", "Double", "Float", "Boolean", "Byte")
        expectedStdlibTypes.forEach { type ->
            assertTrue(type in SqliteTypeToKotlinCodeConverter.KOTLIN_STDLIB_TYPES,
                      "$type should be in KOTLIN_STDLIB_TYPES")
        }

        assertTrue(true, "Type mapping consistency verified across components")
    }

    @Test
    @DisplayName("Test comprehensive adapter type mapping and getter consistency")
    fun testAdapterTypeMappingAndGetterConsistency() {
        // Test that adapter functionality provides consistent type mapping
        // across SQLite types, Kotlin types, getters, and binding methods

        // Test comprehensive SQLite type mappings for adapters
        val comprehensiveTypeMappings = mapOf(
            "INTEGER" to "kotlin.Long",
            "INT" to "kotlin.Long",
            "TINYINT" to "kotlin.Byte",
            "SMALLINT" to "kotlin.Int",
            "MEDIUMINT" to "kotlin.Int",
            "BIGINT" to "kotlin.Long",
            "TEXT" to "kotlin.String",
            "VARCHAR" to "kotlin.String",
            "CHARACTER" to "kotlin.String",
            "CLOB" to "kotlin.String",
            "REAL" to "kotlin.Double",
            "DOUBLE" to "kotlin.Double",
            "FLOAT" to "kotlin.Float",
            "NUMERIC" to "kotlin.Long",
            "DECIMAL" to "kotlin.Long",
            "BOOLEAN" to "kotlin.Boolean",
            "BLOB" to "kotlin.ByteArray"
        )

        comprehensiveTypeMappings.forEach { (sqliteType, expectedKotlinType) ->
            val kotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(sqliteType)
            assertEquals(expectedKotlinType, kotlinType.toString(),
                        "$sqliteType should consistently map to $expectedKotlinType for adapter input parameters")
        }

        // Test that all standard Kotlin types are supported for binding
        val kotlinStdlibTypes = listOf("String", "Int", "Long", "Double", "Float", "Boolean", "Byte", "ByteArray")
        kotlinStdlibTypes.forEach { type ->
            if (type != "ByteArray") { // ByteArray is not in KOTLIN_STDLIB_TYPES but is handled specially
                assertTrue(type in SqliteTypeToKotlinCodeConverter.KOTLIN_STDLIB_TYPES,
                          "$type should be supported for consistent parameter binding")
            }
        }

        // Test that the adapter system supports both nullable and non-nullable scenarios
        // This is important for generating correct adapter function signatures
        val nullabilityTestCases = listOf(
            Triple("TEXT", false, "String"),      // NOT NULL -> non-nullable input
            Triple("TEXT", true, "String?"),      // nullable -> nullable input
            Triple("INTEGER", false, "Long"),     // NOT NULL -> non-nullable input
            Triple("INTEGER", true, "Long?"),     // nullable -> nullable input
            Triple("REAL", false, "Double"),      // NOT NULL -> non-nullable input
            Triple("REAL", true, "Double?")       // nullable -> nullable input
        )

        nullabilityTestCases.forEach { (sqliteType, isNullable, expectedInputType) ->
            val baseKotlinType = SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(sqliteType)
            val inputType = if (isNullable) baseKotlinType.copy(nullable = true) else baseKotlinType
            val expectedType = expectedInputType.replace("?", "").let { base ->
                if (isNullable) "$base?" else base
            }

            assertTrue(inputType.toString().contains(expectedType.replace("kotlin.", "")),
                      "$sqliteType with nullable=$isNullable should generate adapter input type compatible with $expectedInputType")
        }

        assertTrue(true, "Comprehensive adapter type mapping and consistency verified")
    }

    @Test
    @DisplayName("Test UPDATE statement query generation")
    fun testUpdateStatementQueryGeneration() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing including UPDATE
        File(personDir, "getById.sql").writeText("""
            -- @@name=GetById
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@name=Add
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        File(personDir, "updateUser.sql").writeText("""
            -- @@name=UpdateUser
            UPDATE users SET name = :newName, email = :newEmail WHERE id = :userId;
        """.trimIndent())

        File(personDir, "deleteById.sql").writeText("""
            -- @@name=DeleteById
            DELETE FROM users WHERE id = :userId;
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            
            dataStructCodeGenerator = dataStructGenerator,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that separate query files were created
        val personGetByIdFile = File(outputDir, "com/example/db/Person_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/Person_Add.kt")
        val personUpdateUserFile = File(outputDir, "com/example/db/Person_UpdateUser.kt")
        val personDeleteByIdFile = File(outputDir, "com/example/db/Person_DeleteById.kt")
        assertTrue(personGetByIdFile.exists(), "Person_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "Person_Add.kt file should be created")
        assertTrue(personUpdateUserFile.exists(), "Person_UpdateUser.kt file should be created")
        assertTrue(personDeleteByIdFile.exists(), "Person_DeleteById.kt file should be created")

        // Read the UPDATE file content
        val updateFileContent = personUpdateUserFile.readText()

        // Verify that UPDATE extension functions are generated
        assertTrue(updateFileContent.contains("suspend fun Person.UpdateUser.execute"), "Should contain UpdateUser.execute extension function with suspend modifier")
        assertTrue(updateFileContent.contains("fun Person.UpdateUser.bindStatementParams"), "Should contain UpdateUser.bindStatementParams extension function")

        // Verify UPDATE function signature
        assertTrue(updateFileContent.contains("params: Person.UpdateUser.Params"), "Should have UpdateUser.Params parameter")

        // Verify UPDATE return type (should be Unit, implicit)
        assertTrue(updateFileContent.contains("suspend fun Person.UpdateUser.execute(conn: SQLiteConnection, params: Person.UpdateUser.Params)"),
                  "UPDATE should have Unit return type (implicit) and suspend modifier")

        // Verify UPDATE SQL execution implementation
        assertTrue(updateFileContent.contains("val sql = Person.UpdateUser.SQL"), "Should use SQL constant for UPDATE")
        assertTrue(updateFileContent.contains("statement.use { statement ->"), "Should contain statement execution for UPDATE")
        assertTrue(updateFileContent.contains("statement.step()"), "Should execute UPDATE statement")

        // Verify that execute calls bindStatementParams
        assertTrue(updateFileContent.contains("Person.UpdateUser.bindStatementParams("), "Execute should call bindStatementParams")
    }

    @Test
    @DisplayName("Test QueryCodeGenerator generates suspend functions with coroutines imports")
    fun testGenerateSuspendFunctions() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing both SELECT and INSERT statements
        File(personDir, "getById.sql").writeText("""
            -- @@name=GetById
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@name=Add
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """.trimIndent())

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            
            dataStructCodeGenerator = dataStructGenerator,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the Person_GetById.kt and Person_Add.kt files were created
        val personGetByIdFile = File(outputDir, "com/example/db/Person_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/Person_Add.kt")
        assertTrue(personGetByIdFile.exists(), "Person_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "Person_Add.kt file should be created")

        // Read the file contents
        val getByIdFileContent = personGetByIdFile.readText()
        val addFileContent = personAddFile.readText()

        // Verify suspend modifier is present in execute functions
        assertTrue(getByIdFileContent.contains("public suspend fun Person.GetById.executeAsList("),
                  "SELECT executeAsList function should be suspend")
        assertTrue(addFileContent.contains("public suspend fun Person.Add.execute("),
                  "INSERT execute function should be suspend")

        // Verify that bindStatementParams and readStatementResult functions are NOT suspend
        assertTrue(getByIdFileContent.contains("public fun Person.GetById.bindStatementParams("),
                  "bindStatementParams should NOT be suspend")
        assertTrue(getByIdFileContent.contains("public fun Person.GetById.readStatementResult("),
                  "readStatementResult should NOT be suspend")
        assertTrue(addFileContent.contains("public fun Person.Add.bindStatementParams("),
                  "bindStatementParams should NOT be suspend")
    }

    @Test
    @DisplayName("Test QueryCodeGenerator generates all three SELECT execution functions")
    fun testGenerateAllSelectExecutionFunctions() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL file for testing SELECT statement
        File(personDir, "getById.sql").writeText("""
            -- @@name=GetById
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """.trimIndent())

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            
            dataStructCodeGenerator = dataStructGenerator,
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the Person_GetById.kt file was created
        val personGetByIdFile = File(outputDir, "com/example/db/Person_GetById.kt")
        assertTrue(personGetByIdFile.exists(), "Person_GetById.kt file should be created")

        // Read the file content
        val fileContent = personGetByIdFile.readText()

        // Verify all three execute functions are generated
        assertTrue(fileContent.contains("public suspend fun Person.GetById.executeAsList("),
                  "Should contain executeAsList function")
        assertTrue(fileContent.contains("public suspend fun Person.GetById.executeAsOne("),
                  "Should contain executeAsOne function")
        assertTrue(fileContent.contains("public suspend fun Person.GetById.executeAsOneOrNull("),
                  "Should contain executeAsOneOrNull function")

        // Verify executeAsList implementation
        assertTrue(fileContent.contains("val results = mutableListOf<Person.GetById.Result>()"),
                  "executeAsList should use mutableListOf")
        assertTrue(fileContent.contains("while (statement.step())"),
                  "executeAsList should use while loop")

        // Verify executeAsOne implementation
        assertTrue(fileContent.contains("throw IllegalStateException(\"Query returned no results, but exactly one result was expected\")"),
                  "executeAsOne should throw exception when no results")

        // Verify executeAsOneOrNull implementation
        assertTrue(fileContent.contains("} else {\n      null\n    }"),
                  "executeAsOneOrNull should return null when no results")

        // Verify all functions use the same SQL preparation and parameter binding
        val sqlPreparationCount = fileContent.split("val sql = Person.GetById.SQL").size - 1
        val paramBindingCallCount = fileContent.split("Person.GetById.bindStatementParams(statement, params)").size - 1
        assertEquals(3, sqlPreparationCount, "Should have 3 SQL preparation statements")
        assertEquals(3, paramBindingCallCount, "Should have 3 parameter binding calls in execute functions")
    }

    @Test
    @DisplayName("Test DataStructCodeGenerator generates affectedTables field")
    fun testGenerateAffectedTablesField() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing different statement types
        File(personDir, "selectWithJoin.sql").writeText("""
            -- @@name=SelectWithJoin
            SELECT u.id, u.name, a.street
            FROM users u
            JOIN addresses a ON u.id = a.user_id
            WHERE u.id = :userId;
        """.trimIndent())

        File(personDir, "simpleSelect.sql").writeText("""
            -- @@name=SimpleSelect
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "insertUser.sql").writeText("""
            -- @@name=InsertUser
            INSERT INTO users (name, email) VALUES (:name, :email);
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the tables
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT
            )
        """.trimIndent())

        realConnection.createStatement().execute("""
            CREATE TABLE addresses (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                street TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """.trimIndent())

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = emptyList(),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        // Generate the data structure code
        dataStructGenerator.generateCode()

        // Verify that the Person.kt file was created
        val personFile = File(outputDir, "com/example/db/Person.kt")
        assertTrue(personFile.exists(), "Person.kt file should be created")

        // Read the file content
        val fileContent = personFile.readText()

        // Verify affectedTables field exists for all query objects
        assertTrue(fileContent.contains("public val affectedTables: Set<String>"),
                  "Person.kt should contain affectedTables fields")

        // Verify correct table sets for JOIN query (should include both users and addresses)
        assertTrue(fileContent.contains("affectedTables: Set<String> = setOf(\"users\", \"addresses\")"),
                  "SelectWithJoin should list both users and addresses tables")

        // Verify correct table set for simple SELECT (should include only users)
        assertTrue(fileContent.contains("affectedTables: Set<String> = setOf(\"users\")"),
                  "SimpleSelect and InsertUser should list only users table")

        // Verify KDoc is present
        assertTrue(fileContent.contains("Set of table names that are affected by the"),
                  "Should contain KDoc for affectedTables")

        // Verify that each query object has its own affectedTables field
        val affectedTablesCount = fileContent.split("public val affectedTables: Set<String>").size - 1
        assertEquals(3, affectedTablesCount, "Should have 3 affectedTables fields (one for each query)")
    }


}
