package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.query.QueryCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import java.io.File
import java.nio.file.Path
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
    @DisplayName("Select execute functions expose result adapters when propertyType mapping is used")
    fun testSelectExecuteFunctionsExposeResultAdapters() {
        val personDir = File(queriesDir, "personAdapters")
        personDir.mkdirs()

        File(personDir, "selectWithAdapters.sql").writeText(
            """
            -- @@{name=SelectWithAdapters}
            SELECT id, birth_date, created_at
            FROM person
            WHERE id = :personId;
            """.trimIndent()
        )

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        val idColumn = CreateTableStatement.Column(
            name = "id",
            dataType = "INTEGER",
            notNull = true,
            primaryKey = true,
            autoIncrement = false,
            unique = true
        )
        val birthDateColumn = CreateTableStatement.Column(
            name = "birth_date",
            dataType = "TEXT",
            notNull = false,
            primaryKey = false,
            autoIncrement = false,
            unique = false
        )
        val createdAtColumn = CreateTableStatement.Column(
            name = "created_at",
            dataType = "TEXT",
            notNull = true,
            primaryKey = false,
            autoIncrement = false,
            unique = false
        )

        val personTableStatement = AnnotatedCreateTableStatement(
            name = "Person",
            src = CreateTableStatement(
                sql = "CREATE TABLE person(id INTEGER PRIMARY KEY, birth_date TEXT, created_at TEXT NOT NULL);",
                tableName = "person",
                columns = listOf(idColumn, birthDateColumn, createdAtColumn)
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = idColumn,
                    annotations = emptyMap()
                ),
                AnnotatedCreateTableStatement.Column(
                    src = birthDateColumn,
                    annotations = mapOf(
                        AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDate",
                        AnnotationConstants.ADAPTER to AnnotationConstants.ADAPTER_CUSTOM
                    )
                ),
                AnnotatedCreateTableStatement.Column(
                    src = createdAtColumn,
                    annotations = mapOf(
                        AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDateTime",
                        AnnotationConstants.ADAPTER to AnnotationConstants.ADAPTER_CUSTOM
                    )
                )
            )
        )

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = listOf(personTableStatement),
            packageName = "com.example.db",
            outputDir = outputDir
        )

        val queryGenerator = QueryCodeGenerator(
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        queryGenerator.generateCode()

        val generatedFile = File(outputDir, "com/example/db/PersonAdaptersQuery_SelectWithAdapters.kt")
        assertTrue(generatedFile.exists(), "Generated query file should exist")
        val content = generatedFile.readText()

        assertTrue(
            content.contains("sqlValueToBirthDate: (String?) -> LocalDate?"),
            "execute functions should expose sqlValueToBirthDate adapter parameter"
        )
        assertTrue(
            content.contains("sqlValueToCreatedAt: (String) -> LocalDateTime"),
            "execute functions should expose sqlValueToCreatedAt adapter parameter"
        )

        assertTrue(
            content.contains("PersonAdaptersQuery.SelectWithAdapters.readStatementResult(statement, sqlValueToBirthDate, sqlValueToCreatedAt)"),
            "readStatementResult invocation should pass both adapters"
        )
    }

    @Test
    @DisplayName("Test QueryCodeGenerator generates extension functions")
    fun testGenerateQueryExtensionFunctions() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing
        File(personDir, "getById.sql").writeText("""
            -- @@{name=GetById}
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@{name=Add}
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        File(personDir, "deleteById.sql").writeText("""
            -- @@{name=DeleteById}
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
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        try {
            queryGenerator.generateCode()
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }

        // Verify that separate query files were created
        val personGetByIdFile = File(outputDir, "com/example/db/PersonQuery_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/PersonQuery_Add.kt")
        val personDeleteByIdFile = File(outputDir, "com/example/db/PersonQuery_DeleteById.kt")
        assertTrue(personGetByIdFile.exists(), "PersonQuery_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "PersonQuery_Add.kt file should be created")
        assertTrue(personDeleteByIdFile.exists(), "PersonQuery_DeleteById.kt file should be created")

        // Read the file content from one of the generated files
        val getByIdFileContent = personGetByIdFile.readText()
        val addFileContent = personAddFile.readText()
        val deleteByIdFileContent = personDeleteByIdFile.readText()

        // Verify that extension functions are generated with new structure
        assertTrue(getByIdFileContent.contains("fun PersonQuery.GetById.execute"), "Should contain GetById.execute extension function")
        assertTrue(getByIdFileContent.contains("fun PersonQuery.GetById.bindStatementParams"), "Should contain GetById.bindStatementParams extension function")
        assertTrue(getByIdFileContent.contains("fun PersonQuery.GetById.readStatementResult"), "Should contain GetById.readStatementResult extension function")
        assertTrue(addFileContent.contains("fun PersonQuery.Add.execute"), "Should contain Add.execute extension function")
        assertTrue(addFileContent.contains("fun PersonQuery.Add.bindStatementParams"), "Should contain Add.bindStatementParams extension function")
        assertTrue(deleteByIdFileContent.contains("fun PersonQuery.DeleteById.execute"), "Should contain DeleteById.execute extension function")
        assertTrue(deleteByIdFileContent.contains("fun PersonQuery.DeleteById.bindStatementParams"), "Should contain DeleteById.bindStatementParams extension function")

        // Verify function signatures with new structure
        assertTrue(getByIdFileContent.contains("conn: SafeSQLiteConnection"), "Should have SafeSQLiteConnection parameter")
        assertTrue(getByIdFileContent.contains("params: PersonQuery.GetById.Params"), "Should have GetById.Params parameter")
        assertTrue(addFileContent.contains("params: PersonQuery.Add.Params"), "Should have Add.Params parameter")
        assertTrue(deleteByIdFileContent.contains("params: PersonQuery.DeleteById.Params"), "Should have DeleteById.Params parameter")

        // Verify return types with new structure
        assertTrue(getByIdFileContent.contains("List<PersonGetByIdResult>"), "SELECT should return List of results")
        // For Unit return type, Kotlin doesn't require explicit declaration, so check for function without return type
        assertTrue(addFileContent.contains("fun PersonQuery.Add.execute(conn: SafeSQLiteConnection, params: PersonQuery.Add.Params)"),
                  "INSERT should have Unit return type (implicit)")
        assertTrue(deleteByIdFileContent.contains("fun PersonQuery.DeleteById.execute(conn: SafeSQLiteConnection, params: PersonQuery.DeleteById.Params)"),
                  "DELETE should have Unit return type (implicit)")

        // Verify SQL statement variables (now uses SQL constants from query objects)
        assertTrue(getByIdFileContent.contains("val sql = PersonQuery.GetById.SQL"), "Should use SQL constants from query objects")
        assertTrue(addFileContent.contains("val sql = PersonQuery.Add.SQL"), "Should use SQL constants from query objects")
        assertTrue(deleteByIdFileContent.contains("val sql = PersonQuery.DeleteById.SQL"), "Should use SQL constants from query objects")

        // Verify statement preparation in execute functions
        assertTrue(getByIdFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")
        assertTrue(addFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")
        assertTrue(deleteByIdFileContent.contains("val statement = conn.prepare(sql)"), "Should prepare SQL statement")

        // Verify that execute functions call bindStatementParams
        assertTrue(getByIdFileContent.contains("PersonQuery.GetById.bindStatementParams("), "Execute should call bindStatementParams")
        assertTrue(addFileContent.contains("PersonQuery.Add.bindStatementParams("), "Execute should call bindStatementParams")
        assertTrue(deleteByIdFileContent.contains("PersonQuery.DeleteById.bindStatementParams("), "Execute should call bindStatementParams")

        // Verify SELECT-specific implementation (only for SELECT statements)
        assertTrue(getByIdFileContent.contains("val results = mutableListOf<"), "Should contain results collection for SELECT")
        assertTrue(getByIdFileContent.contains("while (statement.step())"), "Should contain result iteration for SELECT")
        assertTrue(getByIdFileContent.contains("results.add("), "Should contain result addition for SELECT")

        // Verify that executeAsList calls readStatementResult for SELECT statements
        assertTrue(getByIdFileContent.contains("PersonQuery.GetById.readStatementResult("), "ExecuteAsList should call readStatementResult for SELECT")

        // Verify INSERT/DELETE-specific implementation (no return value)
        assertTrue(addFileContent.contains("statement.use { statement ->"), "Should contain statement execution for INSERT")
        assertTrue(deleteByIdFileContent.contains("statement.use { statement ->"), "Should contain statement execution for DELETE")

        // Verify file header comments
        assertTrue(getByIdFileContent.contains("Generated query extension functions for person.GetById"), "Should contain query-specific comment")

        // Verify imports
        assertTrue(getByIdFileContent.contains("import dev.goquick.sqlitenow.core.SafeSQLiteConnection"), "Should import dev.goquick.sqlitenow.core.SafeSQLiteConnection")

        // Verify suspend functions
        assertTrue(getByIdFileContent.contains("public suspend fun PersonQuery.GetById.executeAsList("), "ExecuteAsList function should be suspend")
        assertTrue(addFileContent.contains("public suspend fun PersonQuery.Add.execute("), "Execute function should be suspend")
        assertTrue(deleteByIdFileContent.contains("public suspend fun PersonQuery.DeleteById.execute("), "Execute function should be suspend")
    }

    @Test
    @DisplayName("Test QueryCodeGenerator with statements without parameters")
    fun testGenerateQueryFunctionsWithoutParameters() {
        // Create a test namespace directory with SQL files
        val userDir = File(queriesDir, "user")
        userDir.mkdirs()

        // Create a SELECT statement without parameters
        File(userDir, "getAll.sql").writeText("""
            -- @@{name=GetAll}
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
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the UserQuery_GetAll.kt file was created
        val userGetAllFile = File(outputDir, "com/example/db/UserQuery_GetAll.kt")
        assertTrue(userGetAllFile.exists(), "UserQuery_GetAll.kt file should be created")

        // Read the file content
        val fileContent = userGetAllFile.readText()

        // Verify that extension function is generated without params parameter
        assertTrue(fileContent.contains("suspend fun UserQuery.GetAll.executeAsList(conn: SafeSQLiteConnection): List<UserGetAllResult>"),
                  "Should contain GetAll.executeAsList extension function without params parameter and with suspend modifier")
        assertTrue(fileContent.contains("fun UserQuery.GetAll.bindStatementParams(statement: SQLiteStatement)"),
                  "Should contain GetAll.bindStatementParams extension function without params parameter")
        assertTrue(fileContent.contains("fun UserQuery.GetAll.readStatementResult(statement: SQLiteStatement"),
                  "Should contain GetAll.readStatementResult extension function")

        // Should not contain params parameter for statements without named parameters
        assertTrue(!fileContent.contains("params: UserQuery.GetAll.Params"), "Should not have params parameter for parameterless queries")
    }

    @Test
    @DisplayName("Test adapter annotation functionality with real SQL parsing")
    fun testAdapterAnnotationWithRealSqlParsing() {
        // This test verifies that the adapter annotation functionality works correctly
        // by testing the core components that handle adapter annotations.

        // Test 1: Verify FieldAnnotationOverrides correctly parses adapter annotations
        val fieldAnnotationsWithAdapter = mapOf(AnnotationConstants.ADAPTER to "custom", AnnotationConstants.PROPERTY_TYPE to "dev.example.Status")
        val fieldOverrides = FieldAnnotationOverrides.parse(fieldAnnotationsWithAdapter)
        assertTrue(fieldOverrides.adapter == true, "Should parse adapter flag annotation in SELECT fields")
        assertEquals("dev.example.Status", fieldOverrides.propertyType, "Should parse propertyType annotation")

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
            AnnotationConstants.ADAPTER to "custom",  // Flag annotation - key exists, value is null
            AnnotationConstants.PROPERTY_TYPE to "dev.example.CustomType"
        )

        val fieldOverrides = FieldAnnotationOverrides.parse(annotationsWithAdapter)
        assertTrue(fieldOverrides.adapter == true, "adapter flag annotation should be parsed as true")
        assertEquals("dev.example.CustomType", fieldOverrides.propertyType, "propertyType should be parsed correctly")

        // Test 2: Adapter annotation absent with custom type - should infer adapter=true
        val annotationsWithoutAdapter = mapOf(
            AnnotationConstants.PROPERTY_TYPE to "dev.example.AnotherType",
            AnnotationConstants.NOT_NULL to false
        )

        val fieldOverridesNoAdapter = FieldAnnotationOverrides.parse(annotationsWithoutAdapter)
        assertTrue(fieldOverridesNoAdapter.adapter == true, "Custom type should infer adapter=true")
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
        val annotationsWithAdapter = mapOf(AnnotationConstants.ADAPTER to "custom") // Flag annotation
        val fieldAnnotations = FieldAnnotationOverrides.parse(annotationsWithAdapter)
        assertTrue(fieldAnnotations.adapter == true, "Should parse adapter flag annotation correctly")

        val annotationsWithoutAdapter = mapOf(AnnotationConstants.PROPERTY_NAME to "customName")
        val fieldAnnotationsNoAdapter = FieldAnnotationOverrides.parse(annotationsWithoutAdapter)
        assertTrue(fieldAnnotationsNoAdapter.adapter == false, "Should handle missing adapter annotation")

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
            -- @@{name=GetById}
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@{name=Add}
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        File(personDir, "updateUser.sql").writeText("""
            -- @@{name=UpdateUser}
            UPDATE users SET name = :newName, email = :newEmail WHERE id = :userId;
        """.trimIndent())

        File(personDir, "deleteById.sql").writeText("""
            -- @@{name=DeleteById}
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
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that separate query files were created
        val personGetByIdFile = File(outputDir, "com/example/db/PersonQuery_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/PersonQuery_Add.kt")
        val personUpdateUserFile = File(outputDir, "com/example/db/PersonQuery_UpdateUser.kt")
        val personDeleteByIdFile = File(outputDir, "com/example/db/PersonQuery_DeleteById.kt")
        assertTrue(personGetByIdFile.exists(), "PersonQuery_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "PersonQuery_Add.kt file should be created")
        assertTrue(personUpdateUserFile.exists(), "PersonQuery_UpdateUser.kt file should be created")
        assertTrue(personDeleteByIdFile.exists(), "PersonQuery_DeleteById.kt file should be created")

        // Read the UPDATE file content
        val updateFileContent = personUpdateUserFile.readText()

        // Verify that UPDATE extension functions are generated
        assertTrue(updateFileContent.contains("suspend fun PersonQuery.UpdateUser.execute"), "Should contain UpdateUser.execute extension function with suspend modifier")
        assertTrue(updateFileContent.contains("fun PersonQuery.UpdateUser.bindStatementParams"), "Should contain UpdateUser.bindStatementParams extension function")

        // Verify UPDATE function signature
        assertTrue(updateFileContent.contains("params: PersonQuery.UpdateUser.Params"), "Should have UpdateUser.Params parameter")

        // Verify UPDATE return type (should be Unit, implicit)
        assertTrue(updateFileContent.contains("suspend fun PersonQuery.UpdateUser.execute(conn: SafeSQLiteConnection, params: PersonQuery.UpdateUser.Params)"),
                  "UPDATE should have Unit return type (implicit) and suspend modifier")

        // Verify UPDATE SQL execution implementation
        assertTrue(updateFileContent.contains("val sql = PersonQuery.UpdateUser.SQL"), "Should use SQL constant for UPDATE")
        assertTrue(updateFileContent.contains("statement.use { statement ->"), "Should contain statement execution for UPDATE")
        assertTrue(updateFileContent.contains("statement.step()"), "Should execute UPDATE statement")

        // Verify that execute calls bindStatementParams
        assertTrue(updateFileContent.contains("PersonQuery.UpdateUser.bindStatementParams("), "Execute should call bindStatementParams")
    }

    @Test
    @DisplayName("Test QueryCodeGenerator generates suspend functions with coroutines imports")
    fun testGenerateSuspendFunctions() {
        // Create a test namespace directory with SQL files
        val personDir = File(queriesDir, "person")
        personDir.mkdirs()

        // Create SQL files for testing both SELECT and INSERT statements
        File(personDir, "getById.sql").writeText("""
            -- @@{name=GetById}
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "add.sql").writeText("""
            -- @@{name=Add}
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
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the PersonQuery_GetById.kt and PersonQuery_Add.kt files were created
        val personGetByIdFile = File(outputDir, "com/example/db/PersonQuery_GetById.kt")
        val personAddFile = File(outputDir, "com/example/db/PersonQuery_Add.kt")
        assertTrue(personGetByIdFile.exists(), "PersonQuery_GetById.kt file should be created")
        assertTrue(personAddFile.exists(), "PersonQuery_Add.kt file should be created")

        // Read the file contents
        val getByIdFileContent = personGetByIdFile.readText()
        val addFileContent = personAddFile.readText()

        // Verify suspend modifier is present in execute functions
        assertTrue(getByIdFileContent.contains("public suspend fun PersonQuery.GetById.executeAsList("),
                  "SELECT executeAsList function should be suspend")
        assertTrue(addFileContent.contains("public suspend fun PersonQuery.Add.execute("),
                  "INSERT execute function should be suspend")

        // Verify that bindStatementParams and readStatementResult functions are NOT suspend
        assertTrue(getByIdFileContent.contains("public fun PersonQuery.GetById.bindStatementParams("),
                  "bindStatementParams should NOT be suspend")
        assertTrue(getByIdFileContent.contains("public fun PersonQuery.GetById.readStatementResult("),
                  "readStatementResult should NOT be suspend")
        assertTrue(addFileContent.contains("public fun PersonQuery.Add.bindStatementParams("),
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
            -- @@{name=GetById}
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
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify that the PersonQuery_GetById.kt file was created
        val personGetByIdFile = File(outputDir, "com/example/db/PersonQuery_GetById.kt")
        assertTrue(personGetByIdFile.exists(), "PersonQuery_GetById.kt file should be created")

        // Read the file content
        val fileContent = personGetByIdFile.readText()

        // Verify all three execute functions are generated
        assertTrue(fileContent.contains("public suspend fun PersonQuery.GetById.executeAsList("),
                  "Should contain executeAsList function")
        assertTrue(fileContent.contains("public suspend fun PersonQuery.GetById.executeAsOne("),
                  "Should contain executeAsOne function")
        assertTrue(fileContent.contains("public suspend fun PersonQuery.GetById.executeAsOneOrNull("),
                  "Should contain executeAsOneOrNull function")

        // Verify executeAsList implementation
        assertTrue(fileContent.contains("val results = mutableListOf<PersonGetByIdResult>()"),
                  "executeAsList should use mutableListOf")
        assertTrue(fileContent.contains("while (statement.step())"),
                  "executeAsList should use while loop")

        // Verify executeAsOne implementation
        assertTrue(fileContent.contains("throw IllegalStateException(\"Query returned no results, but exactly one result was expected\")"),
                  "executeAsOne should throw exception when no results")

        // Verify executeAsOneOrNull implementation
        assertTrue(fileContent.contains("} else {") && fileContent.contains("null"),
                  "executeAsOneOrNull should return null when no results")

        // Verify all functions use the same SQL preparation and parameter binding
        val sqlPreparationCount = fileContent.split("val sql = PersonQuery.GetById.SQL").size - 1
        val paramBindingCallCount = fileContent.split("PersonQuery.GetById.bindStatementParams(statement, params)").size - 1
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
            -- @@{name=SelectWithJoin}
            SELECT u.id, u.name, a.street
            FROM users u
            JOIN addresses a ON u.id = a.user_id
            WHERE u.id = :userId;
        """.trimIndent())

        File(personDir, "simpleSelect.sql").writeText("""
            -- @@{name=SimpleSelect}
            SELECT id, name FROM users WHERE id = :userId;
        """.trimIndent())

        File(personDir, "insertUser.sql").writeText("""
            -- @@{name=InsertUser}
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

        // Verify that the PersonQuery.kt file was created
        val personFile = File(outputDir, "com/example/db/PersonQuery.kt")
        assertTrue(personFile.exists(), "PersonQuery.kt file should be created")

        // Read the file content
        val fileContent = personFile.readText()

        // Verify affectedTables field exists for all query objects
        assertTrue(fileContent.contains("public val affectedTables: Set<String>"),
                  "PersonQuery.kt should contain affectedTables fields")

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

    @Test
    @DisplayName("Test QueryCodeGenerator generates RETURNING clause functions")
    fun testGenerateReturningClauseFunctions() {
        // Create a test namespace directory with SQL files
        val userDir = File(queriesDir, "user")
        userDir.mkdirs()

        // Create SQL files - one with RETURNING, one without
        File(userDir, "addWithReturning.sql").writeText("""
            -- @@{name=AddWithReturning}
            INSERT INTO users (name, email) VALUES (:userName, :userEmail) RETURNING *;
        """.trimIndent())

        File(userDir, "addWithoutReturning.sql").writeText("""
            -- @@{name=AddWithoutReturning}
            INSERT INTO users (name, email) VALUES (:userName, :userEmail);
        """.trimIndent())

        // Create an in-memory SQLite database for testing
        val realConnection = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the users table
        realConnection.createStatement().execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """.trimIndent())

        // Create table definition for the QueryCodeGenerator
        val usersTableStatement = AnnotatedCreateTableStatement(
            name = "CreateUsers",
            src = CreateTableStatement(
                sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)",
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
                        name = "email",
                        dataType = "TEXT",
                        notNull = true,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false
                    ),
                    CreateTableStatement.Column(
                        name = "created_at",
                        dataType = "TEXT",
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
                queryResult = null,
                collectionKey = null
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column("id", "INTEGER", false, true, false, false),
                    annotations = emptyMap()
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column("name", "TEXT", true, false, false, false),
                    annotations = emptyMap()
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column("email", "TEXT", true, false, false, false),
                    annotations = emptyMap()
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        "created_at",
                        "TEXT",
                        false,
                        false,
                        false,
                        false
                    ),
                    annotations = emptyMap()
                )
            )
        )

        // Create DataStructCodeGenerator with real SQL files
        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = realConnection,
            queriesDir = queriesDir,
            createTableStatements = listOf(usersTableStatement),
            packageName = "com.test.db",
            outputDir = outputDir
        )

        // Create QueryCodeGenerator
        val queryGenerator = QueryCodeGenerator(
            generatorContext = dataStructGenerator.generatorContext,
            dataStructCodeGenerator = dataStructGenerator
        )

        // Generate the query code
        queryGenerator.generateCode()

        // Verify RETURNING query generates three functions
        val returningFile = File(outputDir, "com/test/db/UserQuery_AddWithReturning.kt")
        assertTrue(returningFile.exists(), "AddWithReturning query file should be generated")

        val returningContent = returningFile.readText()

        // Should generate three RETURNING functions
        assertTrue(returningContent.contains("fun UserQuery.AddWithReturning.executeReturningList("),
                  "Should generate executeReturningList function")
        assertTrue(returningContent.contains("fun UserQuery.AddWithReturning.executeReturningOne("),
                  "Should generate executeReturningOne function")
        assertTrue(returningContent.contains("fun UserQuery.AddWithReturning.executeReturningOneOrNull("),
                  "Should generate executeReturningOneOrNull function")

        // Check return types
        assertTrue(returningContent.contains("): List<UserAddWithReturningResult>"),
                  "executeReturningList should return List<Result>")
        assertTrue(returningContent.contains("): UserAddWithReturningResult ="),
                  "executeReturningOne should return Result")
        assertTrue(returningContent.contains("): UserAddWithReturningResult? ="),
                  "executeReturningOneOrNull should return Result?")

        // Verify non-RETURNING query generates single execute function
        val nonReturningFile = File(outputDir, "com/test/db/UserQuery_AddWithoutReturning.kt")
        assertTrue(nonReturningFile.exists(), "AddWithoutReturning query file should be generated")

        val nonReturningContent = nonReturningFile.readText()

        // Should generate only execute function
        assertTrue(nonReturningContent.contains("fun UserQuery.AddWithoutReturning.execute("),
                  "Should generate execute function")
        assertFalse(nonReturningContent.contains("executeReturning"),
                   "Should not generate executeReturning functions")

        realConnection.close()
    }


}
