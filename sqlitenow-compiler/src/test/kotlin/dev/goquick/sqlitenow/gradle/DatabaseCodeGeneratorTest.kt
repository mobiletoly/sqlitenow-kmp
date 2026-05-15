package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.database.DatabaseCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
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
        writeQueryFiles(
            namespace = "person",
            files = mapOf(
                "selectWithAdapters.sql" to """
                    -- @@{name=SelectWithAdapters}
                    SELECT
                        id,
                        name,
                        /* @@{ field=birth_date, adapter=custom } */
                        birth_date
                    FROM users
                    WHERE birth_date >= :myBirthDateStart;
                """.trimIndent(),
                "insertWithAdapters.sql" to """
                    -- @@{name=InsertWithAdapters}
                    INSERT INTO users (name, birth_date, notes)
                    VALUES (:name, :birthDate, :notes);
                """.trimIndent(),
            ),
        )

        val fileContent = generatedDatabaseContent(
            tableSql = """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    birth_date TEXT,
                    notes BLOB
                )
            """.trimIndent(),
            createTableStatements = usersCreateTableStatements(
                columns = listOf(
                    annotatedTableColumn(name = "id", dataType = "INTEGER", notNull = false, primaryKey = true),
                    annotatedTableColumn(name = "name", dataType = "TEXT", notNull = true),
                    annotatedTableColumn(name = "birth_date", dataType = "TEXT", notNull = false, adapter = true),
                    annotatedTableColumn(name = "notes", dataType = "BLOB", notNull = false, adapter = true),
                )
            ),
            databaseClassName = "TestDatabase",
        )

        // Verify basic structure
        assertTrue(fileContent.contains("class TestDatabase"), "Should contain TestDatabase class")
        assertTrue(fileContent.contains(": SqliteNowDatabase"), "Should extend SqliteNowDatabase")

        // Verify constructor parameters for adapter wrappers
        assertTrue(fileContent.contains("personAdapters: PersonAdapters"), "Should contain personAdapters parameter")

        // Verify router class
        assertTrue(fileContent.contains("class PersonRouter"), "Should contain PersonRouter class")
        assertTrue(fileContent.contains("val person: PersonRouter"), "Should contain person router property")

        // Verify router methods - now using SelectRunners pattern
        assertTrue(
            fileContent.contains("val selectWithAdapters"),
            "Should contain selectWithAdapters SelectRunners property"
        )
        assertTrue(fileContent.contains("SelectRunners<"), "Should contain SelectRunners interface usage")
        assertTrue(
            fileContent.contains("override suspend fun asList()"),
            "Should contain asList() method in SelectRunners"
        )
        assertTrue(
            fileContent.contains("override suspend fun asOne()"),
            "Should contain asOne() method in SelectRunners"
        )
        assertTrue(
            fileContent.contains("override suspend fun asOneOrNull()"),
            "Should contain asOneOrNull() method in SelectRunners"
        )
        assertTrue(fileContent.contains("override fun asFlow()"), "Should contain asFlow() method in SelectRunners")
        assertTrue(
            fileContent.contains("ExecuteStatement<"),
            "Should contain ExecuteStatement wrapper"
        )

        // Verify adapter wrapper classes
        assertTrue(fileContent.contains("data class PersonAdapters"), "Should contain PersonAdapters data class")

        // Verify adapter parameter passing through wrapper
        assertTrue(fileContent.contains("ref.personAdapters."), "Should pass adapter parameters through wrapper")
    }

    @TestFactory
    fun databaseRouterGenerationScenarios(): List<DynamicTest> = listOf(
        DatabaseRouterGenerationScenario(
            displayName = "namespaces without adapters omit adapter plumbing",
            queryNamespaces = listOf(
                QueryNamespaceFixture(
                    namespace = "person",
                    files = mapOf(
                        "selectWithoutAdapters.sql" to """
                            -- @@{name=SelectWithoutAdapters}
                            SELECT id, name FROM users WHERE id = :userId;
                        """.trimIndent(),
                    ),
                ),
                QueryNamespaceFixture(
                    namespace = "utils",
                    files = mapOf(
                        "simpleQuery.sql" to """
                            -- @@{name=SimpleQuery}
                            SELECT id, name FROM users WHERE id = :userId;
                        """.trimIndent(),
                    ),
                ),
            ),
            tableSql = "CREATE TABLE users (id INTEGER, name TEXT, birth_date TEXT)",
            expectations = listOf(
                GeneratedContentExpectation("public val person: PersonRouter", "Should have person router property"),
                GeneratedContentExpectation("public val utils: UtilsRouter", "Should have utils router property"),
                GeneratedContentExpectation(
                    "public data class PersonAdapters(",
                    "Should NOT generate PersonAdapters data class when no adapters are used",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "public data class UtilsAdapters(",
                    "Should NOT generate UtilsAdapters data class for namespace without adapters",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "private val personAdapters: PersonAdapters",
                    "Should NOT have constructor parameter for PersonAdapters when no adapters",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "private val utilsAdapters: UtilsAdapters",
                    "Should NOT have constructor parameter for UtilsAdapters",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "ref.personAdapters.",
                    "Person router methods should not reference adapters when none exist",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "ref.utilsAdapters.",
                    "Utils router methods should not reference adapters",
                    present = false,
                ),
            ),
        ),
        DatabaseRouterGenerationScenario(
            displayName = "select queries generate SelectRunners objects",
            queryNamespaces = listOf(
                QueryNamespaceFixture(
                    namespace = "person",
                    files = mapOf(
                        "selectAll.sql" to """
                            -- @@{name=SelectAll}
                            SELECT id, name FROM users;
                        """.trimIndent(),
                        "selectById.sql" to """
                            -- @@{name=SelectById}
                            SELECT id, name FROM users WHERE id = :userId;
                        """.trimIndent(),
                    ),
                ),
            ),
            tableSql = "CREATE TABLE users (id INTEGER, name TEXT)",
            expectations = listOf(
                GeneratedContentExpectation("val selectAll", "Should have selectAll property"),
                GeneratedContentExpectation("val selectById", "Should have selectById property"),
                GeneratedContentExpectation("object : SelectRunners<", "Should contain SelectRunners object expressions"),
                GeneratedContentExpectation("override suspend fun asList()", "Should contain asList() method implementation"),
                GeneratedContentExpectation("override suspend fun asOne()", "Should contain asOne() method implementation"),
                GeneratedContentExpectation(
                    "override suspend fun asOneOrNull()",
                    "Should contain asOneOrNull() method implementation",
                ),
                GeneratedContentExpectation("override fun asFlow()", "Should contain asFlow() method implementation"),
                GeneratedContentExpectation("{ params ->", "Should have lambda function for parameterized queries"),
                GeneratedContentExpectation(
                    "suspend fun selectAllAsList",
                    "Should NOT generate individual selectAllAsList method",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "suspend fun selectAllAsOne",
                    "Should NOT generate individual selectAllAsOne method",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "suspend fun selectAllAsOneOrNull",
                    "Should NOT generate individual selectAllAsOneOrNull method",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "fun selectAllFlow",
                    "Should NOT generate individual selectAllFlow method",
                    present = false,
                ),
            ),
        ),
        DatabaseRouterGenerationScenario(
            displayName = "execute queries generate ExecuteStatement wrappers",
            queryNamespaces = listOf(
                QueryNamespaceFixture(
                    namespace = "person",
                    files = mapOf(
                        "addSimple.sql" to """
                            -- @@{name=AddSimple}
                            INSERT INTO users (name) VALUES ('test');
                        """.trimIndent(),
                        "addWithParams.sql" to """
                            -- @@{name=AddWithParams}
                            INSERT INTO users (name, email) VALUES (:name, :email);
                        """.trimIndent(),
                        "updateById.sql" to """
                            -- @@{name=UpdateById}
                            UPDATE users SET name = :name WHERE id = :id;
                        """.trimIndent(),
                        "addReturning.sql" to """
                            -- @@{name=AddReturning}
                            INSERT INTO users (name) VALUES (:name) RETURNING id, name;
                        """.trimIndent(),
                    ),
                ),
            ),
            tableSql = "CREATE TABLE users (id INTEGER, name TEXT, email TEXT)",
            expectations = listOf(
                GeneratedContentExpectation(
                    "suspend fun addSimple(",
                    "Should generate addSimple suspend function for statements without parameters",
                ),
                GeneratedContentExpectation("val addWithParams:", "Should have addWithParams property"),
                GeneratedContentExpectation(
                    "ExecuteStatement<PersonQuery.AddWithParams.Params>",
                    "Should expose ExecuteStatement with params type",
                ),
                GeneratedContentExpectation("val updateById:", "Should have updateById property"),
                GeneratedContentExpectation("val addReturning:", "Should have addReturning property"),
                GeneratedContentExpectation("ExecuteStatement(", "Should instantiate ExecuteStatement wrapper"),
                GeneratedContentExpectation("ExecuteReturningStatement(", "Should instantiate ExecuteReturningStatement wrapper"),
                GeneratedContentExpectation(
                    "ExecuteReturningStatement<PersonQuery.AddReturning.Params, PersonAddReturningResult>",
                    "Should expose ExecuteReturningStatement with params and result types",
                ),
                GeneratedContentExpectation("ref.notifyTablesChanged", "Should contain table change notifications"),
                GeneratedContentExpectation(
                    "suspend fun addWithParams(",
                    "Should NOT generate individual addWithParams method",
                    present = false,
                ),
                GeneratedContentExpectation(
                    "suspend fun updateById(",
                    "Should NOT generate individual updateById method",
                    present = false,
                ),
            ),
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            resetGeneratedFixtureDirs()
            case.queryNamespaces.forEach { queryNamespace ->
                writeQueryFiles(namespace = queryNamespace.namespace, files = queryNamespace.files)
            }

            val fileContent = generatedDatabaseContent(case.tableSql)

            assertGeneratedContentMatches(fileContent, case.expectations)
        }
    }

    private fun writeQueryFiles(namespace: String, files: Map<String, String>) {
        val namespaceDir = File(queriesDir, namespace).apply { mkdirs() }
        files.forEach { (name, sql) -> File(namespaceDir, name).writeText(sql) }
    }

    private data class DatabaseRouterGenerationScenario(
        val displayName: String,
        val queryNamespaces: List<QueryNamespaceFixture>,
        val tableSql: String,
        val expectations: List<GeneratedContentExpectation>,
    )

    private data class QueryNamespaceFixture(
        val namespace: String,
        val files: Map<String, String>,
    )

    private data class GeneratedContentExpectation(
        val snippet: String,
        val message: String,
        val present: Boolean = true,
    )

    private fun assertGeneratedContentMatches(
        fileContent: String,
        expectations: List<GeneratedContentExpectation>,
    ) {
        expectations.forEach { expectation ->
            assertEquals(
                expectation.present,
                fileContent.contains(expectation.snippet),
                expectation.message,
            )
        }
    }

    private fun generatedDatabaseContent(tableSql: String): String {
        java.sql.DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement -> statement.execute(tableSql) }

            val nsWithStatements = StatementProcessingHelper(
                connection,
                FieldAnnotationResolver(emptyList(), emptyList())
            ).processQueriesDirectory(queriesDir)

            DatabaseCodeGenerator(
                nsWithStatements = nsWithStatements,
                createTableStatements = emptyList(),
                createViewStatements = emptyList(),
                packageName = "com.example.db",
                outputDir = outputDir,
                databaseClassName = "TestDatabase"
            ).generateDatabaseClass()
        }

        val databaseFile = File(outputDir, "com/example/db/TestDatabase.kt")
        assertTrue(databaseFile.exists(), "TestDatabase.kt file should be created")
        return databaseFile.readText()
    }

    private fun generatedDatabaseContent(
        tableSql: String,
        createTableStatements: List<AnnotatedCreateTableStatement>,
        databaseClassName: String,
    ): String {
        java.sql.DriverManager.getConnection("jdbc:sqlite::memory:").use { connection ->
            connection.createStatement().use { statement -> statement.execute(tableSql) }

            val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
                conn = connection,
                queriesDir = queriesDir,
                createTableStatements = createTableStatements,
                packageName = "com.example.db",
                outputDir = outputDir
            )

            DatabaseCodeGenerator(
                nsWithStatements = dataStructGenerator.nsWithStatements,
                createTableStatements = dataStructGenerator.createTableStatements,
                createViewStatements = dataStructGenerator.createViewStatements,
                packageName = "com.example.db",
                outputDir = outputDir,
                databaseClassName = databaseClassName
            ).generateDatabaseClass()
        }

        val databaseFile = File(outputDir, "com/example/db/$databaseClassName.kt")
        assertTrue(databaseFile.exists(), "$databaseClassName.kt file should be created")
        return databaseFile.readText()
    }

    @Test
    @DisplayName("Test DatabaseCodeGenerator deduplicates identical created_at adapters")
    fun testCreatedAtAdapterDeduplicationScenarios() {
        val cases = listOf(
            CreatedAtAdapterDeduplicationCase(
                description = "minimal duplicate adapter queries",
                databaseClassName = "DeduplicationTestDatabase",
                queryFiles = mapOf(
                    "query1.sql" to """
                        -- @@{name=Query1}
                        SELECT
                            id,
                            /* @@{ field=created_at, adapter=custom } */
                            created_at
                        FROM users
                        WHERE id = :userId;
                    """.trimIndent(),
                    "query2.sql" to """
                        -- @@{name=Query2}
                        SELECT
                            name,
                            /* @@{ field=created_at, adapter=custom } */
                            created_at
                        FROM users
                        WHERE name = :userName;
                    """.trimIndent(),
                )
            ),
            CreatedAtAdapterDeduplicationCase(
                description = "real-world select names",
                databaseClassName = "RealWorldTestDatabase",
                queryFiles = mapOf(
                    "selectWithCreatedAt1.sql" to """
                        -- @@{name=SelectWithCreatedAt1}
                        SELECT
                            id,
                            name,
                            /* @@{ field=created_at, adapter=custom } */
                            created_at
                        FROM users
                        WHERE id = :userId;
                    """.trimIndent(),
                    "selectWithCreatedAt2.sql" to """
                        -- @@{name=SelectWithCreatedAt2}
                        SELECT
                            name,
                            /* @@{ field=created_at, adapter=custom } */
                            created_at
                        FROM users
                        WHERE name = :userName;
                    """.trimIndent(),
                )
            )
        )

        cases.forEach { case ->
            resetGeneratedFixtureDirs()
            writeQueryFiles(namespace = "person", files = case.queryFiles)

            val fileContent = generatedCreatedAtAdapterDatabaseContent(case.databaseClassName)

            assertCreatedAtAdapterDeduplicated(fileContent, case.description)
        }
    }

    private data class CreatedAtAdapterDeduplicationCase(
        val description: String,
        val databaseClassName: String,
        val queryFiles: Map<String, String>,
    )

    private fun resetGeneratedFixtureDirs() {
        queriesDir.deleteRecursively()
        outputDir.deleteRecursively()
        queriesDir.mkdirs()
        outputDir.mkdirs()
    }

    private fun generatedCreatedAtAdapterDatabaseContent(databaseClassName: String): String {
        return generatedDatabaseContent(
            tableSql = "CREATE TABLE users (id INTEGER, name TEXT, created_at TEXT)",
            createTableStatements = createdAtAdapterCreateTableStatements(),
            databaseClassName = databaseClassName,
        )
    }

    private fun createdAtAdapterCreateTableStatements(): List<AnnotatedCreateTableStatement> = listOf(
        annotatedCreateTable(
            tableName = "users",
            columns = listOf(
                annotatedTableColumn(name = "id", dataType = "INTEGER", notNull = false),
                annotatedTableColumn(name = "name", dataType = "TEXT", notNull = false),
                annotatedTableColumn(name = "created_at", dataType = "TEXT", notNull = false, adapter = true),
            )
        )
    )

    private fun usersCreateTableStatements(
        columns: List<AnnotatedCreateTableStatement.Column>,
    ): List<AnnotatedCreateTableStatement> = listOf(
        annotatedCreateTable(
            tableName = "users",
            columns = columns,
        )
    )

    private fun assertCreatedAtAdapterDeduplicated(fileContent: String, description: String) {
        val personAdaptersContent = fileContent.substringAfter("public data class PersonAdapters(")
            .substringBefore(")")

        val sqlValueToCreatedAtCount = personAdaptersContent.split("sqlValueToCreatedAt").size - 1
        assertEquals(
            1,
            sqlValueToCreatedAtCount,
            "$description: sqlValueToCreatedAt should appear exactly once in PersonAdapters data class, " +
                "but found $sqlValueToCreatedAtCount occurrences"
        )
        assertTrue(fileContent.contains("sqlValueToCreatedAt"), "$description: should contain sqlValueToCreatedAt adapter")
    }

    @Test
    @DisplayName("Test adapter assignment for built-in types vs custom types")
    fun testAdapterAssignmentForBuiltInVsCustomTypes() {
        writeQueryFiles(
            namespace = "person",
            files = mapOf(
                "insertPerson.sql" to """
                    -- @@{name=InsertPerson}
                    INSERT INTO users (name, phone, birth_date) VALUES (:name, :phone, :birthDate);
                """.trimIndent(),
                "selectPerson.sql" to """
                    -- @@{name=SelectPerson}
                    SELECT
                        id,
                        name,
                        /* @@{ field=phone, adapter=custom } */
                        phone,
                        /* @@{ field=birth_date, adapter=custom } */
                        birth_date
                    FROM users
                    WHERE id = :userId;
                """.trimIndent(),
            ),
        )

        val fileContent = generatedDatabaseContent(
            tableSql = """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    phone TEXT,
                    birth_date TEXT
                )
            """.trimIndent(),
            createTableStatements = usersCreateTableStatements(
                columns = listOf(
                    annotatedTableColumn(name = "id", dataType = "INTEGER", notNull = true, primaryKey = true),
                    annotatedTableColumn(name = "name", dataType = "TEXT", notNull = true),
                    annotatedTableColumn(
                        name = "phone",
                        dataType = "TEXT",
                        notNull = false,
                        adapter = true,
                        annotationNotNull = true,
                    ),
                    annotatedTableColumn(
                        name = "birth_date",
                        dataType = "TEXT",
                        notNull = false,
                        propertyType = "kotlinx.datetime.LocalDate",
                        adapter = true,
                    ),
                )
            ),
            databaseClassName = "AdapterTestDatabase",
        )

        // Check the INSERT statement adapter assignments
        val insertPersonContent = fileContent.substringAfter("insertPerson")
            .substringBefore("}")

        // For built-in type (phone with String type), should use phoneToSqlValue (not sqlValueToPhone)
        assertTrue(
            insertPersonContent.contains("phoneToSqlValue = ref.personAdapters.phoneToSqlValue"),
            "Built-in type adapter should use phoneToSqlValue, not sqlValueToPhone. Content: $insertPersonContent"
        )

        // For custom type (birth_date with LocalDate type), should also use birthDateToSqlValue
        assertTrue(
            insertPersonContent.contains("birthDateToSqlValue = ref.personAdapters.birthDateToSqlValue"),
            "Custom type adapter should use birthDateToSqlValue. Content: $insertPersonContent"
        )

        // Verify that the wrong assignment is NOT present
        assertFalse(
            insertPersonContent.contains("phoneToSqlValue = ref.personAdapters.sqlValueToPhone"),
            "Should NOT have reversed adapter assignment for built-in type"
        )
    }

}
