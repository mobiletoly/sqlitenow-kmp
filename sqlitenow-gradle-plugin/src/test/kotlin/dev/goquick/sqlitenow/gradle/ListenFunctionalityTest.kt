package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.database.DatabaseCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import java.io.File
import java.nio.file.Path
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class ListenFunctionalityTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    @DisplayName("DatabaseCodeGenerator emits reactive flow scaffolding against executeAsList")
    fun databaseCodeGeneratorEmitsReactiveFlowScaffolding() {
        val queriesDir = tempDir.resolve("queries").toFile().apply {
            resolve("person").mkdirs()
        }
        val outputDir = tempDir.resolve("output").toFile().apply { mkdirs() }

        File(queriesDir, "person/selectByBirthDate.sql").writeText(
            """
            -- @@{name=SelectByBirthDate}
            SELECT
                id,
                /* @@{ field=birth_date, adapter=custom } */
                birth_date
            FROM person
            WHERE birth_date >= :birthDateStart;
            """.trimIndent()
        )

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY,
                birth_date TEXT
            )
            """.trimIndent()
        )

        val createTables = listOf(
            annotatedPersonTableWithBirthDateAdapter()
        )

        val dataStructGenerator = createDataStructCodeGeneratorWithMockExecutors(
            conn = conn,
            queriesDir = queriesDir,
            createTableStatements = createTables,
            packageName = "fixture.db",
            outputDir = outputDir,
        )

        DatabaseCodeGenerator(
            nsWithStatements = dataStructGenerator.nsWithStatements,
            createTableStatements = dataStructGenerator.createTableStatements,
            createViewStatements = dataStructGenerator.createViewStatements,
            packageName = "fixture.db",
            outputDir = outputDir,
            databaseClassName = "FixtureDatabase",
        ).generateDatabaseClass()

        val generated = File(outputDir, "fixture/db/FixtureDatabase.kt").readText()
        assertTrue(generated.contains("override fun asFlow() = ref.createReactiveQueryFlow("))
        assertTrue(generated.contains("affectedTables = PersonQuery.SelectByBirthDate.affectedTables"))
        assertTrue(generated.contains("queryExecutor = {"))
        assertTrue(generated.contains("PersonQuery.SelectByBirthDate.executeAsList("))
        assertTrue(generated.contains("birthDateToSqlValue = ref.personAdapters.birthDateToSqlValue"))

        conn.close()
    }

    @Test
    @DisplayName("DatabaseCodeGenerator emits listener notifications after execute-returning blocks resolve")
    fun databaseCodeGeneratorEmitsNotificationsForExecuteReturningBlocks() {
        val queriesDir = tempDir.resolve("queries").toFile().apply {
            resolve("person").mkdirs()
        }
        val outputDir = tempDir.resolve("output").toFile().apply { mkdirs() }

        File(queriesDir, "person/addReturning.sql").writeText(
            """
            -- @@{name=AddReturning}
            INSERT INTO person (birth_date)
            VALUES (:birthDate)
            RETURNING id, birth_date;
            """.trimIndent()
        )

        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.createStatement().execute(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY,
                birth_date TEXT
            )
            """.trimIndent()
        )

        val helper = StatementProcessingHelper(conn, FieldAnnotationResolver(emptyList(), emptyList()))
        val statements = helper.processQueriesDirectory(queriesDir)

        DatabaseCodeGenerator(
            nsWithStatements = statements,
            createTableStatements = listOf(annotatedPersonTableWithBirthDateAdapter()),
            createViewStatements = emptyList(),
            packageName = "fixture.db",
            outputDir = outputDir,
            databaseClassName = "FixtureDatabase",
            debug = true,
        ).generateDatabaseClass()

        val generated = File(outputDir, "fixture/db/FixtureDatabase.kt").readText()
        assertTrue(generated.contains("ExecuteReturningStatement("))
        assertTrue(generated.contains("val result = PersonQuery.AddReturning.executeReturningList("))
        assertTrue(generated.contains("val result = PersonQuery.AddReturning.executeReturningOne("))
        assertTrue(generated.contains("val result = PersonQuery.AddReturning.executeReturningOneOrNull("))
        assertTrue(generated.contains("sqliteNowLogger.d { \"notifyTablesChanged -> \" + PersonQuery.AddReturning.affectedTables.joinToString(\", \") }"))

        val listNotify = generated.indexOf("ref.notifyTablesChanged(PersonQuery.AddReturning.affectedTables)")
        val listResult = generated.indexOf("val result = PersonQuery.AddReturning.executeReturningList(")
        assertTrue(listResult >= 0 && listNotify > listResult)
        assertFalse(generated.contains("ref.notifyTablesChanged(PersonQuery.AddReturning.affectedTables)\n                val result"))

        conn.close()
    }

    private fun annotatedPersonTableWithBirthDateAdapter(): AnnotatedCreateTableStatement =
        AnnotatedCreateTableStatement(
            name = "CreatePerson",
            src = CreateTableStatement(
                sql = "CREATE TABLE person (id INTEGER PRIMARY KEY, birth_date TEXT)",
                tableName = "person",
                columns = listOf(
                    CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = false,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    CreateTableStatement.Column(
                        name = "birth_date",
                        dataType = "TEXT",
                        notNull = false,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                ),
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
            ),
            columns = listOf(
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "id",
                        dataType = "INTEGER",
                        notNull = false,
                        primaryKey = true,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = emptyMap(),
                ),
                AnnotatedCreateTableStatement.Column(
                    src = CreateTableStatement.Column(
                        name = "birth_date",
                        dataType = "TEXT",
                        notNull = false,
                        primaryKey = false,
                        autoIncrement = false,
                        unique = false,
                    ),
                    annotations = mapOf(AnnotationConstants.ADAPTER to "custom"),
                ),
            ),
        )
}
