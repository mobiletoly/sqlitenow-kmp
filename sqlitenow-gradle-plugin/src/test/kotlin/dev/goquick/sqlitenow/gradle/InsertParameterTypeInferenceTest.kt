package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertEquals

class InsertParameterTypeInferenceTest {

    @Test
    fun testInsertParameterTypeInferenceWithAdapters() {
        // Create an in-memory SQLite connection
        val conn = java.sql.DriverManager.getConnection("jdbc:sqlite::memory:")

        // Create the Person table with annotations
        conn.createStatement().execute(
            """
            CREATE TABLE person (
                id INTEGER PRIMARY KEY NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                phone TEXT,
                birth_date TEXT,
                created_at TEXT NOT NULL DEFAULT current_timestamp,
                notes BLOB
            )
        """.trimIndent()
        )

        // Create annotated table statement with the same annotations as in the user's schema
        val createTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "Person",
                src = CreateTableStatement(
                    sql = "CREATE TABLE person (...)",
                    tableName = "person",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "birth_date",
                            dataType = "TEXT",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "created_at",
                            dataType = "TEXT",
                            notNull = true,
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
                    queryResult = null,
                    implements = null,
                    excludeOverrideFields = null,
                    collectionKey = null
                ),
                columns = listOf(
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "birth_date",
                            dataType = "TEXT",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = mapOf(
                            AnnotationConstants.ADAPTER to "custom",
                            AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDate"
                        )
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "created_at",
                            dataType = "TEXT",
                            notNull = true,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = mapOf(
                            AnnotationConstants.ADAPTER to "custom",
                            AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDateTime"
                        )
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
                        annotations = mapOf(
                            AnnotationConstants.ADAPTER to "custom",
                            AnnotationConstants.PROPERTY_TYPE to "dev.goquick.sqlitenow.samplekmp.model.PersonNote"
                        )
                    )
                )
            )
        )

        // Create DataStructCodeGenerator
        val packageName = "com.example.db"
        val dataStructGenerator = DataStructCodeGenerator(
            conn = conn,
            queriesDir = Files.createTempDirectory("test-queries").toFile(),
            packageName = packageName,
            outputDir = Files.createTempDirectory("test-output").toFile(),
            statementExecutors = mutableListOf(),
            providedCreateTableStatements = createTableStatements
        )

        // Parse the INSERT statement from the user's example
        val sql = """
            INSERT INTO Person(email, first_name, last_name, phone, birth_date, notes)
            VALUES ('hello@world.com', :firstName, (SELECT last_name FROM Person WHERE phone = :phone LIMIT 1), :phone, :birthDate, :notes)
            ON CONFLICT(email) DO UPDATE SET 
                first_name = :firstName,
                last_name = :lastName,
                phone = '333-444-5555',
                notes = :newNotes
        """.trimIndent()

        val statement = CCJSqlParserUtil.parse(sql) as Insert
        val insertStatement = InsertStatement.parse(statement, conn)

        val annotatedInsertStatement = AnnotatedExecuteStatement(
            name = "add",
            src = insertStatement,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            )
        )

        // Test parameter type inference
        println("Testing parameter type inference:")

        // birthDate should map to LocalDate? (nullable because column allows NULL)
        val birthDateType = dataStructGenerator.inferParameterType("birthDate", annotatedInsertStatement)
        println("birthDate -> $birthDateType")
        assertEquals(
            "kotlinx.datetime.LocalDate?", birthDateType.toString(),
            "birthDate should map to nullable LocalDate due to propertyType annotation"
        )

        // notes should map to PersonNote? (nullable because column allows NULL)
        val notesType = dataStructGenerator.inferParameterType("notes", annotatedInsertStatement)
        println("notes -> $notesType")
        assertEquals(
            "dev.goquick.sqlitenow.samplekmp.model.PersonNote?", notesType.toString(),
            "notes should map to nullable PersonNote due to propertyType annotation"
        )

        // newNotes should also map to PersonNote? (same column)
        val newNotesType = dataStructGenerator.inferParameterType("newNotes", annotatedInsertStatement)
        println("newNotes -> $newNotesType")
        assertEquals(
            "dev.goquick.sqlitenow.samplekmp.model.PersonNote?", newNotesType.toString(),
            "newNotes should map to nullable PersonNote due to propertyType annotation"
        )

        conn.close()
    }
}
