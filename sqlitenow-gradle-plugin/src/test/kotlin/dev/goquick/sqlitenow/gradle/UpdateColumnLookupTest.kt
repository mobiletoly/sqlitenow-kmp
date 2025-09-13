package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.inspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.inspect.UpdateStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class UpdateColumnLookupTest {

    @Test
    @DisplayName("Test UPDATE statement column lookup uses columnNamesAssociatedWithNamedParameters")
    fun testUpdateColumnLookupUsesDirectMapping() {
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
                            name = "age",
                            dataType = "INTEGER",
                            notNull = false,  // NULL allowed
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "score",
                            dataType = "INTEGER",
                            notNull = true,  // NOT NULL
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "notes",
                            dataType = "BLOB",
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
                    sharedResult = null,
                    implements = null,
                    excludeOverrideFields = null,
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
                            name = "age",
                            dataType = "INTEGER",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "score",
                            dataType = "INTEGER",
                            notNull = true,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
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
                        annotations = emptyMap()
                    )
                )
            )
        )

        // Create ColumnLookup
        val columnLookup = ColumnLookup(createTableStatements, createViewStatements = emptyList())

        // Create a mock UPDATE statement that matches the user's example
        val mockUpdateStatement = AnnotatedExecuteStatement(
            name = "updateById",
            src = UpdateStatement(
                sql = "UPDATE person SET age = ?, score = ?, notes = ? WHERE id = ? AND birth_date <= ? AND birth_date >= ?",
                table = "person",
                namedParameters = listOf("myAge", "myScore", "myNotes", "id", "myBirthDate", "myBirthDate"),
                namedParametersToColumns = mapOf(
                    // WHERE clause parameters
                    "id" to AssociatedColumn.Default("id"),
                    "myBirthDate" to AssociatedColumn.Default("birth_date")
                ),
                namedParametersToColumnNames = mapOf(
                    // SET clause parameters - this is the key fix!
                    "myAge" to "age",
                    "myScore" to "score",
                    "myNotes" to "notes"
                ),
                withSelectStatements = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            )
        )

        // Test that SET clause parameters use the direct column mapping
        val myAgeColumn = columnLookup.findColumnForParameter(mockUpdateStatement, "myAge")
        assertNotNull(myAgeColumn, "Should find column for myAge parameter")
        assertEquals("age", myAgeColumn.src.name, "myAge should map to age column")
        assertEquals("INTEGER", myAgeColumn.src.dataType, "age column should be INTEGER")
        assertEquals(false, myAgeColumn.src.notNull, "age column should allow NULL")

        val myScoreColumn = columnLookup.findColumnForParameter(mockUpdateStatement, "myScore")
        assertNotNull(myScoreColumn, "Should find column for myScore parameter")
        assertEquals("score", myScoreColumn.src.name, "myScore should map to score column")
        assertEquals("INTEGER", myScoreColumn.src.dataType, "score column should be INTEGER")
        assertEquals(true, myScoreColumn.src.notNull, "score column should be NOT NULL")

        val myNotesColumn = columnLookup.findColumnForParameter(mockUpdateStatement, "myNotes")
        assertNotNull(myNotesColumn, "Should find column for myNotes parameter")
        assertEquals("notes", myNotesColumn.src.name, "myNotes should map to notes column")
        assertEquals("BLOB", myNotesColumn.src.dataType, "notes column should be BLOB")
        assertEquals(false, myNotesColumn.src.notNull, "notes column should allow NULL")

        // Test that WHERE clause parameters still work
        val idColumn = columnLookup.findColumnForParameter(mockUpdateStatement, "id")
        assertNotNull(idColumn, "Should find column for id parameter")
        assertEquals("id", idColumn.src.name, "id should map to id column")
        assertEquals("INTEGER", idColumn.src.dataType, "id column should be INTEGER")
        assertEquals(true, idColumn.src.notNull, "id column should be NOT NULL")
    }

    @Test
    @DisplayName("Test UPDATE statement parameter nullability detection")
    fun testUpdateParameterNullabilityDetection() {
        // Create mock CREATE TABLE statements
        val createTableStatements = listOf(
            AnnotatedCreateTableStatement(
                name = "Person",
                src = CreateTableStatement(
                    sql = "",
                    tableName = "person",
                    columns = listOf(
                        CreateTableStatement.Column(
                            name = "age",
                            dataType = "INTEGER",
                            notNull = false,  // NULL allowed
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        CreateTableStatement.Column(
                            name = "score",
                            dataType = "INTEGER",
                            notNull = true,  // NOT NULL
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
                    excludeOverrideFields = null,
                    collectionKey = null
                ),
                columns = listOf(
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
                    ),
                    AnnotatedCreateTableStatement.Column(
                        src = CreateTableStatement.Column(
                            name = "score",
                            dataType = "INTEGER",
                            notNull = true,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = emptyMap()
                    )
                )
            )
        )

        // Create ColumnLookup
        val columnLookup = ColumnLookup(createTableStatements, createViewStatements = emptyList())

        // Create a mock UPDATE statement
        val mockUpdateStatement = AnnotatedExecuteStatement(
            name = "updateAgeAndScore",
            src = UpdateStatement(
                sql = "UPDATE person SET age = ?, score = ?",
                table = "person",
                namedParameters = listOf("myAge", "myScore"),
                namedParametersToColumns = emptyMap(),
                namedParametersToColumnNames = mapOf(
                    "myAge" to "age",
                    "myScore" to "score"
                ),
                withSelectStatements = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            )
        )

        // Test nullability detection
        val isMyAgeNullable = columnLookup.isParameterNullable(mockUpdateStatement, "myAge")
        assertEquals(true, isMyAgeNullable, "myAge should be nullable (age column allows NULL)")

        val isMyScoreNullable = columnLookup.isParameterNullable(mockUpdateStatement, "myScore")
        assertEquals(false, isMyScoreNullable, "myScore should not be nullable (score column is NOT NULL)")
    }
}
