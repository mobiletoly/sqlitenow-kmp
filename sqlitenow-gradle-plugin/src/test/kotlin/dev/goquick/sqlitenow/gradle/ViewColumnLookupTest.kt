package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.context.ColumnLookup
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationMerger
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.processing.extractFieldAssociatedAnnotations
import dev.goquick.sqlitenow.gradle.sqlinspect.AssociatedColumn
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ViewColumnLookupTest {

    @Test
    @DisplayName("Test VIEW parameter type resolution with propertyType annotation")
    fun testViewParameterTypeResolution() {
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
                            name = "birth_date",
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
                            name = "birth_date",
                            dataType = "TEXT",
                            notNull = false,
                            primaryKey = false,
                            autoIncrement = false,
                            unique = false
                        ),
                        annotations = mapOf(
                            AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDate",
                            AnnotationConstants.ADAPTER to "custom"
                        )
                    )
                )
            ),
            AnnotatedCreateTableStatement(
                name = "PersonAddress",
                src = CreateTableStatement(
                    sql = "",
                    tableName = "person_address",
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
                            name = "person_id",
                            dataType = "INTEGER",
                            notNull = true,
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
                            name = "person_id",
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

        // Create mock CREATE VIEW statements
        val createViewStatements = listOf(
            AnnotatedCreateViewStatement(
                name = "PersonWithAddressView",
                src = CreateViewStatement(
                    sql = "CREATE VIEW PersonWithAddressView AS SELECT p.id AS person_id, p.birth_date FROM Person p JOIN PersonAddress a ON p.id = a.person_id",
                    viewName = "PersonWithAddressView",
                    selectStatement = SelectStatement(
                        sql = "SELECT p.id AS person_id, p.birth_date FROM Person p JOIN person_address a ON p.id = a.person_id",
                        fromTable = "person",
                        joinTables = listOf("person_address"),
                        namedParameters = emptyList(),
                        namedParametersToColumns = emptyMap(),
                        offsetNamedParam = null,
                        limitNamedParam = null,
                        fields = emptyList()
                    )
                ),
                annotations = StatementAnnotationOverrides(
                    name = "PersonWithAddressEntity",
                    propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                    queryResult = null,
                    implements = null,
                    excludeOverrideFields = null,
                    collectionKey = null
                ),
                fields = emptyList(),
                dynamicFields = emptyList()
            )
        )

        // Create ColumnLookup with both tables and views
        val columnLookup = ColumnLookup(createTableStatements, createViewStatements)

        // Create a mock SELECT statement against the VIEW
        val mockSelectStatement = AnnotatedSelectStatement(
            name = "selectPersonWithAddressViewById",
            src = SelectStatement(
                sql = "SELECT * FROM PersonWithAddressView WHERE person_id = :myPersonId AND birth_date <= :myBirthDate",
                fromTable = "PersonWithAddressView",
                joinTables = emptyList(),
                namedParameters = listOf("myPersonId", "myBirthDate"),
                namedParametersToColumns = mapOf(
                    "myPersonId" to AssociatedColumn.Default("person_id"),
                    "myBirthDate" to AssociatedColumn.Default("birth_date")
                ),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Test that VIEW parameter lookup works correctly
        val myPersonIdColumn = columnLookup.findColumnForParameter(mockSelectStatement, "myPersonId")
        assertNotNull(myPersonIdColumn, "Should find column for myPersonId parameter")
        assertEquals("person_id", myPersonIdColumn.src.name, "myPersonId should map to person_id column")
        assertEquals("INTEGER", myPersonIdColumn.src.dataType, "person_id column should be INTEGER")

        val myBirthDateColumn = columnLookup.findColumnForParameter(mockSelectStatement, "myBirthDate")
        assertNotNull(myBirthDateColumn, "Should find column for myBirthDate parameter")
        assertEquals("birth_date", myBirthDateColumn.src.name, "myBirthDate should map to birth_date column")
        assertEquals("TEXT", myBirthDateColumn.src.dataType, "birth_date column should be TEXT")
        assertEquals(
            "kotlinx.datetime.LocalDate", myBirthDateColumn.annotations[AnnotationConstants.PROPERTY_TYPE],
            "birth_date column should have LocalDate propertyType annotation"
        )
    }

    @Test
    @DisplayName("Test that VIEW field annotations are properly parsed")
    fun testViewFieldAnnotationsParsing() {
        // Test SQL with field annotations
        val viewSql = """
            -- @@{name=PersonWithAddressEntity}
            CREATE VIEW PersonWithAddressView AS
            SELECT
                p.id AS person_id,
                /* @@{ field=birth_date, propertyName=myBirthDateAAA, propertyType=kotlinx.datetime.LocalDateTime, adapter=custom } */
                p.birth_date,
                p.first_name
            FROM Person p
        """.trimIndent()

        // Parse the VIEW statement with annotations
        val topComments = listOf("-- @@{name=PersonWithAddressEntity}")
        val innerComments =
            listOf("/* @@{ field=birth_date, propertyName=myBirthDateAAA, propertyType=kotlinx.datetime.LocalDateTime, adapter=custom } */")

        val createViewStatement = CreateViewStatement(
            sql = viewSql,
            viewName = "PersonWithAddressView",
            selectStatement = SelectStatement(
                sql = "SELECT p.id AS person_id, p.birth_date, p.first_name FROM Person p",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = listOf(
                    SelectStatement.FieldSource(
                        fieldName = "person_id",
                        tableName = "person",
                        originalColumnName = "id",
                        dataType = "INTEGER"
                    ),
                    SelectStatement.FieldSource(
                        fieldName = "birth_date",
                        tableName = "person",
                        originalColumnName = "birth_date",
                        dataType = "TEXT"
                    ),
                    SelectStatement.FieldSource(
                        fieldName = "first_name",
                        tableName = "person",
                        originalColumnName = "first_name",
                        dataType = "TEXT"
                    )
                )
            )
        )

        val annotatedView = AnnotatedCreateViewStatement.parse(
            name = "PersonWithAddressView",
            createViewStatement = createViewStatement,
            topComments = topComments,
            innerComments = innerComments
        )

        // Verify that field annotations are parsed correctly
        assertEquals(3, annotatedView.fields.size, "Should have 3 fields")

        val birthDateField = annotatedView.fields.find { it.src.fieldName == "birth_date" }
        assertNotNull(birthDateField, "Should find birth_date field")

        assertEquals("myBirthDateAAA", birthDateField!!.annotations.propertyName, "Should have custom property name")
        assertEquals(
            "kotlinx.datetime.LocalDateTime",
            birthDateField.annotations.propertyType,
            "Should have custom property type"
        )
        assertTrue(birthDateField.annotations.adapter == true, "Should have adapter annotation")
    }

    @Test
    @DisplayName("Test that ColumnLookup returns original table columns without merging VIEW annotations")
    fun testColumnLookupReturnsOriginalColumns() {
        // Create table with basic column
        val personTable = AnnotatedCreateTableStatement(
            name = "Person",
            src = CreateTableStatement(
                sql = "CREATE TABLE person (id INTEGER, birth_date TEXT)",
                tableName = "person",
                columns = listOf(
                    CreateTableStatement.Column(
                        name = "birth_date",
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
                    annotations = mapOf("tableAnnotation" to "tableValue") // Original table annotations
                )
            )
        )

        // Create VIEW with field annotations
        val viewWithAnnotations = AnnotatedCreateViewStatement(
            name = "PersonWithAddressView",
            src = CreateViewStatement(
                sql = "CREATE VIEW PersonWithAddressView AS SELECT p.birth_date FROM Person p",
                viewName = "PersonWithAddressView",
                selectStatement = SelectStatement(
                    sql = "SELECT p.birth_date FROM Person p",
                    fromTable = "person",
                    joinTables = emptyList(),
                    namedParameters = emptyList(),
                    namedParametersToColumns = emptyMap(),
                    offsetNamedParam = null,
                    limitNamedParam = null,
                    fields = listOf(
                        SelectStatement.FieldSource(
                            fieldName = "birth_date",
                            tableName = "person",
                            originalColumnName = "birth_date",
                            dataType = "TEXT"
                        )
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
            fields = listOf(
                AnnotatedCreateViewStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "birth_date",
                        tableName = "person",
                        originalColumnName = "birth_date",
                        dataType = "TEXT"
                    ),
                    annotations = FieldAnnotationOverrides(
                        propertyName = "myCustomBirthDate",
                        propertyType = "kotlinx.datetime.LocalDateTime",
                        adapter = true,
                        notNull = null
                    )
                )
            ),
            dynamicFields = emptyList()
        )

        // Create SELECT statement that queries the VIEW
        val selectStatement = AnnotatedSelectStatement(
            name = "SelectFromView",
            src = SelectStatement(
                sql = "SELECT * FROM PersonWithAddressView WHERE birth_date = :birthDateParam",
                fromTable = "PersonWithAddressView",
                joinTables = emptyList(),
                namedParameters = listOf("birthDateParam"),
                namedParametersToColumns = mapOf("birthDateParam" to AssociatedColumn.Default("birth_date")),
                offsetNamedParam = null,
                limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                implements = null,
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Create ColumnLookup
        val columnLookup = ColumnLookup(listOf(personTable), listOf(viewWithAnnotations))

        // Test that ColumnLookup returns the original table column without merging VIEW annotations
        val column = columnLookup.findColumnForParameter(selectStatement, "birthDateParam")
        assertNotNull(column, "Should find column for birthDateParam")

        // Verify that only original table annotations are present (no VIEW annotations merged)
        assertEquals("tableValue", column!!.annotations["tableAnnotation"], "Should have original table annotation")
        assertEquals(
            null,
            column.annotations[AnnotationConstants.PROPERTY_NAME],
            "Should NOT have VIEW's custom property name"
        )
        assertEquals(
            null,
            column.annotations[AnnotationConstants.PROPERTY_TYPE],
            "Should NOT have VIEW's custom property type"
        )
        assertEquals(
            false,
            column.annotations.containsKey(AnnotationConstants.ADAPTER),
            "Should NOT have VIEW's adapter annotation"
        )

        // Verify that the underlying column info is preserved
        assertEquals("birth_date", column.src.name, "Should preserve original column name")
        assertEquals("TEXT", column.src.dataType, "Should preserve original data type")
    }

    @Test
    @DisplayName("Test that AnnotatedSelectStatement merges VIEW field annotations")
    fun testAnnotatedSelectStatementViewFieldMerging() {
        // Create a VIEW with field annotations
        val viewWithAnnotations = AnnotatedCreateViewStatement(
            name = "PersonWithAddressView",
            src = CreateViewStatement(
                sql = "CREATE VIEW PersonWithAddressView AS SELECT p.birth_date FROM Person p",
                viewName = "PersonWithAddressView",
                selectStatement = SelectStatement(
                    sql = "SELECT p.birth_date FROM Person p",
                    fromTable = "person",
                    joinTables = emptyList(),
                    namedParameters = emptyList(),
                    namedParametersToColumns = emptyMap(),
                    offsetNamedParam = null,
                    limitNamedParam = null,
                    fields = listOf(
                        SelectStatement.FieldSource(
                            fieldName = "birth_date",
                            tableName = "person",
                            originalColumnName = "birth_date",
                            dataType = "TEXT"
                        )
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
            fields = listOf(
                AnnotatedCreateViewStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "birth_date",
                        tableName = "person",
                        originalColumnName = "birth_date",
                        dataType = "TEXT"
                    ),
                    annotations = FieldAnnotationOverrides(
                        propertyName = "myCustomBirthDate",
                        propertyType = "kotlinx.datetime.LocalDateTime",
                        adapter = true,
                        notNull = null
                    )
                )
            ),
            dynamicFields = emptyList()
        )

        // Create a SELECT statement that queries the VIEW
        val selectStatement = SelectStatement(
            sql = "SELECT * FROM PersonWithAddressView",
            fromTable = "PersonWithAddressView",
            joinTables = emptyList(),
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "birth_date",
                    tableName = "PersonWithAddressView",
                    originalColumnName = "birth_date",
                    dataType = "TEXT"
                )
            )
        )

        // Create annotation resolver with the VIEW
        val annotationResolver = FieldAnnotationResolver(emptyList(), listOf(viewWithAnnotations))

        // Build fields with merged annotations manually for the test
        val fieldAnnotations = extractFieldAssociatedAnnotations(emptyList())
        val fields = selectStatement.fields.map { column ->
            val mergedAnnotations = mutableMapOf<String, Any?>()

            // Get resolved annotations from the VIEW
            val resolvedAnnotations = annotationResolver.getFieldAnnotations("PersonWithAddressView", column.fieldName)
            if (resolvedAnnotations != null) {
                FieldAnnotationMerger.mergeFieldAnnotations(mergedAnnotations, resolvedAnnotations)
            }

            AnnotatedSelectStatement.Field(
                src = column,
                annotations = FieldAnnotationOverrides.parse(mergedAnnotations)
            )
        }

        // Create the SELECT statement with VIEW annotations
        val annotatedSelectStatement = AnnotatedSelectStatement(
            name = "SelectFromView",
            src = selectStatement,
            annotations = StatementAnnotationOverrides.parse(extractAnnotations(emptyList())),
            fields = fields
        )

        // Verify that VIEW field annotations are merged into the SELECT statement fields
        assertEquals(1, annotatedSelectStatement.fields.size, "Should have 1 field")

        val birthDateField = annotatedSelectStatement.fields.find { it.src.fieldName == "birth_date" }
        assertNotNull(birthDateField, "Should find birth_date field")

        // Verify that VIEW annotations are merged
        assertEquals(
            "myCustomBirthDate",
            birthDateField!!.annotations.propertyName,
            "Should have VIEW's custom property name"
        )
        assertEquals(
            "kotlinx.datetime.LocalDateTime",
            birthDateField.annotations.propertyType,
            "Should have VIEW's custom property type"
        )
        assertTrue(birthDateField.annotations.adapter == true, "Should have VIEW's adapter annotation")
    }

    @Test
    @DisplayName("Test that SelectFieldCodeGenerator uses VIEW field annotations for property generation")
    fun testSelectFieldCodeGeneratorWithViewAnnotations() {
        // Create a SELECT statement field that queries a VIEW with custom annotations
        val selectField = AnnotatedSelectStatement.Field(
            src = SelectStatement.FieldSource(
                fieldName = "birth_date",
                tableName = "PersonWithAddressView",
                originalColumnName = "birth_date",
                dataType = "TEXT"
            ),
            annotations = FieldAnnotationOverrides(
                propertyName = "myCustomBirthDate",
                propertyType = "kotlinx.datetime.LocalDateTime",
                adapter = true,
                notNull = null
            )
        )

        // Create a SelectFieldCodeGenerator
        val generator = SelectFieldCodeGenerator()

        // Generate property using the field with VIEW annotations
        val property = generator.generateProperty(selectField, PropertyNameGeneratorType.LOWER_CAMEL_CASE)

        // Verify that the property uses the custom property name from VIEW annotations
        assertEquals("myCustomBirthDate", property.name, "Should use custom property name from VIEW annotations")

        // Verify that the property type is determined by the custom propertyType annotation
        assertEquals(
            "kotlinx.datetime.LocalDateTime?",
            property.type.toString(),
            "Should use custom property type from VIEW annotations"
        )
    }

    @Test
    @DisplayName("Test that SELECT statement annotations take precedence over VIEW annotations")
    fun testSelectStatementAnnotationsPrecedence() {
        // Create a VIEW with field annotations
        val viewWithAnnotations = AnnotatedCreateViewStatement(
            name = "PersonWithAddressView",
            src = CreateViewStatement(
                sql = "CREATE VIEW PersonWithAddressView AS SELECT p.birth_date FROM Person p",
                viewName = "PersonWithAddressView",
                selectStatement = SelectStatement(
                    sql = "SELECT p.birth_date FROM Person p",
                    fromTable = "person",
                    joinTables = emptyList(),
                    namedParameters = emptyList(),
                    namedParametersToColumns = emptyMap(),
                    offsetNamedParam = null,
                    limitNamedParam = null,
                    fields = listOf(
                        SelectStatement.FieldSource(
                            fieldName = "birth_date",
                            tableName = "person",
                            originalColumnName = "birth_date",
                            dataType = "TEXT"
                        )
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
            fields = listOf(
                AnnotatedCreateViewStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "birth_date",
                        tableName = "person",
                        originalColumnName = "birth_date",
                        dataType = "TEXT"
                    ),
                    annotations = FieldAnnotationOverrides(
                        propertyName = "viewPropertyName",
                        propertyType = "kotlinx.datetime.LocalDate",
                        adapter = true,
                        notNull = null
                    )
                )
            ),
            dynamicFields = emptyList()
        )

        // Create a SELECT statement that queries the VIEW with its own field annotations
        val selectStatement = SelectStatement(
            sql = "SELECT birth_date FROM PersonWithAddressView",
            fromTable = "PersonWithAddressView",
            joinTables = emptyList(),
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "birth_date",
                    tableName = "PersonWithAddressView",
                    originalColumnName = "birth_date",
                    dataType = "TEXT"
                )
            )
        )

        // Create annotation resolver with the VIEW
        val annotationResolver = FieldAnnotationResolver(emptyList(), listOf(viewWithAnnotations))

        // Build fields with merged annotations manually for the test
        val innerComments =
            listOf("-- @@{field=birth_date, propertyName=selectPropertyName, propertyType=kotlinx.datetime.LocalDateTime}")
        val fieldAnnotations = extractFieldAssociatedAnnotations(innerComments)
        val fields = selectStatement.fields.map { column ->
            val mergedAnnotations = mutableMapOf<String, Any?>()

            // Get resolved annotations from the VIEW (as base)
            val resolvedAnnotations = annotationResolver.getFieldAnnotations("PersonWithAddressView", column.fieldName)
            if (resolvedAnnotations != null) {
                FieldAnnotationMerger.mergeFieldAnnotations(mergedAnnotations, resolvedAnnotations)
            }

            // Override with SELECT statement annotations (they take precedence)
            val selectAnnotations = fieldAnnotations[column.fieldName] ?: emptyMap()
            mergedAnnotations.putAll(selectAnnotations)

            AnnotatedSelectStatement.Field(
                src = column,
                annotations = FieldAnnotationOverrides.parse(mergedAnnotations)
            )
        }

        // Create the SELECT statement with both VIEW and SELECT annotations
        val annotatedSelectStatement = AnnotatedSelectStatement(
            name = "SelectFromView",
            src = selectStatement,
            annotations = StatementAnnotationOverrides.parse(extractAnnotations(emptyList())),
            fields = fields
        )

        // Verify that SELECT statement annotations take precedence over VIEW annotations
        assertEquals(1, annotatedSelectStatement.fields.size, "Should have 1 field")

        val birthDateField = annotatedSelectStatement.fields.find { it.src.fieldName == "birth_date" }
        assertNotNull(birthDateField, "Should find birth_date field")

        // SELECT statement annotations should override VIEW annotations
        assertEquals(
            "selectPropertyName",
            birthDateField!!.annotations.propertyName,
            "SELECT statement propertyName should override VIEW propertyName"
        )
        assertEquals(
            "kotlinx.datetime.LocalDateTime",
            birthDateField.annotations.propertyType,
            "SELECT statement propertyType should override VIEW propertyType"
        )
        assertTrue(
            birthDateField.annotations.adapter == true,
            "Should inherit adapter annotation from VIEW (not overridden)"
        )
    }
}
