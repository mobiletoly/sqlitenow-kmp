package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.PropertySpec
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SelectFieldCodeGeneratorTest {

    private fun assertGeneratedProperty(
        fieldName: String,
        dataType: String,
        expectedPropertyName: String,
        expectedType: String,
        expectedNullable: Boolean,
        propertyName: String? = null,
        propertyType: String? = null,
        notNull: Boolean? = null,
        originalColumnName: String = fieldName,
    ) {
        val property = generatedProperty(
            fieldName = fieldName,
            originalColumnName = originalColumnName,
            propertyName = propertyName,
            propertyType = propertyType,
            notNull = notNull,
            dataType = dataType
        )

        assertProperty(property, expectedPropertyName, expectedType, expectedNullable)
    }

    private fun generatedProperty(
        fieldName: String,
        dataType: String,
        tableName: String = "",
        originalColumnName: String = fieldName,
        propertyName: String? = null,
        propertyType: String? = null,
        notNull: Boolean? = null,
        schemaTables: List<AnnotatedCreateTableStatement> = emptyList(),
    ) = SelectFieldCodeGenerator(schemaTables).generateProperty(
        AnnotatedSelectStatement.Field(
            src = SelectStatement.FieldSource(
                fieldName = fieldName,
                tableName = tableName,
                originalColumnName = originalColumnName,
                dataType = dataType
            ),
            annotations = FieldAnnotationOverrides(
                propertyName = propertyName,
                propertyType = propertyType,
                notNull = notNull,
                adapter = false
            )
        )
    )

    private fun assertProperty(property: PropertySpec, name: String, type: String, nullable: Boolean) {
        assertEquals(name, property.name)
        assertEquals(type, property.type.toString())
        assertEquals(nullable, property.type.isNullable)
    }

    private fun annotatedTable(
        name: String,
        sourceTableName: String = name,
        columns: List<SchemaColumn>,
    ): AnnotatedCreateTableStatement {
        val sourceColumns = columns.map { column ->
            mock(CreateTableStatement.Column::class.java).also {
                `when`(it.name).thenReturn(column.name)
                `when`(it.dataType).thenReturn(column.dataType)
                `when`(it.notNull).thenReturn(column.notNull)
                `when`(it.primaryKey).thenReturn(column.primaryKey)
                `when`(it.autoIncrement).thenReturn(column.autoIncrement)
                `when`(it.unique).thenReturn(column.unique)
            }
        }
        val createTable = mock(CreateTableStatement::class.java).also {
            `when`(it.tableName).thenReturn(sourceTableName)
            `when`(it.columns).thenReturn(sourceColumns)
        }

        return AnnotatedCreateTableStatement(
            name = name,
            src = createTable,
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null
            ),
            columns = sourceColumns.zip(columns).map { (sourceColumn, column) ->
                AnnotatedCreateTableStatement.Column(
                    src = sourceColumn,
                    annotations = column.annotations
                )
            }
        )
    }

    @Test
    @DisplayName("Test generating property for a field with SQLite INTEGER type")
    fun testGeneratePropertyForIntegerField() {
        assertGeneratedProperty(
            fieldName = "user_id",
            dataType = "INTEGER",
            expectedPropertyName = "userId",
            expectedType = "kotlin.Long?",
            expectedNullable = true
        )
    }

    @Test
    @DisplayName("Test generating property for a field with SQLite TEXT type")
    fun testGeneratePropertyForTextField() {
        assertGeneratedProperty(
            fieldName = "username",
            dataType = "TEXT",
            expectedPropertyName = "username",
            expectedType = "kotlin.String?",
            expectedNullable = true
        )
    }

    @Test
    @DisplayName("Test generating property for a field with non-null annotation")
    fun testGeneratePropertyForNonNullField() {
        assertGeneratedProperty(
            fieldName = "email",
            dataType = "TEXT",
            expectedPropertyName = "email",
            expectedType = "kotlin.String",
            expectedNullable = false,
            notNull = true
        )
    }

    @TestFactory
    @DisplayName("Schema-backed property generation")
    fun schemaBackedPropertyGeneration(): List<DynamicTest> {
        return listOf(
            SchemaBackedPropertyCase(
                displayName = "uses NOT NULL constraint from schema",
                fieldName = "username",
                tableName = "users",
                dataType = "TEXT",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "users",
                            columns = listOf(SchemaColumn(name = "username", dataType = "TEXT", notNull = true))
                        )
                    )
                },
                expectedName = "username",
                expectedType = "kotlin.String",
                expectedNullable = false
            ),
            SchemaBackedPropertyCase(
                displayName = "uses joined table schema when SELECT field has no table name",
                fieldName = "address",
                dataType = "TEXT",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "users",
                            columns = listOf(SchemaColumn(name = "username", dataType = "TEXT", notNull = true))
                        ),
                        annotatedTable(
                            name = "addresses",
                            columns = listOf(SchemaColumn(name = "address", dataType = "TEXT", notNull = true))
                        )
                    )
                },
                expectedName = "address",
                expectedType = "kotlin.String",
                expectedNullable = false
            ),
            SchemaBackedPropertyCase(
                displayName = "uses original column for aliased JOIN field nullability",
                fieldName = "address_id",
                tableName = "a",
                originalColumnName = "id",
                dataType = "INTEGER",
                propertyName = "myAddressId",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "PersonAddress",
                            sourceTableName = "person_address",
                            columns = listOf(SchemaColumn(name = "id", dataType = "INTEGER", notNull = true))
                        )
                    )
                },
                expectedName = "myAddressId",
                expectedType = "kotlin.Long",
                expectedNullable = false
            ),
            SchemaBackedPropertyCase(
                displayName = "CREATE TABLE nullability annotation overrides schema constraint",
                fieldName = "id",
                tableName = "person",
                dataType = "INTEGER",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "person",
                            columns = listOf(
                                SchemaColumn(
                                    name = "id",
                                    dataType = "INTEGER",
                                    notNull = true,
                                    primaryKey = true,
                                    annotations = mapOf(AnnotationConstants.NOT_NULL to false)
                                )
                            )
                        )
                    )
                },
                expectedName = "id",
                expectedType = "kotlin.Long?",
                expectedNullable = true
            ),
            SchemaBackedPropertyCase(
                displayName = "CREATE TABLE propertyType annotation is respected",
                fieldName = "created_at",
                tableName = "person",
                dataType = "TEXT",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "person",
                            columns = listOf(
                                SchemaColumn(
                                    name = "created_at",
                                    dataType = "TEXT",
                                    annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to "java.time.LocalDateTime")
                                )
                            )
                        )
                    )
                },
                expectedName = "createdAt",
                expectedType = "java.time.LocalDateTime?",
                expectedNullable = true
            ),
            SchemaBackedPropertyCase(
                displayName = "SELECT propertyType annotation overrides CREATE TABLE propertyType",
                fieldName = "created_at",
                tableName = "person",
                dataType = "TEXT",
                propertyType = "java.time.ZonedDateTime",
                schemaTables = {
                    listOf(
                        annotatedTable(
                            name = "person",
                            columns = listOf(
                                SchemaColumn(
                                    name = "created_at",
                                    dataType = "TEXT",
                                    annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to "java.time.LocalDateTime")
                                )
                            )
                        )
                    )
                },
                expectedName = "createdAt",
                expectedType = "java.time.ZonedDateTime?",
                expectedNullable = true
            )
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val property = generatedProperty(
                    fieldName = case.fieldName,
                    tableName = case.tableName,
                    originalColumnName = case.originalColumnName,
                    dataType = case.dataType,
                    propertyName = case.propertyName,
                    propertyType = case.propertyType,
                    schemaTables = case.schemaTables()
                )

                assertProperty(property, case.expectedName, case.expectedType, case.expectedNullable)
            }
        }
    }

    @Test
    @DisplayName("Test generating property for a field with custom property name")
    fun testGeneratePropertyWithCustomName() {
        assertGeneratedProperty(
            fieldName = "user_full_name",
            dataType = "TEXT",
            expectedPropertyName = "fullName",
            expectedType = "kotlin.String?",
            expectedNullable = true,
            propertyName = "fullName"
        )
    }

    @Test
    @DisplayName("Test generating property for a field with custom property type")
    fun testGeneratePropertyWithCustomType() {
        assertGeneratedProperty(
            fieldName = "created_at",
            dataType = "TEXT",
            expectedPropertyName = "createdAt",
            expectedType = "java.time.LocalDateTime?",
            expectedNullable = true,
            propertyType = "java.time.LocalDateTime"
        )
    }

    @TestFactory
    @DisplayName("Aggregate inference maps generated property types")
    fun aggregateInferenceMapsGeneratedPropertyTypes(): List<DynamicTest> = listOf(
        AggregateInferenceCase(
            displayName = "COUNT inference returns non-null Long",
            expressionSql = "COUNT(*)",
            fieldName = "activityCount",
            dataType = "INTEGER",
            expectedType = "kotlin.Long",
            expectedNullable = false,
        ),
        AggregateInferenceCase(
            displayName = "GROUP_CONCAT inference returns nullable String",
            expressionSql = "GROUP_CONCAT(act__title, ', ')",
            fieldName = "titles",
            dataType = "TEXT",
            expectedType = "kotlin.String?",
            expectedNullable = true,
        ),
        AggregateInferenceCase(
            displayName = "SUM inference returns nullable Double",
            expressionSql = "SUM(amount)",
            fieldName = "totalAmount",
            dataType = "INTEGER",
            expectedType = "kotlin.Double?",
            expectedNullable = true,
        ),
    ).map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val field = AnnotatedSelectStatement.Field(
                src = SelectStatement.FieldSource(
                    fieldName = case.fieldName,
                    tableName = "",
                    originalColumnName = case.fieldName,
                    dataType = case.dataType,
                    expression = CCJSqlParserUtil.parseExpression(case.expressionSql),
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false,
                ),
            )

            val property = SelectFieldCodeGenerator().generateProperty(field)

            assertEquals(case.fieldName, property.name)
            assertEquals(case.expectedType, property.type.toString())
            assertEquals(case.expectedNullable, property.type.isNullable)
        }
    }

    private data class AggregateInferenceCase(
        val displayName: String,
        val expressionSql: String,
        val fieldName: String,
        val dataType: String,
        val expectedType: String,
        val expectedNullable: Boolean,
    )

    private data class SchemaBackedPropertyCase(
        val displayName: String,
        val fieldName: String,
        val dataType: String,
        val tableName: String = "",
        val originalColumnName: String = fieldName,
        val propertyName: String? = null,
        val propertyType: String? = null,
        val schemaTables: () -> List<AnnotatedCreateTableStatement>,
        val expectedName: String,
        val expectedType: String,
        val expectedNullable: Boolean,
    )

    private data class SchemaColumn(
        val name: String,
        val dataType: String,
        val notNull: Boolean = false,
        val primaryKey: Boolean = false,
        val autoIncrement: Boolean = false,
        val unique: Boolean = false,
        val annotations: Map<String, Any?> = emptyMap(),
    )
}
