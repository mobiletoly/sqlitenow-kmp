package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.PropertySpec
import dev.goquick.sqlitenow.gradle.generator.data.DataStructPropertyEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExcludeOverrideFieldsPatternTest {

    private fun regularField(
        label: String,
        originalColumn: String = label,
        tableAlias: String = "",
        dataType: String = "TEXT",
        propertyNameOverride: String? = null,
    ): AnnotatedSelectStatement.Field {
        val source = SelectStatement.FieldSource(
            fieldName = label,
            tableName = tableAlias,
            originalColumnName = originalColumn,
            dataType = dataType
        )
        val overrides = FieldAnnotationOverrides(
            propertyName = propertyNameOverride,
            propertyType = null,
            notNull = null,
            adapter = false,
            isDynamicField = false,
            defaultValue = null,
            aliasPrefix = null,
            mappingType = null,
            sourceTable = null,
            collectionKey = null,
            suppressProperty = false,
        )
        return AnnotatedSelectStatement.Field(src = source, annotations = overrides)
    }

    private fun buildStatement(fields: List<AnnotatedSelectStatement.Field>): AnnotatedSelectStatement {
        val selectStatement = SelectStatement(
            sql = "SELECT stub",
            fromTable = null,
            joinTables = emptyList(),
            fields = fields.filter { !it.annotations.isDynamicField }.map { it.src },
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            parameterCastTypes = emptyMap(),
            tableAliases = emptyMap(),
            joinConditions = emptyList()
        )
        val annotations = StatementAnnotationOverrides(
            name = null,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            queryResult = null,
            implements = "MyInterface",
            excludeOverrideFields = null,
            collectionKey = null
        )
        return AnnotatedSelectStatement(
            name = "TestStatement",
            src = selectStatement,
            annotations = annotations,
            fields = fields
        )
    }

    private fun emitProperties(
        statement: AnnotatedSelectStatement,
        excludePatterns: Set<String>,
    ): List<PropertySpec> {
        val props = mutableListOf<PropertySpec>()
        val adjustedStatement = statement.copy(
            annotations = statement.annotations.copy(excludeOverrideFields = excludePatterns)
        )
        DataStructPropertyEmitter().emitPropertiesWithInterfaceSupport(
            statement = adjustedStatement,
            propertyNameGenerator = adjustedStatement.annotations.propertyNameGenerator,
            implementsInterface = adjustedStatement.annotations.implements,
            excludeOverrideFields = excludePatterns,
            fieldCodeGenerator = SelectFieldCodeGenerator(),
            constructorBuilder = FunSpec.constructorBuilder(),
        ) { prop -> props += prop }
        return props
    }

    @Test
    fun wildcardMatchesAliasAndCamelCase() {
        val statement = buildStatement(
            listOf(
                regularField(label = "schedule__activity_id", originalColumn = "activity_id"),
                regularField(label = "email")
            )
        )

        val properties = emitProperties(statement, excludePatterns = setOf("schedule__*"))
        val scheduleProp = properties.first { it.name == "scheduleActivityId" }
        val emailProp = properties.first { it.name == "email" }

        assertFalse(scheduleProp.modifiers.contains(KModifier.OVERRIDE))
        assertTrue(emailProp.modifiers.contains(KModifier.OVERRIDE))
    }

    @Test
    fun wildcardMatchesGeneratedPropertyName() {
        val statement = buildStatement(
            listOf(
                regularField(
                    label = "activity_id",
                    originalColumn = "activity_id",
                    propertyNameOverride = "joinedScheduleActivityId"
                )
            )
        )
        val properties = emitProperties(statement, excludePatterns = setOf("joinedSchedule*"))
        val prop = properties.first { it.name == "joinedScheduleActivityId" }
        assertFalse(prop.modifiers.contains(KModifier.OVERRIDE))
    }

    @Test
    fun exactNameExcludes() {
        val statement = buildStatement(listOf(regularField(label = "id")))
        val properties = emitProperties(statement, excludePatterns = setOf("id"))
        val prop = properties.first { it.name == "id" }
        assertFalse(prop.modifiers.contains(KModifier.OVERRIDE))
    }
}
