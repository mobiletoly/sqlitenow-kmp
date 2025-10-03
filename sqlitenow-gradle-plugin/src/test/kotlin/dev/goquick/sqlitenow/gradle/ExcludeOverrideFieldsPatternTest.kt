package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.KModifier
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.generator.data.DataStructPropertyEmitter
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.sql.Connection
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ExcludeOverrideFieldsPatternTest {

    private fun makeField(
        aliasOrLabel: String,
        originalColumn: String = aliasOrLabel,
        tableAlias: String = "",
        dataType: String = "TEXT",
        propertyNameOverride: String? = null,
    ): AnnotatedSelectStatement.Field {
        val src = Mockito.mock(SelectStatement.FieldSource::class.java)
        Mockito.`when`(src.fieldName).thenReturn(aliasOrLabel)
        Mockito.`when`(src.originalColumnName).thenReturn(originalColumn)
        Mockito.`when`(src.tableName).thenReturn(tableAlias)
        Mockito.`when`(src.dataType).thenReturn(dataType)

        val ann = FieldAnnotationOverrides(
            propertyName = propertyNameOverride,
            propertyType = null,
            notNull = null,
            adapter = false
        )
        return AnnotatedSelectStatement.Field(src = src, annotations = ann)
    }

    private fun newGenerator(): DataStructCodeGenerator {
        val conn = Mockito.mock(Connection::class.java)
        val tempDir = createTempDir(prefix = "sqlitenow-test-")
        tempDir.deleteOnExit()
        return DataStructCodeGenerator(
            conn = conn,
            queriesDir = tempDir,
            packageName = "dev.test",
            outputDir = tempDir,
            statementExecutors = mutableListOf(),
            providedCreateTableStatements = emptyList()
        )
    }

    @Test
    fun wildcardMatchesAliasAndCamelCase() {
        // Sanity check: our pattern should exclude alias names like schedule__activity_id
        run {
            val re = Regex("^schedule__.*$")
            assertTrue(re.matches("schedule__activity_id"))
        }
        val gen = newGenerator()
        val fieldA = makeField(aliasOrLabel = "schedule__activity_id", originalColumn = "activity_id")
        val fieldB = makeField(aliasOrLabel = "email")

        val props = mutableListOf<com.squareup.kotlinpoet.PropertySpec>()
        val ctor = com.squareup.kotlinpoet.FunSpec.constructorBuilder()
        val fieldCodeGen = SelectFieldCodeGenerator()
        val emitter = DataStructPropertyEmitter(gen.generatorContext)

        val fields = listOf(fieldA, fieldB)
        val skipSet = DynamicFieldUtils.computeSkipSet(fields)

        emitter.emitPropertiesWithInterfaceSupport(
            fields = fields,
            mappedColumns = emptySet(),
            dynamicFieldSkipSet = skipSet,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = "MyInterface",
            excludeOverrideFields = setOf("schedule__*"),
            fieldCodeGenerator = fieldCodeGen,
            constructorBuilder = ctor
        ) { p -> props += p }

        // The property name for "schedule__activity_id" should be generated based on the field name
        val propA = props.firstOrNull { it.name.contains("schedule") || it.name.contains("activity") }
            ?: props.firstOrNull { it.name == "scheduleActivityId" }
            ?: throw NoSuchElementException("Could not find property for schedule__activity_id. Available: ${props.map { it.name }}")
        val propB = props.first { it.name == "email" }

        // A is excluded by pattern, so no OVERRIDE
        assertFalse(propA.modifiers.contains(KModifier.OVERRIDE))
        // B is not excluded, so OVERRIDE is present
        assertTrue(propB.modifiers.contains(KModifier.OVERRIDE))
    }

    @Test
    fun wildcardMatchesGeneratedPropertyName() {
        val gen = newGenerator()
        val field = makeField(aliasOrLabel = "activity_id", originalColumn = "activity_id", propertyNameOverride = "joinedScheduleActivityId")

        val props = mutableListOf<com.squareup.kotlinpoet.PropertySpec>()
        val ctor = com.squareup.kotlinpoet.FunSpec.constructorBuilder()
        val fieldCodeGen = SelectFieldCodeGenerator()
        val emitter = DataStructPropertyEmitter(gen.generatorContext)

        val fields = listOf(field)
        val skipSet = DynamicFieldUtils.computeSkipSet(fields)

        emitter.emitPropertiesWithInterfaceSupport(
            fields = fields,
            mappedColumns = emptySet(),
            dynamicFieldSkipSet = skipSet,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = "MyInterface",
            excludeOverrideFields = setOf("joinedSchedule*"),
            fieldCodeGenerator = fieldCodeGen,
            constructorBuilder = ctor
        ) { p -> props += p }

        val prop = props.first { it.name == "joinedScheduleActivityId" }
        assertFalse(prop.modifiers.contains(KModifier.OVERRIDE))
    }

    @Test
    fun exactNameExcludes() {
        val gen = newGenerator()
        val field = makeField(aliasOrLabel = "id")

        val props = mutableListOf<com.squareup.kotlinpoet.PropertySpec>()
        val ctor = com.squareup.kotlinpoet.FunSpec.constructorBuilder()
        val fieldCodeGen = SelectFieldCodeGenerator()
        val emitter = DataStructPropertyEmitter(gen.generatorContext)

        val fields = listOf(field)
        val skipSet = DynamicFieldUtils.computeSkipSet(fields)

        emitter.emitPropertiesWithInterfaceSupport(
            fields = fields,
            mappedColumns = emptySet(),
            dynamicFieldSkipSet = skipSet,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = "MyInterface",
            excludeOverrideFields = setOf("id"),
            fieldCodeGenerator = fieldCodeGen,
            constructorBuilder = ctor
        ) { p -> props += p }

        val prop = props.first { it.name == "id" }
        assertFalse(prop.modifiers.contains(KModifier.OVERRIDE))
    }
}
