package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.KModifier
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
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
        // Sanity check: our pattern should exclude alias names like joined_schedule_activity_id
        run {
            val re = Regex("^joined_schedule_.*$")
            assertTrue(re.matches("joined_schedule_activity_id"))
        }
        val gen = newGenerator()
        val fieldA = makeField(aliasOrLabel = "joined_schedule_activity_id", originalColumn = "activity_id")
        val fieldB = makeField(aliasOrLabel = "email")

        val props = mutableListOf<com.squareup.kotlinpoet.PropertySpec>()
        val ctor = com.squareup.kotlinpoet.FunSpec.constructorBuilder()
        val fieldCodeGen = SelectFieldCodeGenerator()

        gen.generatePropertiesWithInterfaceSupport(
            fields = listOf(fieldA, fieldB),
            mappedColumns = emptySet(),
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = "MyInterface",
            excludeOverrideFields = setOf("joined_schedule_*"),
            fieldCodeGenerator = fieldCodeGen,
            constructorBuilder = ctor
        ) { p -> props += p }

        val propA = props.first { it.name == "joinedScheduleActivityId" }
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

        gen.generatePropertiesWithInterfaceSupport(
            fields = listOf(field),
            mappedColumns = emptySet(),
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

        gen.generatePropertiesWithInterfaceSupport(
            fields = listOf(field),
            mappedColumns = emptySet(),
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
