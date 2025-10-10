package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.PropertySpec
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator

/**
 * Responsible for emitting properties and constructor parameters for generated data classes.
 * Encapsulates the previous DataStructCodeGenerator.generatePropertiesWithInterfaceSupport logic.
 */
internal class DataStructPropertyEmitter {
    fun emitPropertiesWithInterfaceSupport(
        statement: AnnotatedSelectStatement,
        propertyNameGenerator: PropertyNameGeneratorType,
        implementsInterface: String?,
        excludeOverrideFields: Set<String>?,
        fieldCodeGenerator: SelectFieldCodeGenerator,
        constructorBuilder: FunSpec.Builder,
        onPropertyGenerated: (PropertySpec) -> Unit
    ) {
        val fields = statement.fields
        val mappingPlan = statement.mappingPlan
        val regularFieldSet = mappingPlan.regularFields.toSet()
        val dynamicFieldSet = mappingPlan.includedDynamicFields.toSet()
        val excludeRegexes =
            excludeOverrideFields?.map { pattern -> globToRegex(pattern) } ?: emptyList()

        fields.forEach { field ->
            val include = if (field.annotations.isDynamicField) {
                dynamicFieldSet.contains(field)
            } else {
                regularFieldSet.contains(field)
            }
            if (!include) return@forEach

            val parameter = fieldCodeGenerator.generateParameter(field, propertyNameGenerator)
            constructorBuilder.addParameter(parameter)

            val property = fieldCodeGenerator.generateProperty(field, propertyNameGenerator)
            val finalProperty = if (implementsInterface != null) {
                val fieldName = property.name
                val candidates = listOf(
                    fieldName,
                    field.src.fieldName,
                    field.src.originalColumnName
                ).filter { it.isNotBlank() }
                val isExcluded = excludeRegexes.any { regex ->
                    candidates.any { candidate ->
                        regex.matches(candidate)
                    }
                }
                if (!isExcluded) {
                    property.toBuilder().addModifiers(KModifier.OVERRIDE).build()
                } else {
                    property
                }
            } else {
                property
            }

            onPropertyGenerated(finalProperty)
        }
    }

    private fun globToRegex(glob: String): Regex {
        val builder = StringBuilder()
        var escaping = false
        glob.forEach { ch ->
            when {
                escaping -> {
                    builder.append(Regex.escape(ch.toString()))
                    escaping = false
                }

                ch == '*' -> builder.append(".*")
                ch == '?' -> builder.append('.')
                ch == '\\' -> escaping = true
                else -> builder.append(Regex.escape(ch.toString()))
            }
        }
        return Regex(builder.toString())
    }
}
