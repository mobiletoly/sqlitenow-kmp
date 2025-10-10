package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.FunSpec
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
        fieldCodeGenerator: SelectFieldCodeGenerator,
        constructorBuilder: FunSpec.Builder,
        onPropertyGenerated: (PropertySpec) -> Unit
    ) {
        val fields = statement.fields
        val mappingPlan = statement.mappingPlan
        val regularFieldSet = mappingPlan.regularFields.toSet()
        val dynamicFieldSet = mappingPlan.includedDynamicFields.toSet()

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
            onPropertyGenerated(property)
        }
    }
}
