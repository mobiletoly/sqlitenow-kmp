/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
