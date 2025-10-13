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
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType

/**
 * Emits joined-row data classes used when dynamic field mapping is present.
 */
internal class DataStructJoinedEmitter(
    private val generatorContext: GeneratorContext,
) {
    fun generateJoinedDataClass(
        joinedClassName: String,
        fields: List<AnnotatedSelectStatement.Field>,
        propertyNameGenerator: PropertyNameGeneratorType,
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(joinedClassName)
            .addModifiers(KModifier.DATA)
            .addKdoc("Joined row data containing all fields from the SQL query without any dynamic field mapping")

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = generatorContext.selectFieldGenerator
        val joinedNameMap = JoinedPropertyNameResolver.computeNameMap(
            fields = fields,
            propertyNameGenerator = propertyNameGenerator,
            selectFieldGenerator = fieldCodeGenerator
        )

        val collectedProps = mutableListOf<PropertySpec>()
        fields.forEach { field ->
            if (!field.annotations.isDynamicField) {
                val key = JoinedPropertyNameResolver.JoinedFieldKey(field.src.tableName, field.src.fieldName)
                val uniqueName = joinedNameMap[key]
                    ?: fieldCodeGenerator.generateProperty(field, propertyNameGenerator).name

                val generatedProperty = fieldCodeGenerator.generateProperty(field, propertyNameGenerator)
                val adjustedType = adjustTypeForJoinNullability(generatedProperty.type, field, fields)

                val property = PropertySpec.builder(uniqueName, adjustedType)
                    .initializer(uniqueName)
                    .build()

                collectedProps.add(property)
                dataClassBuilder.addProperty(property)
                constructorBuilder.addParameter(uniqueName, adjustedType)
            }
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        dataClassBuilder.addType(
            TypeSpec.companionObjectBuilder()
                .addModifiers(KModifier.PUBLIC)
                .build()
        )
        DataStructUtils.addArraySafeEqualsAndHashCodeIfNeeded(
            classBuilder = dataClassBuilder,
            className = joinedClassName,
            properties = collectedProps
        )
        return dataClassBuilder.build()
    }

    private fun adjustTypeForJoinNullability(
        originalType: TypeName,
        field: AnnotatedSelectStatement.Field,
        allFields: List<AnnotatedSelectStatement.Field>
    ): TypeName {
        val explicitNotNull = field.annotations.notNull == true
        val explicitNullable = field.annotations.notNull == false
        if (explicitNotNull) {
            return originalType.copy(nullable = false)
        }
        if (explicitNullable) {
            return originalType.copy(nullable = true)
        }

        val fieldTableAlias = field.src.tableName
        val mainTableAlias = generatorContext.findMainTableAlias(allFields)

        if (fieldTableAlias.isNotBlank()) {
            if (fieldTableAlias != mainTableAlias) {
                return originalType.copy(nullable = true)
            }
            return originalType
        }

        val aliasPrefixes = allFields
            .filter { it.annotations.isDynamicField }
            .mapNotNull { it.annotations.aliasPrefix }
            .filter { it.isNotBlank() }
        val visibleName = field.src.fieldName
        if (aliasPrefixes.any { prefix -> visibleName.startsWith(prefix) }) {
            return originalType.copy(nullable = true)
        }
        return originalType
    }
}
