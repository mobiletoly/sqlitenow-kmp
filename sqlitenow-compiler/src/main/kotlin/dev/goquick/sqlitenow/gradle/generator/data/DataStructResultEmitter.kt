/*
 * Copyright 2025 Toly Pochkin
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
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType

internal class DataStructResultEmitter(
    private val generatorContext: GeneratorContext,
    private val propertyEmitter: DataStructPropertyEmitter,
) {
    fun generateSelectResult(
        statement: AnnotatedSelectStatement,
        className: String,
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(className)
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query results.")

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = generatorContext.selectFieldGenerator
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

        val collectedProps = mutableListOf<PropertySpec>()
        propertyEmitter.emitPropertiesWithInterfaceSupport(
            statement = statement,
            propertyNameGenerator = propertyNameGeneratorType,
            fieldCodeGenerator = fieldCodeGenerator,
            constructorBuilder = constructorBuilder
        ) { prop ->
            collectedProps.add(prop)
            dataClassBuilder.addProperty(prop)
        }

        dataClassBuilder.primaryConstructor(constructorBuilder.build())
        dataClassBuilder.addType(
            TypeSpec.companionObjectBuilder()
                .addModifiers(KModifier.PUBLIC)
                .build()
        )
        DataStructUtils.addArraySafeEqualsAndHashCodeIfNeeded(
            classBuilder = dataClassBuilder,
            className = className,
            properties = collectedProps
        )
        return dataClassBuilder.build()
    }

    fun generateExecuteResult(
        statement: AnnotatedExecuteStatement,
        className: String,
        columnsToInclude: List<AnnotatedCreateTableStatement.Column>
    ): TypeSpec {
        val properties = columnsToInclude.map { column ->
            val baseType =
                SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(column.src.dataType)
            val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
            val isNullable = column.isNullable()
            val kotlinType = SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
                baseType,
                propertyType,
                isNullable,
                generatorContext.packageName
            )
            val propertyName =
                PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(column.src.name)
            PropertySpec.builder(propertyName, kotlinType)
                .initializer(propertyName)
                .build()
        }

        val constructorParams = properties.map { prop ->
            ParameterSpec.builder(prop.name, prop.type).build()
        }

        return TypeSpec.classBuilder(className)
            .addModifiers(KModifier.DATA)
            .primaryConstructor(
                FunSpec.constructorBuilder()
                    .addParameters(constructorParams)
                    .build()
            )
            .addProperties(properties)
            .addKdoc("Data class for ${statement.name} query results.")
            .addType(
                TypeSpec.companionObjectBuilder().build()
            )
            .build()
    }
}
