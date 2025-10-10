package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.ClassName
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
        excludeOverrideFields: Set<String>?,
    ): TypeSpec {
        val dataClassBuilder = TypeSpec.classBuilder(className)
            .addModifiers(KModifier.DATA)
            .addKdoc("Data class for ${statement.name} query results.")

        statement.annotations.implements?.let { implement ->
            val interfaceType = if (implement.contains('.')) {
                ClassName.bestGuess(implement)
            } else {
                ClassName("", implement)
            }
            dataClassBuilder.addSuperinterface(interfaceType)
        }

        val constructorBuilder = FunSpec.constructorBuilder()
        val fieldCodeGenerator = generatorContext.selectFieldGenerator
        val propertyNameGeneratorType = statement.annotations.propertyNameGenerator

        val collectedProps = mutableListOf<PropertySpec>()
        propertyEmitter.emitPropertiesWithInterfaceSupport(
            statement = statement,
            propertyNameGenerator = propertyNameGeneratorType,
            implementsInterface = statement.annotations.implements,
            excludeOverrideFields = excludeOverrideFields,
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
