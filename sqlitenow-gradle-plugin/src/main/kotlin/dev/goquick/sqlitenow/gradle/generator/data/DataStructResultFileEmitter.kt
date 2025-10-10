package dev.goquick.sqlitenow.gradle.generator.data

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.io.File

internal class DataStructResultFileEmitter(
    private val generatorContext: GeneratorContext,
    private val joinedEmitter: DataStructJoinedEmitter,
    private val resultEmitter: DataStructResultEmitter,
    private val outputDir: File,
) {
    fun writeSelectResultFile(
        statement: AnnotatedSelectStatement,
        namespace: String,
        packageName: String,
    ) {
        val className = statement.annotations.queryResult ?: "${pascalize(namespace)}${statement.getDataClassName()}Result"
        val resultDataClass = resultEmitter.generateSelectResult(
            statement = statement,
            className = className,
        )
        createFileSpec(packageName, className)
            .addType(resultDataClass)
            .build()
            .writeTo(outputDir)
    }

    fun writeExecuteResultFile(
        statement: AnnotatedExecuteStatement,
        namespace: String,
        packageName: String,
    ) {
        val className = statement.annotations.queryResult
            ?: "${pascalize(namespace)}${statement.getDataClassName()}Result"
        val columnsToInclude = ReturningColumnsResolver.resolveColumns(generatorContext, statement)
        val resultDataClass = resultEmitter.generateExecuteResult(
            statement = statement,
            className = className,
            columnsToInclude = columnsToInclude
        )
        createFileSpec(packageName, className)
            .addType(resultDataClass)
            .build()
            .writeTo(outputDir)
    }

    fun writeJoinedClassFile(
        statement: AnnotatedSelectStatement,
        namespace: String,
        packageName: String,
    ) {
        val joinedClassName = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val queryClassName = statement.getDataClassName()
            "${pascalize(namespace)}${queryClassName}Result_Joined"
        }
        val joinedDataClass = joinedEmitter.generateJoinedDataClass(
            joinedClassName = joinedClassName,
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator
        )
        createFileSpec(packageName, joinedClassName)
            .addType(joinedDataClass)
            .build()
            .writeTo(outputDir)
    }

    private fun createFileSpec(packageName: String, className: String): FileSpec.Builder {
        return FileSpec.builder(packageName, className)
            .addFileComment("Generated code for $packageName.$className")
            .addFileComment("\nDo not modify this file manually")
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "Suppress"))
                    .addMember("%S", "UNNECESSARY_NOT_NULL_ASSERTION")
                    .build()
            )
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )
    }
}
