package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import java.io.File

/**
 * Coordinates emission of a single generated Kotlin file per SQL statement.
 * Delegates the actual function body generation to the specialised emitters supplied
 * via constructor callbacks so `QueryCodeGenerator` stays as a thin orchestrator.
 */
internal class QueryFileEmitter(
    private val packageName: String,
    private val debug: Boolean,
    private val outputDir: File,
    private val queryNamespaceName: (String) -> String,
    private val generateBindStatementParamsFunction: (String, AnnotatedStatement) -> FunSpec,
    private val generateReadStatementResultFunction: (String, AnnotatedSelectStatement) -> FunSpec,
    private val generateReadJoinedStatementResultFunction: (String, AnnotatedSelectStatement) -> FunSpec,
    private val generateReadStatementResultFunctionForExecute: (String, AnnotatedExecuteStatement) -> FunSpec,
    private val generateSelectQueryFunction: (String, AnnotatedSelectStatement, String) -> FunSpec,
    private val generateExecuteQueryFunction: (String, AnnotatedExecuteStatement, String) -> FunSpec,
) {
    fun emit(namespace: String, statement: AnnotatedStatement) {
        val className = statement.getDataClassName()
        val fileName = "${queryNamespaceName(namespace)}_$className"
        val fileSpecBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated query extension functions for ${namespace}.${className}")
            .addFileComment("\nDO NOT MODIFY THIS FILE MANUALLY!")
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .addMember("%T::class", ClassName("kotlin.uuid", "ExperimentalUuidApi"))
                    .build()
            )
            .addImport("dev.goquick.sqlitenow.core.util", "jsonEncodeToSqlite")

        if (!debug) {
            fileSpecBuilder.addImport("kotlinx.coroutines", "withContext")
        } else {
            fileSpecBuilder.addImport("dev.goquick.sqlitenow.common", "sqliteNowLogger")
            fileSpecBuilder.addImport("dev.goquick.sqlitenow.core.util", "sqliteNowPreview")
        }

        val bindFunction = generateBindStatementParamsFunction(namespace, statement)
        fileSpecBuilder.addFunction(bindFunction)

        when (statement) {
            is AnnotatedSelectStatement -> {
                if (!statement.hasCollectionMapping()) {
                    fileSpecBuilder.addFunction(
                        generateReadStatementResultFunction(namespace, statement)
                    )
                }
                if (statement.hasDynamicFieldMapping()) {
                    fileSpecBuilder.addFunction(
                        generateReadJoinedStatementResultFunction(namespace, statement)
                    )
                }
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    fileSpecBuilder.addFunction(
                        generateReadStatementResultFunctionForExecute(namespace, statement)
                    )
                }
            }

            else -> Unit
        }

        when (statement) {
            is AnnotatedSelectStatement -> {
                fileSpecBuilder.addFunction(
                    generateSelectQueryFunction(namespace, statement, "executeAsList")
                )
                if (!statement.hasCollectionMapping()) {
                    fileSpecBuilder.addFunction(
                        generateSelectQueryFunction(namespace, statement, "executeAsOne")
                    )
                    fileSpecBuilder.addFunction(
                        generateSelectQueryFunction(namespace, statement, "executeAsOneOrNull")
                    )
                }
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    fileSpecBuilder.addFunction(
                        generateExecuteQueryFunction(namespace, statement, "executeReturningList")
                    )
                    fileSpecBuilder.addFunction(
                        generateExecuteQueryFunction(namespace, statement, "executeReturningOne")
                    )
                    fileSpecBuilder.addFunction(
                        generateExecuteQueryFunction(namespace, statement, "executeReturningOneOrNull")
                    )
                } else {
                    fileSpecBuilder.addFunction(
                        generateExecuteQueryFunction(namespace, statement, "execute")
                    )
                }
            }

            is AnnotatedCreateTableStatement, is AnnotatedCreateViewStatement -> return
        }

        fileSpecBuilder.build().writeTo(outputDir)
    }
}
