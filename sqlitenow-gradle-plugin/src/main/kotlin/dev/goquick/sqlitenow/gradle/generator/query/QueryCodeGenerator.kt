package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
import dev.goquick.sqlitenow.gradle.processing.StatementProcessor
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.io.File

/**
 * Generates Kotlin extension functions for query operations.
 * Works with DataStructCodeGenerator to create query extension functions.
 */
internal class QueryCodeGenerator(
    private val generatorContext: GeneratorContext,
    dataStructCodeGenerator: DataStructCodeGenerator,
    private val debug: Boolean = false,
) {
    private val packageName: String = generatorContext.packageName
    private val outputDir: File = generatorContext.outputDir
    private fun queryNamespaceName(namespace: String): String = pascalize(namespace) + "Query"
    private val columnLookup = generatorContext.columnLookup
    private val typeMapping = generatorContext.typeMapping
    private val parameterBinding =
        ParameterBinding(columnLookup, typeMapping, dataStructCodeGenerator, debug)
    private val adapterConfig = generatorContext.adapterConfig
    private val adapterParameterEmitter = AdapterParameterEmitter(generatorContext)
    private val queryFunctionScaffolder = QueryFunctionScaffolder(
        packageName = packageName,
        namespaceFormatter = { namespaceValue -> queryNamespaceName(namespaceValue) },
        adapterParameterEmitter = adapterParameterEmitter,
    )
    private val selectFieldGenerator = generatorContext.selectFieldGenerator
    private val adapterNameResolver = generatorContext.adapterNameResolver
    private val getterCallFactory = GetterCallFactory(
        adapterConfig = adapterConfig,
        adapterNameResolver = adapterNameResolver,
        selectFieldGenerator = selectFieldGenerator,
        typeMapping = typeMapping,
    )
    private val queryBindEmitter =
        QueryBindEmitter(parameterBinding, queryFunctionScaffolder, debug)
    private val resultMappingHelper = ResultMappingHelper(
        generatorContext = generatorContext,
        selectFieldGenerator = selectFieldGenerator,
        adapterConfig = adapterConfig,
    )
    private val collectionExecuteEmitter = CollectionExecuteEmitter(
        resultMappingHelper = resultMappingHelper,
        selectFieldGenerator = selectFieldGenerator,
        queryNamespaceName = { namespaceValue -> queryNamespaceName(namespaceValue) },
    )

    private val queryExecuteEmitter = QueryExecuteEmitter(
        packageName = packageName,
        debug = debug,
        scaffolder = queryFunctionScaffolder,
        adapterParameterEmitter = adapterParameterEmitter,
        queryNamespaceName = { namespaceValue -> queryNamespaceName(namespaceValue) },
        collectionMappingBuilder = collectionExecuteEmitter::emitExecuteAsListImplementation,
    )
    private val queryReadEmitter = QueryReadEmitter(
        packageName = packageName,
        queryNamespaceName = { namespaceValue -> queryNamespaceName(namespaceValue) },
        scaffolder = queryFunctionScaffolder,
        adapterParameterEmitter = adapterParameterEmitter,
        adapterConfig = adapterConfig,
        selectFieldGenerator = selectFieldGenerator,
        typeMapping = typeMapping,
        resultMappingHelper = resultMappingHelper,
        generateGetterCallWithPrefixes = getterCallFactory::buildGetterCall,
        generateDynamicFieldMappingFromJoined = { request, rowsVar ->
            resultMappingHelper.generateDynamicFieldMappingCodeFromJoined(request, rowsVar)
        },
        createSelectLikeFieldsFromExecuteReturning = { statement ->
            ReturningColumnsResolver.createSelectLikeFields(generatorContext, statement)
        },
        findMainTableAlias = { fields -> generatorContext.findMainTableAlias(fields) },
    )

    /**
     * Generates query extension function files for all namespaces.
     * Creates separate files per query like Person_SelectWeird.kt, Person_AddUser.kt, etc.
     */
    fun generateCode() {
        generatorContext.nsWithStatements.forEach { (namespace, statements) ->
            val statementProcessor = StatementProcessor(statements)
            statementProcessor.processStatements(
                onSelectStatement = { statement ->
                    emitQueryFile(namespace, statement)
                },
                onExecuteStatement = { statement ->
                    emitQueryFile(namespace, statement)
                }
            )
        }
    }

    private fun emitQueryFile(namespace: String, statement: AnnotatedStatement) {
        val className = statement.getDataClassName()
        val fileName = "${queryNamespaceName(namespace)}_$className"
        val fileBuilder = FileSpec.builder(packageName, fileName)
            .addFileComment("Generated query extension functions for ${namespace}.${className}")
            .addFileComment("\nDO NOT MODIFY THIS FILE MANUALLY!")
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
            .addImport("dev.goquick.sqlitenow.core.util", "jsonEncodeToSqlite")

        if (!debug) {
            fileBuilder.addImport("kotlinx.coroutines", "withContext")
        } else {
            fileBuilder.addImport("dev.goquick.sqlitenow.common", "sqliteNowLogger")
            fileBuilder.addImport("dev.goquick.sqlitenow.core.util", "sqliteNowPreview")
        }

        val bindFunction: FunSpec = queryBindEmitter.generateBindStatementParamsFunction(namespace, statement)
        fileBuilder.addFunction(bindFunction)

        when (statement) {
            is AnnotatedSelectStatement -> {
                if (!statement.hasCollectionMapping()) {
                    fileBuilder.addFunction(
                        queryReadEmitter.generateReadStatementResultFunction(namespace, statement)
                    )
                }
                if (statement.hasDynamicFieldMapping()) {
                    fileBuilder.addFunction(
                        queryReadEmitter.generateReadJoinedStatementResultFunction(namespace, statement)
                    )
                }
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    fileBuilder.addFunction(
                        queryReadEmitter.generateReadStatementResultFunctionForExecute(namespace, statement)
                    )
                }
            }

            is AnnotatedCreateTableStatement, is AnnotatedCreateViewStatement -> Unit
        }

        when (statement) {
            is AnnotatedSelectStatement -> {
                fileBuilder.addFunction(
                    queryExecuteEmitter.generateSelectQueryFunction(namespace, statement, "executeAsList")
                )
                if (!statement.hasCollectionMapping()) {
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateSelectQueryFunction(namespace, statement, "executeAsOne")
                    )
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateSelectQueryFunction(namespace, statement, "executeAsOneOrNull")
                    )
                }
            }

            is AnnotatedExecuteStatement -> {
                if (statement.hasReturningClause()) {
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateExecuteQueryFunction(namespace, statement, "executeReturningList")
                    )
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateExecuteQueryFunction(namespace, statement, "executeReturningOne")
                    )
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateExecuteQueryFunction(namespace, statement, "executeReturningOneOrNull")
                    )
                } else {
                    fileBuilder.addFunction(
                        queryExecuteEmitter.generateExecuteQueryFunction(namespace, statement, "execute")
                    )
                }
            }

            is AnnotatedCreateTableStatement, is AnnotatedCreateViewStatement -> return
        }

        fileBuilder.build().writeTo(outputDir)
    }

}
