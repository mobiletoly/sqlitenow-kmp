package dev.goquick.sqlitenow.gradle.generator.query

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.AdapterParameterEmitter
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.util.pascalize
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementProcessor
import dev.goquick.sqlitenow.gradle.processing.ReturningColumnsResolver
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
    private val queryBindEmitter =
        QueryBindEmitter(parameterBinding, queryFunctionScaffolder, debug)
    private val queryExecuteEmitter = QueryExecuteEmitter(
        packageName = packageName,
        debug = debug,
        scaffolder = queryFunctionScaffolder,
        adapterParameterEmitter = adapterParameterEmitter,
        queryNamespaceName = { namespaceValue -> queryNamespaceName(namespaceValue) },
        collectionMappingBuilder = this::addCollectionMappingExecuteAsListImplementation,
    )
    private val resultMappingHelper = ResultMappingHelper(
        generatorContext = generatorContext,
        selectFieldGenerator = selectFieldGenerator,
        adapterConfig = adapterConfig,
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
        generateGetterCallWithPrefixes = { statement, field, columnIndex, propertyNameGenerator, isFromJoinedTable, tableAliases, aliasPrefixes ->
            generateGetterCallInternalWithPrefixes(
                statement = statement,
                field = field,
                columnIndex = columnIndex,
                propertyNameGenerator = propertyNameGenerator,
                isFromJoinedTable = isFromJoinedTable,
                tableAliases = tableAliases,
                aliasPrefixes = aliasPrefixes,
            )
        },
        generateDynamicFieldMappingFromJoined = resultMappingHelper::generateDynamicFieldMappingCodeFromJoined,
        addReadJoinedStatementResultProcessing = { fnBld, statement, namespace ->
            addReadJoinedStatementResultProcessing(fnBld, statement, namespace)
        },
        createSelectLikeFieldsFromExecuteReturning = { statement ->
            ReturningColumnsResolver.createSelectLikeFields(generatorContext, statement)
        },
        createJoinedResultTypeName = { namespaceValue, statement ->
            createJoinedResultTypeName(namespaceValue, statement)
        },
    )
    private val queryFileEmitter = QueryFileEmitter(
        packageName = packageName,
        debug = debug,
        outputDir = outputDir,
        queryNamespaceName = { namespaceValue -> queryNamespaceName(namespaceValue) },
        generateBindStatementParamsFunction = queryBindEmitter::generateBindStatementParamsFunction,
        generateReadStatementResultFunction = queryReadEmitter::generateReadStatementResultFunction,
        generateReadJoinedStatementResultFunction = queryReadEmitter::generateReadJoinedStatementResultFunction,
        generateReadStatementResultFunctionForExecute = queryReadEmitter::generateReadStatementResultFunctionForExecute,
        generateSelectQueryFunction = queryExecuteEmitter::generateSelectQueryFunction,
        generateExecuteQueryFunction = queryExecuteEmitter::generateExecuteQueryFunction,
    )

    // --- Helper: choose final adapter parameter names for a statement, canonicalizing and de-duplicating by signature ---
    internal fun chooseAdapterParamNames(
        configs: List<AdapterConfig.ParamConfig>
    ): Map<AdapterConfig.ParamConfig, String> {
        return adapterParameterEmitter.chooseAdapterParamNames(configs)
    }

    // --- Helper: resolve chosen adapter param name for a given output field ---
    private fun resolveOutputAdapterParamNameForField(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>
    ): String? {
        return adapterNameResolver.resolveOutputAdapterParamNameForField(
            statement, field, tableAliases, aliasPrefixes, adapterConfig
        )
    }

    /**
     * Generates query extension function files for all namespaces.
     * Creates separate files per query like Person_SelectWeird.kt, Person_AddUser.kt, etc.
     */
    fun generateCode() {
        generatorContext.nsWithStatements.forEach { (namespace, statements) ->
            val statementProcessor = StatementProcessor(statements)
            statementProcessor.processStatements(
                onSelectStatement = { statement ->
                    queryFileEmitter.emit(namespace, statement)
                },
                onExecuteStatement = { statement ->
                    queryFileEmitter.emit(namespace, statement)
                }
            )
        }
    }

    /**
     * Helper function to create Joined Result type name for SELECT statements.
     * Uses SharedResult_Joined if the statement has sharedResult annotation, otherwise uses regular Result_Joined.
     */
    private fun createJoinedResultTypeName(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): ClassName {
        val capitalizedNamespace = queryNamespaceName(namespace)
        return if (statement.annotations.queryResult != null) {
            // For queryResult: PersonWithAddressRow_Joined (separate file)
            ClassName(packageName, "${statement.annotations.queryResult}_Joined")
        } else {
            // For regular results: PersonSelectWeirdResult_Joined (separate file)
            val className = statement.getDataClassName()
            val resultClassName = "${pascalize(namespace)}${className}Result"
            ClassName(packageName, "${resultClassName}_Joined")
        }
    }

    /**
     * Helper function to create Params type name.
     * Consolidates duplicate ClassName construction for Params types.
     */
    /**
     * Helper function to get the underlying SQLite type and appropriate getter for a field.
     * Returns a pair of (KotlinType, GetterCall) based on the original SQLite column type.
     */
    private fun getUnderlyingTypeAndGetter(
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int
    ): Pair<TypeName, String> {
        val kotlinType =
            SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(field.src.dataType)
        val baseGetterCall = typeMapping.getGetterCall(kotlinType, columnIndex)
        return Pair(kotlinType, baseGetterCall)
    }

    /**
     * Variant of getter call generation that uses known aliasPrefixes for dynamic fields.
     */
    private fun generateGetterCallInternalWithPrefixes(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        isFromJoinedTable: Boolean,
        tableAliases: Map<String, String>,
        aliasPrefixes: List<String>,
    ): String {
        val desiredType = selectFieldGenerator.generateProperty(field, propertyNameGenerator).type
        val isCustomDesiredType = isCustomKotlinType(desiredType)
        if (isCustomDesiredType || adapterConfig.hasAdapterAnnotation(field, aliasPrefixes)) {
            // Name the adapter param by the visible (aliased) column name, then canonicalize to provider namespace to reduce noise
            val visibleName = field.src.fieldName
            val columnName =
                PropertyNameGeneratorType.LOWER_CAMEL_CASE.convertToPropertyName(visibleName)
            val rawAdapterName = adapterConfig.getOutputAdapterFunctionName(columnName)
            val providerNs = if (field.src.tableName.isNotBlank()) {
                tableAliases[field.src.tableName] ?: field.src.tableName
            } else null
            val adapterParamName = resolveOutputAdapterParamNameForField(
                statement = statement,
                field = field,
                tableAliases = tableAliases,
                aliasPrefixes = aliasPrefixes
            ) ?: (providerNs?.let {
                adapterNameResolver.canonicalizeAdapterNameForNamespace(
                    it,
                    rawAdapterName
                )
            } ?: rawAdapterName)

            val inputNullable = desiredType.isNullable
            val baseGetterCall =
                getUnderlyingTypeAndGetter(field, columnIndex).second.replace("stmt", "statement")

            return if (isFromJoinedTable || inputNullable) {
                // For joined-table fields, the Joined property must be nullable; return null if DB value is NULL
                //   or
                // If the receiver property is nullable, propagate DB NULL as null (do not call adapter with null)
                "if (statement.isNull($columnIndex)) null else $adapterParamName($baseGetterCall)"
            } else {
                // Non-nullable receiver: read directly and let adapter handle the concrete value
                "$adapterParamName($baseGetterCall)"
            }
        } else {
            val kotlinType = desiredType
            val baseGetterCall = typeMapping
                .getGetterCall(kotlinType.copy(nullable = false), columnIndex)
                .replace("stmt", "statement")
            return if (isFromJoinedTable || desiredType.isNullable) {
                "if (statement.isNull($columnIndex)) null else $baseGetterCall"
            } else {
                baseGetterCall
            }
        }
    }

    private fun isCustomKotlinType(type: TypeName): Boolean {
        val t = type.toString()
        return !typeMapping.isStandardKotlinType(t)
    }

    /**
     * Helper function to add readJoinedStatementResult processing logic.
     * This generates code to read a single row from the statement and convert it to a Joined Result object.
     * Unlike the regular version, this includes ALL columns without any dynamic field mapping or exclusions.
     */
    private fun addReadJoinedStatementResultProcessing(
        fnBld: FunSpec.Builder,
        statement: AnnotatedSelectStatement,
        namespace: String
    ) {
        // Create the joined result type name
        val resultType = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val className = statement.getDataClassName()
            "${pascalize(namespace)}${className}Result_Joined"
        }
        // Build the constructor call with ALL properties (no exclusions)
        val constructorCall = buildString {
            append("return $resultType(\n")
            // Include ALL fields from the SELECT statement (no dynamic field mapping or exclusions)
            val allFields = statement.fields.filter { !it.annotations.isDynamicField }
            val joinedNameMap = resultMappingHelper.computeJoinedNameMap(statement)
            allFields.forEachIndexed { index, field ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(
                    field.src.tableName.orEmpty(),
                    field.src.fieldName
                )
                val propertyName = joinedNameMap[key]
                    ?: resultMappingHelper.getPropertyName(
                        field,
                        statement.annotations.propertyNameGenerator
                    )
                val getterCall = generateJoinedGetterCall(statement, field, index, allFields)
                val comment = resultMappingHelper.buildFieldDebugComment(
                    field = field,
                    selectStatement = statement.src,
                    propertyNameGenerator = statement.annotations.propertyNameGenerator,
                    includeType = true,
                )
                append("  $propertyName = $getterCall, // $comment\n")
            }
            append(")")
        }
        fnBld.addStatement(constructorCall)
    }

    /**
     * Generates executeAsList implementation for collection mapping queries.
     * This reads all joined rows, groups them by the main entity, and creates mapped objects
     * with collections.
     */
    private fun addCollectionMappingExecuteAsListImplementation(
        b: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        namespace: String,
        className: String,
        paramsString: String
    ) {
        val capitalizedNamespace = queryNamespaceName(namespace)
        val resultType = SharedResultTypeUtils.createResultTypeString(namespace, statement)
        val mappingPlan = statement.mappingPlan
        val collectionFields = mappingPlan.includedCollectionFields
        if (collectionFields.isEmpty()) return
        // Validate that statement-level collectionKey is provided when there are collection fields
        if (statement.annotations.collectionKey == null || statement.annotations.collectionKey.isBlank()) {
            throw IllegalArgumentException(
                "Statement-level annotation '${AnnotationConstants.COLLECTION_KEY}' is required when there are " +
                        "fields with '${AnnotationConstants.MAPPING_TYPE}=collection'. Found collection " +
                        "fields: ${collectionFields.map { it.annotations.propertyName ?: it.src.fieldName }}"
            )
        }
        // Determine the grouping key from statement-level collectionKey annotation
        // (validation that collectionKey exists is done above)
        val collectionKey = statement.annotations.collectionKey
        val groupingKey = if (collectionKey.contains(".")) {
            // Format: "p.id" -> find the field and convert to property name
            val (tableAlias, columnName) = collectionKey.split(".", limit = 2)
            val matchingField = statement.fields.find { field ->
                field.src.tableName == tableAlias && field.src.originalColumnName == columnName
            }
            if (matchingField != null) {
                // Use the actual property name from the field (considering propertyName annotation)
                resultMappingHelper.getPropertyName(
                    matchingField,
                    statement.annotations.propertyNameGenerator
                )
            } else {
                throw IllegalArgumentException("Statement-level collectionKey '$collectionKey' not found in SELECT statement")
            }
        } else {
            // Format: "person_id" -> find the field with this alias
            val matchingField = statement.fields.find { field ->
                field.src.fieldName == collectionKey
            }
            if (matchingField != null) {
                // Use the actual property name from the field (considering propertyName annotation)
                resultMappingHelper.getPropertyName(
                    matchingField,
                    statement.annotations.propertyNameGenerator
                )
            } else {
                throw IllegalArgumentException("Statement-level collectionKey '$collectionKey' not found in SELECT statement")
            }
        }
        // Get the joined class name with correct scope
        val joinedClassFullName = if (statement.annotations.queryResult != null) {
            "${statement.annotations.queryResult}_Joined"
        } else {
            val resultClassName = "${pascalize(namespace)}${className}Result"
            "${resultClassName}_Joined"
        }
        b.line("// Read all joined rows first")
        b.line("val joinedRows = mutableListOf<$joinedClassFullName>()")
        b.line("while (statement.step()) {")
        b.indent(by = 2) { b.line("joinedRows.add($capitalizedNamespace.$className.readJoinedStatementResult($paramsString))") }
        b.line("}")
        b.line("")
        // Determine the type of the grouping key field using the helper function
        val groupingField =
            resultMappingHelper.findFieldByCollectionKey(collectionKey, statement.fields)
        val groupingKeyType = if (groupingField != null) {
            val property =
                selectFieldGenerator.generateProperty(
                    groupingField,
                    PropertyNameGeneratorType.LOWER_CAMEL_CASE
                )
            property.type.copy(nullable = false)
                .toString() // Remove nullability for the Map key type
        } else {
            "Any" // Fallback, though this should not happen due to validation above
        }
        b.line("// Group joined rows by $groupingKey")
        b.line("val groupedRows: Map<$groupingKeyType, List<$joinedClassFullName>> = joinedRows.groupBy { it.$groupingKey }")
        b.line("")
        b.line("// Create mapped objects with collections")
        b.line("groupedRows.map { (_, rowsForEntity: List<$joinedClassFullName>) ->")
        b.indent(by = 2) {
            b.line("val firstRow = rowsForEntity.first()")
            b.line("$resultType(")
            b.indent(by = 2) {
                resultMappingHelper.emitCollectionConstructorBlocks(
                    builder = b,
                    statement = statement,
                    firstRowVar = "firstRow",
                    rowsVar = "rowsForEntity",
                )
            }
            b.line(")")
        }
        b.line("}")
    }

    /**
     * Generates getter call for joined data classes with proper JOIN nullability handling.
     * For fields from joined tables, always adds NULL checks regardless of schema nullability.
     */
    private fun generateJoinedGetterCall(
        statement: AnnotatedSelectStatement,
        field: AnnotatedSelectStatement.Field,
        columnIndex: Int,
        allFields: List<AnnotatedSelectStatement.Field>
    ): String {
        // Determine if this property's nullability in the Joined data class comes from JOINs
        val fieldTableAlias = field.src.tableName
        val mainTableAlias = generatorContext.findMainTableAlias(allFields)
        val joinedNullable = if (fieldTableAlias.isNotBlank()) {
            // If the field is not from the main table, Joined property must be nullable
            fieldTableAlias != mainTableAlias
        } else {
            // Fallback for cases without explicit alias metadata: rely on aliasPrefix from dynamic fields
            val dynAliasPrefixes = allFields
                .filter { it.annotations.isDynamicField }
                .mapNotNull { it.annotations.aliasPrefix }
                .filter { it.isNotBlank() }
            val visibleName = field.src.fieldName
            dynAliasPrefixes.any { prefix -> visibleName.startsWith(prefix) }
        }

        // Base (schema) nullability for the column itself
        val baseDesiredType = selectFieldGenerator
            .generateProperty(field, PropertyNameGeneratorType.LOWER_CAMEL_CASE)
            .type

        // Only use the "joined" null guard when nullability is introduced by JOINs.
        // If the base type is nullable, generateGetterCallInternalWithPrefixes will handle that path.
        val isFromJoinedForGetter = joinedNullable && !baseDesiredType.isNullable

        val aliasPrefixes = adapterConfig.collectAliasPrefixesForSelect(statement)
        return generateGetterCallInternalWithPrefixes(
            statement = statement,
            field = field,
            columnIndex = columnIndex,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            isFromJoinedTable = isFromJoinedForGetter,
            tableAliases = statement.src.tableAliases,
            aliasPrefixes = aliasPrefixes,
        )
    }
}
