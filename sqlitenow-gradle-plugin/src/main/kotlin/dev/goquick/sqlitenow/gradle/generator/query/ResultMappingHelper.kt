package dev.goquick.sqlitenow.gradle.generator.query

import dev.goquick.sqlitenow.gradle.util.GenericTypeParser
import dev.goquick.sqlitenow.gradle.context.AdapterConfig
import dev.goquick.sqlitenow.gradle.context.GeneratorContext
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.JoinedPropertyNameResolver
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.util.AliasPathUtils
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.IndentedCodeBuilder
import java.util.IdentityHashMap

internal data class DynamicFieldInvocation(
    val field: AnnotatedSelectStatement.Field,
    val statement: AnnotatedSelectStatement,
    val mapping: DynamicFieldMapper.DynamicFieldMapping,
    val sourceVar: String,
    val baseIndentLevel: Int,
)

/**
 * Signature used by dynamic-field emitters.
 * - The first argument fully describes the field being mapped and the source join row.
 * - The optional `rowsVarName` denotes the variable that holds grouped rows when mapping
 *   collection results (null when mapping a single row/entity).
 */
internal typealias DynamicFieldExpression = (DynamicFieldInvocation, String?) -> String

/**
 * Minimal helper that centralises shared logic for mapping joined rows to DTOs.
 */
internal class ResultMappingHelper(
    private val generatorContext: GeneratorContext,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val adapterConfig: AdapterConfig,
) {

    private val dynamicFieldEmitter = DynamicFieldEmitter()
    private val nullGuardBuilder = NullGuardBuilder()
    private val collectionEmitter = CollectionMappingEmitter()
    private val resultConstructorEmitter = ResultConstructorEmitter()
    private val joinedNameMapCache =
        IdentityHashMap<AnnotatedSelectStatement, Map<JoinedPropertyNameResolver.JoinedFieldKey, String>>()
    private val tableLookupCache =
        IdentityHashMap<AnnotatedSelectStatement, CaseInsensitiveMap<AnnotatedCreateTableStatement>>()
    private val tableColumnLookupCache =
        IdentityHashMap<AnnotatedCreateTableStatement, CaseInsensitiveMap<AnnotatedCreateTableStatement.Column>>()

    private data class ColumnAssignment(
        val rendered: String,
        val isSuffixed: Boolean,
        val order: Int,
        val disambiguationIndex: Int?,
    )

    private data class NullGuardMetadata(
        val requiresGuard: Boolean,
        val relevantColumns: List<SelectStatement.FieldSource>,
    )

    private data class NullGuardConfig(
        val invocation: DynamicFieldInvocation,
        val relevantColumns: List<SelectStatement.FieldSource>,
        val notNull: Boolean,
        val rowsVarName: String?,
        val constructorExpression: String,
    )

    internal data class ConstructorRenderContext(
        val invocation: DynamicFieldInvocation,
        val additionalIndent: Int,
        val enforceNonNull: Boolean,
        val rowsVar: String?,
        val aliasPathOverride: List<String>? = null,
        val dynamicFieldMapper: DynamicFieldExpression,
    ) {
        val statement: AnnotatedSelectStatement get() = invocation.statement
        val sourceVariableName: String get() = invocation.sourceVar
        val baseIndentLevel: Int get() = invocation.baseIndentLevel
        val aliasPath: List<String>?
            get() = aliasPathOverride ?: invocation.field.aliasPath.takeIf { it.isNotEmpty() }
    }

    /** Cache-friendly wrapper around JoinedPropertyNameResolver to avoid recomputing per mapping site. */
    fun computeJoinedNameMap(statement: AnnotatedSelectStatement): Map<JoinedPropertyNameResolver.JoinedFieldKey, String> {
        joinedNameMapCache[statement]?.let { return it }
        val map = JoinedPropertyNameResolver.computeNameMap(
            fields = statement.fields,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            selectFieldGenerator = selectFieldGenerator,
        )
        joinedNameMapCache[statement] = map
        return map
    }

    private data class JoinedMappingEntry(val comment: String?, val assignment: String)

    fun addJoinedToMappedTransformation(
        builder: StringBuilder,
        statement: AnnotatedSelectStatement,
        dynamicFieldMapper: DynamicFieldExpression,
    ) {
        val entries = collectJoinedToMappedEntries(statement, dynamicFieldMapper)
        entries.forEach { entry ->
            entry.comment?.let { builder.append("  // $it\n") }
            builder.append("  ${entry.assignment},\n")
        }
    }

    /** Build the list of comment + assignment pairs for the strongly-typed DTO constructor. */
    private fun collectJoinedToMappedEntries(
        statement: AnnotatedSelectStatement,
        dynamicFieldMapper: DynamicFieldExpression,
    ): List<JoinedMappingEntry> {
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val joinedNameMap = computeJoinedNameMap(statement)
        val entries = mutableListOf<JoinedMappingEntry>()

        statement.mappingPlan.regularFields.forEach { field ->
            val propertyName = getPropertyName(field, propertyNameGenerator)
            val key = JoinedPropertyNameResolver.JoinedFieldKey(
                field.src.tableName,
                field.src.fieldName
            )
            val joinedProp = joinedNameMap[key] ?: propertyName
            val comment = buildFieldDebugComment(
                field = field,
                selectStatement = statement.src,
                propertyNameGenerator = propertyNameGenerator,
                includeType = true,
            ).takeIf { it.isNotEmpty() }

            entries += JoinedMappingEntry(
                comment = comment,
                assignment = "$propertyName = joinedData.$joinedProp",
            )
        }

        statement.mappingPlan.includedDynamicEntries.forEach { entry ->
            val field = entry.field
            val propertyName = getPropertyName(field, propertyNameGenerator)
            val comment = buildFieldDebugComment(
                field = field,
                selectStatement = statement.src,
                propertyNameGenerator = propertyNameGenerator,
                includeType = false,
            ).takeIf { it.isNotEmpty() }

            val assignment = if (entry.mappingType != null) {
                val mapping = statement.mappingPlan.dynamicMappingsByField[field.src.fieldName]
                val mappedValue = if (mapping != null) {
                    dynamicFieldMapper(
                        DynamicFieldInvocation(
                            field = field,
                            statement = statement,
                            mapping = mapping,
                            sourceVar = "joinedData",
                            baseIndentLevel = 2,
                        ),
                        null,
                    )
                } else {
                    "null"
                }
                "$propertyName = $mappedValue"
            } else {
                val defaultValue = field.annotations.defaultValue ?: "null"
                "$propertyName = $defaultValue"
            }

            entries += JoinedMappingEntry(
                comment = comment,
                assignment = assignment,
            )
        }

        return entries
    }

    data class ResolvedJoinedName(
        val property: String,
        val suffixed: Boolean,
    )

    fun resolveJoinedPropertyName(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        aliasPath: List<String>?,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>,
    ): ResolvedJoinedName {
        val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
        val tableCandidates = AliasResolutionHelper.buildTableCandidates(column, mapping, statement)
        val candidateNames = AliasResolutionHelper.candidateFieldNames(column, aliasPrefix)
        val preferSuffixed =
            AliasResolutionHelper.shouldPreferSuffixed(aliasPath, mapping, statement)

        tableCandidates.forEach { table ->
            val expandedCandidates =
                AliasResolutionHelper.expandCandidateNamesForTable(
                    table,
                    candidateNames,
                    joinedNameMap,
                    preferSuffixed
                )
            expandedCandidates.forEach { candidate ->
                val key = JoinedPropertyNameResolver.JoinedFieldKey(table, candidate)
                joinedNameMap[key]?.let { return ResolvedJoinedName(it, candidate.contains(':')) }
            }
        }

        val fallbackName = candidateNames.firstOrNull().orEmpty().ifBlank { column.fieldName }
        val resolved =
            statement.annotations.propertyNameGenerator.convertToPropertyName(fallbackName)
        return ResolvedJoinedName(resolved, fallbackName.contains(':'))
    }

    fun getPropertyName(
        field: AnnotatedSelectStatement.Field,
        propertyNameGenerator: PropertyNameGeneratorType,
    ): String {
        return adapterConfig.getPropertyName(field, propertyNameGenerator, selectFieldGenerator)
    }

    fun buildFieldDebugComment(
        field: AnnotatedSelectStatement.Field,
        selectStatement: SelectStatement,
        propertyNameGenerator: PropertyNameGeneratorType,
        includeType: Boolean,
    ): String {
        val parts = mutableListOf<String>()
        if (includeType) {
            val sqlType = field.src.dataType
            val kotlinType = selectFieldGenerator
                .generateProperty(field, propertyNameGenerator)
                .type
                .toString()
            parts += "type=$sqlType -> $kotlinType"
        }
        field.src.fieldName.takeIf { it.isNotBlank() }?.let { parts += "select=$it" }
        val sourceAlias = when {
            !field.annotations.sourceTable.isNullOrBlank() -> field.annotations.sourceTable
            field.src.tableName.isNotBlank() -> field.src.tableName
            else -> null
        }
        sourceAlias?.let { alias ->
            val target = selectStatement.tableAliases[alias] ?: alias
            val descriptor =
                if (!alias.equals(target, ignoreCase = true)) "$alias->$target" else alias
            parts += "source=$descriptor"
        }
        field.src.originalColumnName.takeIf { it.isNotBlank() && it != field.src.fieldName }
            ?.let { parts += "column=$it" }
        field.annotations.aliasPrefix?.takeIf { it.isNotBlank() }?.let { parts += "prefix=$it" }
        if (field.aliasPath.isNotEmpty()) {
            parts += "aliasPath=${field.aliasPath.joinToString("->")}"
        }
        field.annotations.mappingType?.let { mappingType ->
            parts += "mapping=${mappingType.lowercase()}"
            field.annotations.collectionKey?.takeIf { it.isNotBlank() }
                ?.let { key -> parts += "collectionKey=$key" }
        }
        field.annotations.notNull?.let { parts += "notNull=$it" }
        return parts.joinToString(", ")
    }

    fun isTargetPropertyNullable(
        statement: AnnotatedSelectStatement,
        column: SelectStatement.FieldSource,
    ): Boolean {
        val annotatedField = findAnnotatedField(statement, column)
        if (annotatedField != null) {
            return selectFieldGenerator.determineNullability(annotatedField)
        }

        val mockFieldAnnotations = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            notNull = null,
            adapter = false,
        )
        val mockField = AnnotatedSelectStatement.Field(
            src = column,
            annotations = mockFieldAnnotations,
        )
        return selectFieldGenerator.determineNullability(mockField)
    }

    private fun findAnnotatedField(
        statement: AnnotatedSelectStatement,
        column: SelectStatement.FieldSource,
    ): AnnotatedSelectStatement.Field? {
        val normalizedFieldName = column.fieldName.substringBefore(':')
        return statement.fields.firstOrNull { field ->
            val candidateFieldName = field.src.fieldName.substringBefore(':')
            val candidateOriginal = field.src.originalColumnName.substringBefore(':')
            val matchesField = candidateFieldName.equals(column.fieldName, ignoreCase = true) ||
                    candidateFieldName.equals(normalizedFieldName, ignoreCase = true)
            val matchesOriginal = column.originalColumnName.isNotBlank() &&
                    candidateOriginal.equals(column.originalColumnName, ignoreCase = true)
            val tableMatches = field.src.tableName.equals(column.tableName, ignoreCase = true) ||
                    field.src.tableName.isBlank() || column.tableName.isBlank()
            (matchesField || matchesOriginal) && tableMatches
        }
    }

    fun findOriginalColumnPropertyName(
        baseColumnName: String,
        sourceTableAlias: String,
        statement: AnnotatedSelectStatement,
    ): String? {
        val tableName = statement.src.tableAliases[sourceTableAlias] ?: sourceTableAlias
        val tableLookup = tableLookupCache.getOrPut(statement) {
            CaseInsensitiveMap(generatorContext.createTableStatements.map { it.src.tableName to it })
        }
        val table = tableLookup[tableName] ?: return null

        val columnLookup = tableColumnLookupCache.getOrPut(table) {
            CaseInsensitiveMap(table.columns.map { it.src.name to it })
        }
        val column = columnLookup[baseColumnName] ?: return null

        val propertyName = column.annotations[AnnotationConstants.PROPERTY_NAME] as? String
        return propertyName ?: statement.annotations.propertyNameGenerator.convertToPropertyName(
            baseColumnName
        )
    }

    fun requiresNestedConstruction(propertyType: String): Boolean {
        val actualType = GenericTypeParser.Companion.extractFirstTypeArgument(propertyType)
        val resultClassName = actualType.substringAfterLast('.')
        val nestedStatement = generatorContext.findSelectStatementByResultName(actualType)
            ?: generatorContext.findSelectStatementByResultName(resultClassName)

        return nestedStatement?.mappingPlan?.includedDynamicEntries?.isNotEmpty() == true
    }

    fun extractFirstTypeArgumentOrSelf(typeName: String): String {
        return GenericTypeParser.Companion.extractFirstTypeArgument(typeName)
    }

    /** Render constructor arguments for simple dynamic mappings that do not require nested DTO rebuilding. */
    fun generateFlatFieldMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        context: ConstructorRenderContext,
    ): String {
        val indent = "  ".repeat(context.additionalIndent + 3)
        val assignments = buildConstructorAssignments(mapping, context)
        return assignments.joinToString(",\n$indent") { it.rendered }
    }

    /** Rebuild nested DTO instances when dynamic mapping points at another generated result type. */
    fun generateNestedResultConstructor(
        targetPropertyType: String,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        parentStatement: AnnotatedSelectStatement,
        context: ConstructorRenderContext,
    ): String {
        val sourceVariableName = context.sourceVariableName
        val additionalIndent = context.additionalIndent
        val enforceNonNull = context.enforceNonNull
        val rowsVar = context.rowsVar
        val baseIndentLevel = context.baseIndentLevel
        val dynamicFieldMapper = context.dynamicFieldMapper
        val actualType = GenericTypeParser.Companion.extractFirstTypeArgument(targetPropertyType)
        val resultClassName = actualType.substringAfterLast('.')

        val nestedStatement = generatorContext.findSelectStatementByResultName(actualType)
            ?: generatorContext.findSelectStatementByResultName(resultClassName)

        return if (nestedStatement != null) {
            generateResultConstructorFromStatement(
                targetStatement = nestedStatement,
                parentStatement = parentStatement,
                sourceVar = sourceVariableName,
                rowsVar = rowsVar,
                additionalIndent = additionalIndent,
                enforceNonNull = enforceNonNull,
                baseIndentLevel = baseIndentLevel,
                dynamicFieldMapper = dynamicFieldMapper,
            )
        } else {
            val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
            if (aliasPrefix != null) {
                generateNestedObjectConstruction(
                    targetType = actualType,
                    mapping = mapping,
                    context = context,
                )
            } else {
                generateFlatFieldMapping(
                    mapping = mapping,
                    context = context,
                )
            }
        }
    }

    /** Entry point used by read/execute emitters to derive mapping code from a joined row. */
    fun generateDynamicFieldMappingCodeFromJoined(
        request: DynamicFieldInvocation,
        rowsVar: String? = null,
    ): String {
        val dynamicField = request.field
        val statement = request.statement
        val mapping = statement.mappingPlan.dynamicMappingsByField[dynamicField.src.fieldName]
        if (mapping == null || mapping.columns.isEmpty()) {
            return "null // No columns found for mapping"
        }
        val mappingType =
            AnnotationConstants.MappingType.fromString(dynamicField.annotations.mappingType)
        if (mappingType == AnnotationConstants.MappingType.COLLECTION) {
            val rowsVariableName = rowsVar ?: "rowsForEntity"
            return generateCollectionMappingCode(
                request = request.copy(mapping = mapping),
                rowsVar = rowsVariableName,
                generateConstructorArgumentsFromMapping = { map, ctx ->
                    generateConstructorArgumentsFromMapping(
                        mapping = map,
                        context = ctx,
                    )
                },
                dynamicFieldMapper = { nestedRequest, nestedRowsVar ->
                    generateDynamicFieldMappingCodeFromJoined(nestedRequest, nestedRowsVar)
                },
            )
        }

        return dynamicFieldEmitter.emitFromJoined(
            request = request.copy(mapping = mapping),
            rowsVar = rowsVar,
        )
    }

    fun generateConstructorArgumentsFromMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        context: ConstructorRenderContext,
    ): String {
        val statement = context.statement
        val targetPropertyType = mapping.propertyType
        if (requiresNestedConstruction(targetPropertyType)) {
            return generateNestedResultConstructor(
                targetPropertyType = targetPropertyType,
                mapping = mapping,
                parentStatement = statement,
                context = context,
            )
        }

        val indent = " ".repeat(6 + context.additionalIndent)
        val assignments = buildConstructorAssignments(mapping, context)
        return assignments.joinToString(",\n$indent") { it.rendered }
    }

    /**
     * Assemble constructor assignments for a dynamic mapping, taking
     * alias-prefix and suffix rules into account.
     */
    private fun buildConstructorAssignments(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        context: ConstructorRenderContext,
    ): List<ColumnAssignment> {
        val statement = context.statement
        val joinedNameMap = computeJoinedNameMap(statement)
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val aliasPrefix = mapping.aliasPrefix?.takeIf { it.isNotBlank() }
        val preferSuffixed =
            AliasResolutionHelper.shouldPreferSuffixed(context.aliasPath, mapping, statement)
        val assignments = linkedMapOf<String, ColumnAssignment>()

        mapping.columns.forEachIndexed { index, column ->
            if (DynamicFieldUtils.isNestedAlias(column.fieldName, mapping.aliasPrefix)) {
                return@forEachIndexed
            }

            val resolvedJoinedName = resolveJoinedPropertyName(
                column = column,
                mapping = mapping,
                statement = statement,
                aliasPath = context.aliasPath,
                joinedNameMap = joinedNameMap,
            )
            val effectiveJoinedPropertyName = resolvedJoinedName.property

            val normalizedFieldName = column.fieldName.substringBefore(':')
            val baseColumnName = column.originalColumnName.ifBlank { normalizedFieldName }
            val strippedBaseName =
                if (aliasPrefix != null && baseColumnName.startsWith(aliasPrefix)) {
                    baseColumnName.removePrefix(aliasPrefix)
                } else {
                    baseColumnName
                }

            val resolvedNameFromSource = mapping.sourceTableAlias
                .takeIf { it.isNotBlank() }
                ?.let { alias ->
                    findOriginalColumnPropertyName(strippedBaseName, alias, statement)
                }

            val parameterName = resolvedNameFromSource
                ?: propertyNameGenerator.convertToPropertyName(strippedBaseName)
            val isSuffixed = resolvedJoinedName.suffixed

            val isTargetNullable = isTargetPropertyNullable(statement, column)
            val valueExpression = when {
                context.enforceNonNull && !isTargetNullable -> "${context.sourceVariableName}.$effectiveJoinedPropertyName!!"
                isTargetNullable -> "${context.sourceVariableName}.$effectiveJoinedPropertyName"
                else -> "${context.sourceVariableName}.$effectiveJoinedPropertyName!!"
            }

            val disambiguationIndex = column.fieldName.substringAfter(':', "").toIntOrNull()

            val candidate = ColumnAssignment(
                rendered = "$parameterName = $valueExpression",
                isSuffixed = isSuffixed,
                order = index,
                disambiguationIndex = disambiguationIndex,
            )

            val existing = assignments[parameterName]
            if (existing == null || shouldReplaceAssignment(existing, candidate, preferSuffixed)) {
                assignments[parameterName] = candidate
            }
        }

        return assignments.values.sortedBy { it.order }
    }

    fun generateResultConstructorFromStatement(
        targetStatement: AnnotatedSelectStatement,
        parentStatement: AnnotatedSelectStatement,
        sourceVar: String,
        rowsVar: String?,
        additionalIndent: Int,
        enforceNonNull: Boolean,
        baseIndentLevel: Int,
        dynamicFieldMapper: DynamicFieldExpression,
    ): String {
        return resultConstructorEmitter.emitConstructorArguments(
            targetStatement = targetStatement,
            parentStatement = parentStatement,
            sourceVar = sourceVar,
            rowsVar = rowsVar,
            additionalIndent = additionalIndent,
            baseIndentLevel = baseIndentLevel,
            enforceNonNull = enforceNonNull,
            dynamicFieldMapper = dynamicFieldMapper,
        )
    }

    private fun shouldReplaceAssignment(
        existing: ColumnAssignment,
        candidate: ColumnAssignment,
        preferSuffixed: Boolean,
    ): Boolean {
        return when {
            preferSuffixed && candidate.isSuffixed && !existing.isSuffixed -> true
            !preferSuffixed && !candidate.isSuffixed && existing.isSuffixed -> true
            preferSuffixed && candidate.isSuffixed && existing.isSuffixed -> {
                val existingIndex = existing.disambiguationIndex ?: -1
                val candidateIndex = candidate.disambiguationIndex ?: -1
                candidateIndex > existingIndex
            }

            else -> false
        }
    }

    private fun generateNestedObjectConstruction(
        targetType: String,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        context: ConstructorRenderContext,
    ): String {
        val assignments = buildConstructorAssignments(mapping, context)
        if (assignments.isEmpty()) {
            return "${targetType.substringAfterLast('.')}()"
        }

        val baseIndent = "  ".repeat(context.additionalIndent + 3)
        val nestedIndent = "$baseIndent  "
        val renderedArgs = assignments.joinToString(",\n$nestedIndent") { it.rendered }
        val simpleTypeName = targetType.substringAfterLast('.')

        return buildString {
            append(simpleTypeName)
            append("(\n")
            append(nestedIndent)
            append(renderedArgs)
            append("\n")
            append(baseIndent)
            append(")")
        }
    }

    /** Render mapping code for collection dynamic fields (grouped rows → nested lists). */
    fun generateCollectionMappingCode(
        request: DynamicFieldInvocation,
        rowsVar: String,
        generateConstructorArgumentsFromMapping: (
            DynamicFieldMapper.DynamicFieldMapping,
            ConstructorRenderContext,
        ) -> String,
        dynamicFieldMapper: DynamicFieldExpression,
    ): String {
        val builder = IndentedCodeBuilder(request.baseIndentLevel * 2)
        collectionEmitter.emitCollectionMapping(
            builder = builder,
            context = request,
            rowsVar = rowsVar,
            constructorArgumentsProvider = generateConstructorArgumentsFromMapping,
            dynamicFieldMapper = dynamicFieldMapper,
        )
        return builder.build().trimEnd()
    }

    /**
     * Prepare comment + assignment blocks used when grouping rows by collection key before mapping to DTOs.
     */
    fun buildCollectionConstructorBlocks(
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
        collectionIndentLevel: Int = 3,
        dynamicIndentLevel: Int = 4,
    ): List<List<String>> {
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val joinedNameMap = computeJoinedNameMap(statement)
        val collectionFields = statement.mappingPlan.includedCollectionFields
        val collectionAliasPaths = collectionFields.mapNotNull { field ->
            field.aliasPath.takeIf { it.isNotEmpty() }?.let { AliasPathUtils.lowercase(it) }
        }

        val regularBlocks = statement.mappingPlan.regularFields.map { field ->
            val propertyName = getPropertyName(field, propertyNameGenerator)
            val key = JoinedPropertyNameResolver.JoinedFieldKey(
                field.src.tableName,
                field.src.fieldName
            )
            val joinedProp = joinedNameMap[key] ?: propertyName
            buildAssignmentBlock(
                field = field,
                statement = statement,
                propertyName = propertyName,
                assignmentExpression = "$firstRowVar.$joinedProp",
                includeTypeInComment = true,
            )
        }

        val perRowBlocks = buildDynamicAssignmentBlocks(
            fields = statement.mappingPlan.includedPerRowEntries.map { it.field },
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
            dynamicIndentLevel = dynamicIndentLevel,
            propertyNameGenerator = propertyNameGenerator,
            collectionAliasPaths = collectionAliasPaths,
        )

        val entityBlocks = buildDynamicAssignmentBlocks(
            fields = statement.mappingPlan.includedEntityEntries.map { it.field },
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
            dynamicIndentLevel = dynamicIndentLevel,
            propertyNameGenerator = propertyNameGenerator,
            collectionAliasPaths = collectionAliasPaths,
        )

        val collectionBlocks = collectionFields.map { collectionField ->
            val propertyName = getPropertyName(collectionField, propertyNameGenerator)
            val mapping =
                statement.mappingPlan.dynamicMappingsByField[collectionField.src.fieldName]

            if (mapping != null && mapping.columns.isNotEmpty()) {
                val collectionExpr = generateCollectionMappingCode(
                    request = DynamicFieldInvocation(
                        field = collectionField,
                        statement = statement,
                        mapping = mapping,
                        sourceVar = firstRowVar,
                        baseIndentLevel = collectionIndentLevel,
                    ),
                    rowsVar = rowsVar,
                    generateConstructorArgumentsFromMapping = { map, ctx ->
                        generateConstructorArgumentsFromMapping(map, ctx)
                    },
                    dynamicFieldMapper = { nestedRequest, nestedRowsVar ->
                        generateDynamicFieldMappingCodeFromJoined(nestedRequest, nestedRowsVar)
                    },
                )
                val exprLines = collectionExpr.split("\n")
                val firstLine = exprLines.firstOrNull() ?: "emptyList()"
                val trailingLines = if (exprLines.size > 1) exprLines.drop(1) else emptyList()
                buildAssignmentBlock(
                    field = collectionField,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = firstLine,
                    includeTypeInComment = false,
                    additionalLines = trailingLines,
                )
            } else {
                buildAssignmentBlock(
                    field = collectionField,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = "emptyList()",
                    includeTypeInComment = false,
                )
            }
        }

        return regularBlocks + perRowBlocks + entityBlocks + collectionBlocks
    }

    private fun buildDynamicAssignmentBlocks(
        fields: List<AnnotatedSelectStatement.Field>,
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
        dynamicIndentLevel: Int,
        propertyNameGenerator: PropertyNameGeneratorType,
        collectionAliasPaths: List<List<String>>,
    ): List<List<String>> {
        if (fields.isEmpty()) return emptyList()

        return fields
            .filter { field ->
                AliasResolutionHelper.shouldIncludeDynamicForCollection(
                    aliasPath = AliasPathUtils.lowercase(field.aliasPath),
                    collectionAliasPaths = collectionAliasPaths,
                )
            }
            .map { field ->
                val propertyName = getPropertyName(field, propertyNameGenerator)
                val mappingPlanMapping =
                    statement.mappingPlan.dynamicMappingsByField[field.src.fieldName]
                val mappingCode = if (mappingPlanMapping != null) {
                    generateDynamicFieldMappingCodeFromJoined(
                        DynamicFieldInvocation(
                            field = field,
                            statement = statement,
                            mapping = mappingPlanMapping,
                            sourceVar = firstRowVar,
                            baseIndentLevel = dynamicIndentLevel,
                        ),
                        rowsVar,
                    )
                } else {
                    "null"
                }
                buildAssignmentBlock(
                    field = field,
                    statement = statement,
                    propertyName = propertyName,
                    assignmentExpression = mappingCode,
                    includeTypeInComment = false,
                )
            }
    }

    /** Produce a reusable "comment + assignment" block for the generated constructor code. */
    private fun buildAssignmentBlock(
        field: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        propertyName: String,
        assignmentExpression: String,
        includeTypeInComment: Boolean,
        additionalLines: List<String> = emptyList(),
    ): List<String> {
        val comment = buildFieldDebugComment(
            field = field,
            selectStatement = statement.src,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            includeType = includeTypeInComment,
        )
        return buildStringList { lines ->
            if (comment.isNotEmpty()) {
                lines += "// $comment"
            }
            lines += "$propertyName = $assignmentExpression"
            if (additionalLines.isNotEmpty()) {
                lines += additionalLines
            }
        }
    }

    fun emitCollectionConstructorBlocks(
        builder: IndentedCodeBuilder,
        statement: AnnotatedSelectStatement,
        firstRowVar: String,
        rowsVar: String,
    ) {
        val blocks = buildCollectionConstructorBlocks(
            statement = statement,
            firstRowVar = firstRowVar,
            rowsVar = rowsVar,
        )

        blocks.forEachIndexed { blockIndex, lines ->
            val isLastBlock = blockIndex == blocks.lastIndex
            val indentPrefix = " ".repeat(builder.currentIndent())
            lines.forEachIndexed { lineIndex, line ->
                val suffix = if (lineIndex == lines.lastIndex && !isLastBlock) "," else ""
                val content = if (line.isNotEmpty() && line.first().isWhitespace()) {
                    line
                } else {
                    indentPrefix + line
                }
                builder.lineRaw(content + suffix)
            }
        }
    }

    fun findFieldByCollectionKey(
        statement: AnnotatedSelectStatement,
        collectionKey: String,
    ): AnnotatedSelectStatement.Field? {
        val preferredFields = statement.mappingPlan.regularFields
        val candidateSets = listOf(
            preferredFields,
            statement.mappingPlan.includedDynamicFields,
            statement.fields,
        )

        val tableLookup = mutableMapOf<Pair<String, String>, AnnotatedSelectStatement.Field>()
        preferredFields.forEach { field ->
            val table = field.src.tableName
            val column = field.src.originalColumnName.ifBlank { field.src.fieldName }
            tableLookup[table.lowercase() to column.lowercase()] = field
        }

        if (collectionKey.contains(".")) {
            val (tableAliasRaw, columnNameRaw) = collectionKey.split(".", limit = 2)
            val key = tableAliasRaw.lowercase() to columnNameRaw.lowercase()
            tableLookup[key]?.let { return it }

            candidateSets.forEach { fields ->
                fields.firstOrNull { field ->
                    field.src.tableName.equals(tableAliasRaw, ignoreCase = true) &&
                            (field.src.originalColumnName.equals(
                                columnNameRaw,
                                ignoreCase = true
                            ) ||
                                    field.src.fieldName.equals(columnNameRaw, ignoreCase = true))
                }?.let { return it }
            }
        } else {
            preferredFields.firstOrNull { field ->
                field.src.fieldName.equals(collectionKey, ignoreCase = true)
            }?.let { return it }

            candidateSets.forEach { fields ->
                fields.firstOrNull { field ->
                    field.src.fieldName.equals(collectionKey, ignoreCase = true) ||
                            field.src.originalColumnName.equals(collectionKey, ignoreCase = true)
                }?.let { return it }
            }
        }

        return null
    }

    fun findUniqueFieldForCollection(
        collectionField: AnnotatedSelectStatement.Field,
        annotatedStatement: AnnotatedSelectStatement,
    ): String? {
        val collectionKey = collectionField.annotations.collectionKey
        if (collectionKey.isNullOrBlank()) return null

        val matchingField = findFieldByCollectionKey(annotatedStatement, collectionKey)
        if (matchingField == null) {
            val availableFields = annotatedStatement.fields.map { field ->
                val tableInfo = if (field.src.tableName.isNotBlank()) {
                    "${field.src.tableName}.${field.src.originalColumnName}"
                } else {
                    field.src.originalColumnName
                }
                "$tableInfo AS ${field.src.fieldName}"
            }
            throw IllegalArgumentException(
                "collectionKey '$collectionKey' not found in SELECT statement. Available fields: $availableFields"
            )
        }

        val mapping =
            annotatedStatement.mappingPlan.dynamicMappingsByField[collectionField.src.fieldName]
        if (mapping != null) {
            val mappingColumn = mapping.columns.find { it.fieldName == matchingField.src.fieldName }
            if (mappingColumn != null) {
                val basePropertyName = if (mapping.aliasPrefix != null &&
                    mappingColumn.fieldName.startsWith(mapping.aliasPrefix)
                ) {
                    mappingColumn.fieldName.removePrefix(mapping.aliasPrefix)
                } else {
                    mappingColumn.originalColumnName
                }
                return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
                    basePropertyName
                )
            }
        }

        return annotatedStatement.annotations.propertyNameGenerator.convertToPropertyName(
            matchingField.src.fieldName
        )
    }

    fun findDistinctByPathForNestedConstruction(
        targetPropertyType: String,
        uniqueField: String,
    ): String {
        val actualType = GenericTypeParser.Companion.extractFirstTypeArgument(targetPropertyType)
        val resolved = resolveDistinctPathForType(actualType, uniqueField, mutableSetOf())
        return resolved ?: uniqueField
    }

    private fun resolveDistinctPathForType(
        rawType: String,
        uniqueField: String,
        visited: MutableSet<String>,
    ): String? {
        val normalizedType = normalizeType(rawType)
        if (!visited.add(normalizedType)) return null

        val nestedStatement = resolveStatementForType(normalizedType) ?: return null
        val propertyNameGenerator = nestedStatement.annotations.propertyNameGenerator

        nestedStatement.mappingPlan.regularFields.firstOrNull { field ->
            getPropertyName(field, propertyNameGenerator) == uniqueField
        }?.let {
            return uniqueField
        }

        nestedStatement.mappingPlan.includedDynamicEntries.forEach { entry ->
            val field = entry.field
            val mappingType = field.annotations.mappingType
            if (mappingType != null &&
                mappingType.equals(
                    AnnotationConstants.MappingType.COLLECTION.value,
                    ignoreCase = true
                )
            ) {
                return@forEach
            }
            val propertyName = getPropertyName(field, propertyNameGenerator)
            if (propertyName == uniqueField) {
                return propertyName
            }

            val propertyType = field.annotations.propertyType
            val nestedType = propertyType?.let { type ->
                val innerType = GenericTypeParser.Companion.extractFirstTypeArgument(type)
                normalizeType(innerType)
            }

            if (!nestedType.isNullOrBlank()) {
                resolveDistinctPathForType(nestedType, uniqueField, visited)?.let { nestedPath ->
                    return if (nestedPath.isEmpty()) propertyName else "$propertyName.$nestedPath"
                }
            }

            val mapping = nestedStatement.mappingPlan.dynamicMappingsByField[field.src.fieldName]
            if (mapping != null) {
                val aliasPrefix = mapping.aliasPrefix
                val match = mapping.columns.firstOrNull { column ->
                    val basePropertyName = when {
                        aliasPrefix != null && column.fieldName.startsWith(aliasPrefix) ->
                            column.fieldName.removePrefix(aliasPrefix)

                        column.originalColumnName.isNotBlank() -> column.originalColumnName
                        else -> column.fieldName
                    }
                    val candidate = propertyNameGenerator.convertToPropertyName(basePropertyName)
                    candidate == uniqueField
                }
                if (match != null) {
                    return "$propertyName.$uniqueField"
                }
            }
        }

        return null
    }

    private fun normalizeType(typeName: String): String = typeName.trim().removeSuffix("?")

    private fun resolveStatementForType(typeName: String): AnnotatedSelectStatement? {
        val simpleName = typeName.substringAfterLast('.')
        return generatorContext.findSelectStatementByResultName(typeName)
            ?: generatorContext.findSelectStatementByResultName(simpleName)
    }

    private inner class DynamicFieldEmitter {
        fun emitFromJoined(
            request: DynamicFieldInvocation,
            rowsVar: String?,
        ): String {
            val dynamicField = request.field
            val mapping = request.mapping
            val sourceVar = request.sourceVar
            val baseIndentLevel = request.baseIndentLevel

            val notNull = dynamicField.annotations.notNull == true
            val mappingType = dynamicField.annotations.mappingType
                ?.let { AnnotationConstants.MappingType.fromString(it) }
            val aliasPrefix = request.mapping.aliasPrefix.orEmpty()
            val hasNestedColumns = aliasPrefix.isNotEmpty() && request.mapping.columns.any {
                // Columns that reference deeper joined aliases carry the alias prefix later in the
                // field name (e.g., joined__pkg__category__). Those should still perform null-guards.
                DynamicFieldUtils.isNestedAlias(it.fieldName, aliasPrefix)
            }
            val skipNullGuardForEntity =
                mappingType == AnnotationConstants.MappingType.ENTITY &&
                    notNull &&
                    !hasNestedColumns
            val guardMetadata = if (skipNullGuardForEntity) {
                NullGuardMetadata(requiresGuard = false, relevantColumns = emptyList())
            } else {
                computeNullGuardMetadata(request)
            }
            val needsNullGuard = guardMetadata.requiresGuard

            val useRowVariable = needsNullGuard && rowsVar != null
            val effectiveSourceVar = if (useRowVariable) "row" else sourceVar

            val contextInvocation = request.copy(
                sourceVar = effectiveSourceVar,
                baseIndentLevel = baseIndentLevel,
            )
            val constructorContext = ConstructorRenderContext(
                invocation = contextInvocation,
                additionalIndent = 6,
                enforceNonNull = notNull,
                rowsVar = rowsVar,
                dynamicFieldMapper = { nestedRequest, nestedRowsVar ->
                    generateDynamicFieldMappingCodeFromJoined(nestedRequest, nestedRowsVar)
                },
            )
            val constructorArgs =
                generateConstructorArgumentsFromMapping(mapping, constructorContext)
            val constructorExpression = renderConstructorExpression(
                dynamicField.annotations.propertyType,
                constructorArgs,
            )

            if (!needsNullGuard) {
                return constructorExpression
            }

            val effectiveRowsVar = if (useRowVariable) {
                rowsVar
            } else {
                null
            }

            val guardConfig = NullGuardConfig(
                invocation = request,
                relevantColumns = guardMetadata.relevantColumns,
                notNull = notNull,
                rowsVarName = effectiveRowsVar,
                constructorExpression = constructorExpression,
            )
            return nullGuardBuilder.build(guardConfig)
        }

        private fun renderConstructorExpression(
            typeName: String?,
            constructorArgs: String,
        ): String {
            val nonNullType = typeName ?: "null"
            if (constructorArgs.isBlank()) {
                return "$nonNullType()"
            }

            val builder = IndentedCodeBuilder()
            builder.line("$nonNullType(")
            builder.indent {
                constructorArgs.trim().lines().forEach { raw ->
                    val trimmed = raw.trim()
                    if (trimmed.isNotEmpty()) builder.line(trimmed)
                }
            }
            builder.line(")")
            return builder.build().trimEnd()
        }
    }

    private fun computeNullGuardMetadata(invocation: DynamicFieldInvocation): NullGuardMetadata {
        val mapping = invocation.mapping
        val relevantColumns = mapping.columns.filterNot { column ->
            DynamicFieldUtils.isNestedAlias(column.fieldName, mapping.aliasPrefix)
        }
        if (relevantColumns.isEmpty()) {
            return NullGuardMetadata(
                requiresGuard = false,
                relevantColumns = relevantColumns,
            )
        }

        val sourceAlias = invocation.mapping.sourceTableAlias.lowercase()
        val resolvedSourceTable = sourceAlias.let { alias ->
            invocation.statement.src.tableAliases[alias]?.lowercase() ?: alias
        }
        val rootAlias = invocation.statement.src.tableAliases.keys.firstOrNull()?.lowercase()
            ?: invocation.statement.src.fromTable?.lowercase()

        fun shouldTreatAsJoin(column: SelectStatement.FieldSource): Boolean {
            val tableName = column.tableName.lowercase()
            val isJoinAlias = sourceAlias != rootAlias
            if (isJoinAlias) return true
            if (tableName.isBlank()) return false
            if (tableName == resolvedSourceTable) return false
            return true
        }

        val guardedColumns = mutableListOf<SelectStatement.FieldSource>()
        relevantColumns.forEach { column ->
            val propertyNullable = isTargetPropertyNullable(invocation.statement, column)
            val metadataNullable = column.isNullable

            val shouldGuard = when {
                propertyNullable -> true
                metadataNullable && shouldTreatAsJoin(column) -> true
                else -> false
            }

            if (shouldGuard) {
                guardedColumns += column
            }
        }

        val requiresGuard = guardedColumns.isNotEmpty()
        return NullGuardMetadata(
            requiresGuard = requiresGuard,
            relevantColumns = guardedColumns,
        )
    }

    private inner class NullGuardBuilder {
        fun build(config: NullGuardConfig): String {
            val invocation = config.invocation
            val relevantColumns = config.relevantColumns
            if (relevantColumns.isEmpty()) {
                return config.constructorExpression
            }

            val joinedNameMap = computeJoinedNameMap(invocation.statement)

            fun nullCheckCondition(variable: String): String {
                return relevantColumns.joinToString(" && ") { column ->
                    val resolved = resolveJoinedPropertyName(
                        column = column,
                        mapping = invocation.mapping,
                        statement = invocation.statement,
                        aliasPath = invocation.field.aliasPath,
                        joinedNameMap = joinedNameMap,
                    )
                    "$variable.${resolved.property} == null"
                }
            }

            val builder = IndentedCodeBuilder()
            if (config.rowsVarName != null) {
                val selectorCondition = nullCheckCondition("row")
                val selector = if (selectorCondition == "false") {
                    "${config.rowsVarName}.firstOrNull()"
                } else {
                    "${config.rowsVarName}.firstOrNull { row -> !($selectorCondition) }"
                }
                builder.line("$selector?.let { row ->")
                builder.indent(invocation.baseIndentLevel * 2 + 2) {
                    appendTrimmedBlock(this, config.constructorExpression)
                }
                if (config.notNull) {
                    builder.line("} ?: error(\"Required dynamic field '${invocation.field.src.fieldName}' is null\")")
                } else {
                    builder.line("}")
                }
            } else {
                val nullCondition = nullCheckCondition(invocation.sourceVar)
                builder.line("if ($nullCondition) {")
                builder.indent(invocation.baseIndentLevel * 2 + 2) {
                    if (config.notNull) {
                        line("error(\"Required dynamic field '${invocation.field.src.fieldName}' is null\")")
                    } else {
                        line("null")
                    }
                }
                builder.line("} else {")
                builder.indent(invocation.baseIndentLevel * 2 + 2) {
                    appendTrimmedBlock(this, config.constructorExpression)
                }
                builder.line("}")
            }
            return builder.build().trimEnd()
        }

        private fun appendTrimmedBlock(builder: IndentedCodeBuilder, text: String) {
            text.trimEnd().lines().forEach { raw ->
                val trimmed = raw.trim()
                if (trimmed.isEmpty()) {
                    builder.line("")
                } else {
                    builder.line(trimmed)
                }
            }
        }
    }

    private inner class CollectionMappingEmitter {
        fun emitCollectionMapping(
            builder: IndentedCodeBuilder,
            context: DynamicFieldInvocation,
            rowsVar: String,
            constructorArgumentsProvider: (
                DynamicFieldMapper.DynamicFieldMapping,
                ConstructorRenderContext,
            ) -> String,
            dynamicFieldMapper: DynamicFieldExpression,
        ) {
            val dynamicField = context.field
            val mapping = context.mapping
            val statement = context.statement
            val baseIndentLevel = context.baseIndentLevel
            val propertyType =
                dynamicField.annotations.propertyType ?: "kotlin.collections.List<Any>"
            val elementType = extractFirstTypeArgumentOrSelf(propertyType)
            val elementSimpleName = elementType.substringAfterLast('.')
            val propertyNameGenerator = statement.annotations.propertyNameGenerator
            val guardMetadata = computeNullGuardMetadata(context)
            val requiresNullGuard = guardMetadata.requiresGuard
            val nullCondition = if (requiresNullGuard) {
                val joinedNameMap = computeJoinedNameMap(statement)
                guardMetadata.relevantColumns.joinToString(" && ") { column ->
                    val resolved = resolveJoinedPropertyName(
                        column = column,
                        mapping = mapping,
                        statement = statement,
                        aliasPath = dynamicField.aliasPath,
                        joinedNameMap = joinedNameMap,
                    )
                    "row.${resolved.property} == null"
                }.ifBlank { "false" }
            } else {
                null
            }

            val requiresNested = requiresNestedConstruction(elementType)
            val rawGroupBy = mapping.groupByColumn?.takeIf { it.isNotBlank() }
                ?: dynamicField.annotations.collectionKey?.takeIf { it.isNotBlank() }
            val groupByProperty =
                rawGroupBy?.let { propertyNameGenerator.convertToPropertyName(it) }

            val chainBuilder = CollectionMappingBuilder(builder)
            chainBuilder.emit(rowsVar) {
                if (requiresNullGuard) {
                    filter(nullCondition!!)
                }

                if (requiresNested && groupByProperty != null) {
                    groupedMap(
                        groupExpression = "row.$groupByProperty",
                        rowsVarName = "rowsForNested",
                        firstRowVar = "firstNestedRow",
                        elementSimpleName = elementSimpleName,
                    ) {
                        val nestedInvocation = context.copy(
                            sourceVar = "firstNestedRow",
                            baseIndentLevel = baseIndentLevel + 2,
                        )
                        val nestedContext = ConstructorRenderContext(
                            invocation = nestedInvocation,
                            additionalIndent = 4,
                            enforceNonNull = dynamicField.annotations.notNull == true,
                            rowsVar = "rowsForNested",
                            dynamicFieldMapper = dynamicFieldMapper,
                        )
                        val nestedArgs = generateNestedResultConstructor(
                            targetPropertyType = elementType,
                            mapping = mapping,
                            parentStatement = statement,
                            context = nestedContext,
                        )
                        emitMultiline(nestedArgs)
                    }
                } else {
                    mapRows(
                        elementSimpleName = elementSimpleName,
                    ) {
                        val elementInvocation = context.copy(
                            sourceVar = "row",
                            baseIndentLevel = baseIndentLevel + 2,
                        )
                        val elementContext = ConstructorRenderContext(
                            invocation = elementInvocation,
                            additionalIndent = 4,
                            enforceNonNull = dynamicField.annotations.notNull == true,
                            rowsVar = rowsVar,
                            dynamicFieldMapper = dynamicFieldMapper,
                        )
                        val elementArgs = constructorArgumentsProvider(mapping, elementContext)
                        emitMultiline(elementArgs)
                    }
                }

                val distinctProperty = findUniqueFieldForCollection(dynamicField, statement)
                if (distinctProperty != null) {
                    val distinctPath = if (requiresNested) {
                        findDistinctByPathForNestedConstruction(
                            elementType,
                            distinctProperty,
                        )
                    } else {
                        distinctProperty
                    }
                    distinctBy(distinctPath)
                }
            }
        }
    }

    private inner class ResultConstructorEmitter {
        fun emitConstructorArguments(
            targetStatement: AnnotatedSelectStatement,
            parentStatement: AnnotatedSelectStatement,
            sourceVar: String,
            rowsVar: String?,
            additionalIndent: Int,
            baseIndentLevel: Int,
            enforceNonNull: Boolean,
            dynamicFieldMapper: DynamicFieldExpression,
        ): String {
            val indent = "  ".repeat(additionalIndent + 3)
            val propertyIndent = indent
            val propertyNameGenerator = targetStatement.annotations.propertyNameGenerator
            val mappingPlan = targetStatement.mappingPlan
            val joinedNameMap = computeJoinedNameMap(parentStatement)
            val properties = mutableListOf<String>()

            mappingPlan.regularFields.forEach { field ->
                val propName = getPropertyName(field, propertyNameGenerator)
                val tableAlias = field.src.tableName
                val candidateKeys = buildList {
                    add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, field.src.fieldName))
                    field.src.originalColumnName.takeIf { it.isNotBlank() }?.let { original ->
                        add(JoinedPropertyNameResolver.JoinedFieldKey(tableAlias, original))
                    }
                }
                val joinedPropertyName =
                    candidateKeys.firstNotNullOfOrNull { key -> joinedNameMap[key] }
                        ?: parentStatement.annotations.propertyNameGenerator
                            .convertToPropertyName(field.src.fieldName)
                val isNullable = isTargetPropertyNullable(targetStatement, field.src)
                val expression = when {
                    enforceNonNull && !isNullable -> "$sourceVar.$joinedPropertyName!!"
                    isNullable -> "$sourceVar.$joinedPropertyName"
                    else -> "$sourceVar.$joinedPropertyName!!"
                }
                properties += "$propName = $expression"
            }

            mappingPlan.includedDynamicFields.forEach { field ->
                val propName = getPropertyName(field, propertyNameGenerator)
                val nestedMapping = mappingPlan.dynamicMappingsByField[field.src.fieldName]
                val fieldExpression = if (nestedMapping != null) {
                    dynamicFieldMapper(
                        DynamicFieldInvocation(
                            field = field,
                            statement = targetStatement,
                            mapping = nestedMapping,
                            sourceVar = sourceVar,
                            baseIndentLevel = baseIndentLevel + additionalIndent + 2,
                        ),
                        rowsVar,
                    )
                } else {
                    "null"
                }
                properties += "$propName = $fieldExpression"
            }

            return if (properties.isEmpty()) {
                ""
            } else {
                properties.joinToString(",\n$propertyIndent")
            }
        }
    }
}

private inline fun buildStringList(builderAction: (MutableList<String>) -> Unit): List<String> {
    val lines = mutableListOf<String>()
    builderAction(lines)
    return lines
}
