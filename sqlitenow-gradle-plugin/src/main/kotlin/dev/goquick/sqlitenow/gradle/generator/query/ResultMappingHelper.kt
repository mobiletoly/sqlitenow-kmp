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

/**
 * Minimal helper that centralises shared logic for mapping joined rows to DTOs.
 */
internal class ResultMappingHelper(
    private val generatorContext: GeneratorContext,
    private val selectFieldGenerator: SelectFieldCodeGenerator,
    private val adapterConfig: AdapterConfig,
) {

    private val dynamicFieldEmitter = DynamicFieldEmitter(this)
    private val nullGuardBuilder = NullGuardBuilder(this)
    private val collectionEmitter = CollectionMappingEmitter(this)
    private val resultConstructorEmitter = ResultConstructorEmitter(this)
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

    internal data class ConstructorRenderContext(
        val statement: AnnotatedSelectStatement,
        val sourceVariableName: String,
        val additionalIndent: Int,
        val enforceNonNull: Boolean,
        val rowsVar: String?,
        val baseIndentLevel: Int,
        val aliasPath: List<String>?,
        val dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
    )

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
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
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
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
    ): List<JoinedMappingEntry> {
        val propertyNameGenerator = statement.annotations.propertyNameGenerator
        val joinedNameMap = computeJoinedNameMap(statement)
        val entries = mutableListOf<JoinedMappingEntry>()

        statement.mappingPlan.regularFields.forEach { field ->
            val propertyName = getPropertyName(field, propertyNameGenerator)
            val key = JoinedPropertyNameResolver.JoinedFieldKey(
                field.src.tableName.orEmpty(),
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
                val mappedValue = dynamicFieldMapper(field, statement, "joinedData", null, 2)
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

    fun resolveJoinedPropertyName(
        column: SelectStatement.FieldSource,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        aliasPath: List<String>?,
        joinedNameMap: Map<JoinedPropertyNameResolver.JoinedFieldKey, String>,
    ): String {
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
                joinedNameMap[key]?.let { return it }
            }
        }

        val fallbackName = candidateNames.firstOrNull().orEmpty().ifBlank { column.fieldName }
        return statement.annotations.propertyNameGenerator.convertToPropertyName(fallbackName)
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
        field.annotations.aliasPrefix?.takeIf { !it.isNullOrBlank() }?.let { parts += "prefix=$it" }
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

    fun isTargetPropertyNullable(column: SelectStatement.FieldSource): Boolean {
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
        return GenericTypeParser.Companion.extractFirstTypeArgument(typeName) ?: typeName
    }

    fun buildNullGuard(config: NullGuardBuilder.GuardConfig): String {
        return nullGuardBuilder.build(config)
    }

    /** Render constructor arguments for simple dynamic mappings that do not require nested DTO rebuilding. */
    fun generateFlatFieldMapping(
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVariableName: String,
        additionalIndent: Int,
        enforceNonNull: Boolean,
    ): String {
        val indent = "  ".repeat(additionalIndent + 3)
        val context = ConstructorRenderContext(
            statement = statement,
            sourceVariableName = sourceVariableName,
            additionalIndent = additionalIndent,
            enforceNonNull = enforceNonNull,
            rowsVar = null,
            baseIndentLevel = 0,
            aliasPath = null,
            dynamicFieldMapper = { _, _, _, _, _ -> "" },
        )
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
                    statement = parentStatement,
                    sourceVariableName = sourceVariableName,
                    additionalIndent = additionalIndent,
                    enforceNonNull = enforceNonNull,
                )
            }
        }
    }

    private fun generateDynamicFieldMappingFromJoinedInternal(
        dynamicField: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        sourceVar: String,
        rowsVar: String?,
        baseIndentLevel: Int,
    ): String {
        return dynamicFieldEmitter.emitFromJoined(
            dynamicField = dynamicField,
            statement = statement,
            mapping = mapping,
            sourceVar = sourceVar,
            rowsVar = rowsVar,
            baseIndentLevel = baseIndentLevel,
        )
    }

    /** Entry point used by read/execute emitters to derive mapping code from a joined row. */
    fun generateDynamicFieldMappingCodeFromJoined(
        dynamicField: AnnotatedSelectStatement.Field,
        statement: AnnotatedSelectStatement,
        sourceVar: String = "joinedData",
        rowsVar: String? = null,
        baseIndentLevel: Int = 2,
    ): String {
        val mapping = statement.mappingPlan.dynamicMappingsByField[dynamicField.src.fieldName]
        if (mapping == null || mapping.columns.isEmpty()) {
            return "null // No columns found for mapping"
        }
        val mappingType =
            AnnotationConstants.MappingType.fromString(dynamicField.annotations.mappingType)
        if (mappingType == AnnotationConstants.MappingType.COLLECTION) {
            val rowsVariableName = rowsVar ?: "rowsForEntity"
            return generateCollectionMappingCode(
                dynamicField = dynamicField,
                mapping = mapping,
                statement = statement,
                sourceVar = sourceVar,
                rowsVar = rowsVariableName,
                baseIndentLevel = baseIndentLevel,
                generateConstructorArgumentsFromMapping = { map, ctx ->
                    generateConstructorArgumentsFromMapping(
                        mapping = map,
                        context = ctx,
                    )
                },
                dynamicFieldMapper = { field, stmt, src, rows, indent ->
                    generateDynamicFieldMappingCodeFromJoined(
                        dynamicField = field,
                        statement = stmt,
                        sourceVar = src,
                        rowsVar = rows,
                        baseIndentLevel = indent,
                    )
                },
            )
        }

        return dynamicFieldEmitter.emitFromJoined(
            dynamicField = dynamicField,
            statement = statement,
            mapping = mapping,
            sourceVar = sourceVar,
            rowsVar = rowsVar,
            baseIndentLevel = baseIndentLevel,
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

            val effectiveJoinedPropertyName = resolveJoinedPropertyName(
                column = column,
                mapping = mapping,
                statement = statement,
                aliasPath = context.aliasPath,
                joinedNameMap = joinedNameMap,
            )

            val normalizedFieldName = column.fieldName.substringBefore(':')
            val baseColumnName = column.originalColumnName.ifBlank { normalizedFieldName }
            val strippedBaseName =
                if (aliasPrefix != null && baseColumnName.startsWith(aliasPrefix)) {
                    baseColumnName.removePrefix(aliasPrefix)
                } else {
                    baseColumnName
                }

            val resolvedNameFromSource = mapping.sourceTableAlias
                ?.takeIf { it.isNotBlank() }
                ?.let { alias ->
                    findOriginalColumnPropertyName(strippedBaseName, alias, statement)
                }

            val parameterName = resolvedNameFromSource
                ?: propertyNameGenerator.convertToPropertyName(strippedBaseName)

            val expectedJoinedName =
                propertyNameGenerator.convertToPropertyName(normalizedFieldName)
            val isSuffixed = effectiveJoinedPropertyName != expectedJoinedName

            val isTargetNullable = isTargetPropertyNullable(column)
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
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
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
        val nestedIndent = baseIndent + "  "
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
        dynamicField: AnnotatedSelectStatement.Field,
        mapping: DynamicFieldMapper.DynamicFieldMapping,
        statement: AnnotatedSelectStatement,
        sourceVar: String,
        rowsVar: String,
        baseIndentLevel: Int,
        generateConstructorArgumentsFromMapping: (
            DynamicFieldMapper.DynamicFieldMapping,
            ConstructorRenderContext,
        ) -> String,
        dynamicFieldMapper: (
            AnnotatedSelectStatement.Field,
            AnnotatedSelectStatement,
            String,
            String?,
            Int,
        ) -> String,
    ): String {
        val builder = IndentedCodeBuilder(baseIndentLevel * 2)
        collectionEmitter.emitCollectionMapping(
            builder = builder,
            dynamicField = dynamicField,
            mapping = mapping,
            statement = statement,
            rowsVar = rowsVar,
            baseIndentLevel = baseIndentLevel,
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
            val mapping = statement.mappingPlan.dynamicMappingsByField[collectionField.src.fieldName]

            if (mapping != null && mapping.columns.isNotEmpty()) {
                val collectionExpr = generateCollectionMappingCode(
                    dynamicField = collectionField,
                    mapping = mapping,
                    statement = statement,
                    sourceVar = firstRowVar,
                    rowsVar = rowsVar,
                    baseIndentLevel = collectionIndentLevel,
                    generateConstructorArgumentsFromMapping = { map, ctx ->
                        generateConstructorArgumentsFromMapping(map, ctx)
                    },
                    dynamicFieldMapper = { field, stmt, src, rows, indent ->
                        generateDynamicFieldMappingCodeFromJoined(
                            dynamicField = field,
                            statement = stmt,
                            sourceVar = src,
                            rowsVar = rows,
                            baseIndentLevel = indent,
                        )
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
                val mappingCode = generateDynamicFieldMappingCodeFromJoined(
                    dynamicField = field,
                    statement = statement,
                    sourceVar = firstRowVar,
                    rowsVar = rowsVar,
                    baseIndentLevel = dynamicIndentLevel,
                )
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
        collectionKey: String,
        fields: List<AnnotatedSelectStatement.Field>,
    ): AnnotatedSelectStatement.Field? {
        return if (collectionKey.contains(".")) {
            val (tableAlias, columnName) = collectionKey.split(".", limit = 2)
            fields.find { field ->
                field.src.tableName == tableAlias && field.src.originalColumnName == columnName
            }
        } else {
            fields.find { field -> field.src.fieldName == collectionKey }
        }
    }

    fun findUniqueFieldForCollection(
        collectionField: AnnotatedSelectStatement.Field,
        annotatedStatement: AnnotatedSelectStatement,
    ): String? {
        val collectionKey = collectionField.annotations.collectionKey
        if (collectionKey.isNullOrBlank()) return null

        val matchingField = findFieldByCollectionKey(collectionKey, annotatedStatement.fields)
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
        statement: AnnotatedSelectStatement,
    ): String {
        val actualType = GenericTypeParser.Companion.extractFirstTypeArgument(targetPropertyType)
            ?: targetPropertyType
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
                val innerType = GenericTypeParser.Companion.extractFirstTypeArgument(type) ?: type
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
}

private inline fun buildStringList(builderAction: (MutableList<String>) -> Unit): List<String> {
    val lines = mutableListOf<String>()
    builderAction(lines)
    return lines
}
