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
package dev.goquick.sqlitenow.gradle.processing

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveMap
import dev.goquick.sqlitenow.gradle.util.CaseInsensitiveSet
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import java.util.Locale
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.MySQLGroupConcat

/**
 * Generates KotlinPoet property specifications for data class properties based on
 * AnnotatedSelectStatement.Field parameters.
 *
 * This generator is specifically tailored for SQLite dialect.
 *
 * @param createTableStatements List of AnnotatedCreateTableStatement objects to assist with type and nullability inference
 */
class SelectFieldCodeGenerator(
    createTableStatements: List<AnnotatedCreateTableStatement> = emptyList(),
    private val createViewStatements: List<AnnotatedCreateViewStatement> = emptyList(),
    private val packageName: String? = null
) {
    // Convert the list to a map with table name as the key for more efficient lookups
    private val tableMap: Map<String, AnnotatedCreateTableStatement> =
        createTableStatements.associateBy { it.src.tableName.lowercase() }
    private val viewMap = CaseInsensitiveMap(createViewStatements.map { it.src.viewName to it })
    private val viewAliasMap = CaseInsensitiveMap(createViewStatements.map { it.name to it })
    private val kotlinStringType = ClassName("kotlin", "String")
    private val kotlinLongType = ClassName("kotlin", "Long")
    private val kotlinDoubleType = ClassName("kotlin", "Double")

    /**
     * Generates field information for a property or parameter based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate information for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A Pair of property name and property type
     */
    private fun generateFieldInfo(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType
    ): Pair<String, TypeName> {
        // For dynamic fields, use the field name directly and get type from propertyType annotation
        if (field.annotations.isDynamicField) {
            val propertyName = generatePropertyName(
                field.src.fieldName,
                field.annotations,
                propertyNameGeneratorType
            )

            // Determine nullability for dynamic fields
            val isNullable = when (field.annotations.notNull) {
                true -> false   // Explicitly marked as notNull
                false -> true   // Explicitly marked as nullable
                null -> {
                    // Default behavior: mapped dynamic fields (with mappingType) are nullable by default
                    // since they typically come from JOINs which can be null
                    field.annotations.mappingType != null
                }
            }

            // For dynamic fields, we must have a propertyType annotation
            val propertyType = field.annotations.propertyType?.let {
                // Use determinePropertyType with a dummy base type since we only care about the annotation
                SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
                    baseType = ClassName("kotlin", "String"),
                    propertyType = it,
                    isNullable = isNullable,
                    packageName = packageName
                )
            } ?: throw IllegalStateException("Dynamic field must have propertyType annotation")

            return Pair(propertyName, propertyType)
        }

        // Regular field processing
        val fieldName = field.src.fieldName
        val propertyName =
            generatePropertyName(fieldName, field.annotations, propertyNameGeneratorType)
        val baseType = mapSqlTypeToKotlinType(field.src.dataType, field)
        val isNullable = determineNullability(field)

        // Determine the final property type based on annotations and nullability
        // The SELECT statement annotations have higher priority than CREATE TABLE annotations
        val propertyType = SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
            baseType,
            field.annotations.propertyType,
            isNullable,
            packageName
        )

        return Pair(propertyName, propertyType)
    }

    /**
     * Generates a PropertySpec for a data class property based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate a property for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A PropertySpec representing the property
     */
    fun generateProperty(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType = PropertyNameGeneratorType.LOWER_CAMEL_CASE
    ): PropertySpec {
        val (propertyName, propertyType) = generateFieldInfo(field, propertyNameGeneratorType)

        // Build and return the property spec
        return PropertySpec.Companion.builder(propertyName, propertyType)
            .initializer(propertyName)
            .build()
    }

    /**
     * Generates a ParameterSpec for a constructor parameter based on the given field.
     *
     * @param field The AnnotatedSelectStatement.Field to generate a parameter for
     * @param propertyNameGeneratorType The type of property name generator to use
     * @return A ParameterSpec representing the constructor parameter
     */
    fun generateParameter(
        field: AnnotatedSelectStatement.Field,
        propertyNameGeneratorType: PropertyNameGeneratorType = PropertyNameGeneratorType.LOWER_CAMEL_CASE
    ): ParameterSpec {
        val (propertyName, propertyType) = generateFieldInfo(field, propertyNameGeneratorType)

        // Build the parameter spec
        val parameterBuilder = ParameterSpec.Companion.builder(propertyName, propertyType)

        // Add default value for dynamic fields if specified
        if (field.annotations.isDynamicField && field.annotations.defaultValue != null) {
            parameterBuilder.defaultValue(field.annotations.defaultValue)
        }

        return parameterBuilder.build()
    }

    /**
     * Finds a column in the schema based on field information.
     * This method encapsulates the common pattern of looking up columns by table name first,
     * then searching all tables if not found.
     *
     * @param field The field to find the column for
     * @return The column if found, null otherwise
     */
    private fun findColumnForField(field: AnnotatedSelectStatement.Field): AnnotatedCreateTableStatement.Column? {
        val aliasPrefix = field.annotations.aliasPrefix
        val sourceAlias = field.annotations.sourceTable
        val candidates = buildNameCandidatesFromAliasPrefix(field)

        field.src.tableName.takeIf { it.isNotBlank() }?.let { tableName ->
            tableMap[tableName.lowercase()]?.let { table ->
                lookupColumnUsingCandidates(table, field, candidates)?.let { return it }
            }

            findViewByName(tableName)?.let { view ->
                val viewField = findMatchingViewField(view, field, aliasPrefix, sourceAlias)
                if (viewField != null) {
                    tableMap[viewField.src.tableName.lowercase()]?.let { underlying ->
                        val viewCandidates = buildNameCandidates(
                            viewField.src.originalColumnName,
                            field.src.fieldName,
                            viewField.src.originalColumnName
                        )
                        lookupColumnUsingCandidates(
                            underlying,
                            field,
                            viewCandidates,
                            viewField.src.originalColumnName
                        )?.let { return it }
                    }
                }
            }
        }

        tableMap.values.forEach { table ->
            lookupColumnUsingCandidates(table, field, candidates)?.let { return it }
        }

        createViewStatements.forEach { view ->
            val viewField = findMatchingViewField(view, field, aliasPrefix, sourceAlias)
            if (viewField != null) {
                tableMap[viewField.src.tableName.lowercase()]?.let { table ->
                    val viewCandidates = buildNameCandidates(
                        viewField.src.originalColumnName,
                        field.src.fieldName,
                        viewField.src.originalColumnName
                    )
                    lookupColumnUsingCandidates(
                        table,
                        field,
                        viewCandidates,
                        viewField.src.originalColumnName
                    )?.let { return it }
                }
            }
        }

        return null
    }

    /**
     * Maps a SQLite type to a Kotlin type.
     *
     * @param sqliteType The SQLite type to map
     * @param field Optional field information for more accurate type mapping
     * @return The corresponding Kotlin type
     */
    private fun mapSqlTypeToKotlinType(
        sqliteType: String,
        field: AnnotatedSelectStatement.Field? = null
    ): TypeName {
        inferAggregateType(field)?.let { return it }

        // Extract the base type without size or precision information
        val baseType = sqliteType.split("(")[0].uppercase()

        // First check if we have a type from the SQL type mapping
        val kotlinType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(baseType)

        // If we have field information, try to get a more accurate type from the table schema
        if (field != null) {
            val column = findColumnForField(field)
            if (column != null) {
                val propertyType = column.annotations[AnnotationConstants.PROPERTY_TYPE] as? String
                if (propertyType != null) {
                    return SqliteTypeToKotlinCodeConverter.Companion.determinePropertyType(
                        kotlinType,
                        propertyType,
                        false,
                        packageName
                    )
                }

                // If no annotation, use the column's data type
                val columnType = column.src.dataType.uppercase()
                return SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(columnType)
            }
        }

        return kotlinType
    }

    private fun inferAggregateType(field: AnnotatedSelectStatement.Field?): TypeName? {
        val expression = field?.src?.expression ?: return null
        return when (expression) {
            is MySQLGroupConcat -> kotlinStringType
            is Function -> inferFunctionType(expression)
            else -> null
        }
    }

    private fun inferFunctionType(function: Function): TypeName? {
        return when (extractFunctionName(function)) {
            "COUNT", "COUNT_BIG" -> kotlinLongType
            "GROUP_CONCAT" -> kotlinStringType
            "SUM", "TOTAL", "AVG" -> kotlinDoubleType
            "JSON_GROUP_ARRAY", "JSON_GROUP_OBJECT" -> kotlinStringType
            else -> null
        }
    }

    private fun extractFunctionName(function: Function): String? {
        val explicitName = function.name?.takeIf { it.isNotBlank() }
        if (!explicitName.isNullOrBlank()) {
            return explicitName.uppercase(Locale.ROOT)
        }

        val raw = function.toString()
        val prefix = buildString {
            for (ch in raw) {
                if (ch.isLetterOrDigit() || ch == '_') {
                    append(ch)
                } else {
                    break
                }
            }
        }

        return prefix.takeIf { it.isNotBlank() }?.uppercase(Locale.ROOT)
    }

    private fun inferAggregateNullability(field: AnnotatedSelectStatement.Field): Boolean? {
        val expression = field.src.expression ?: return null
        val function = when (expression) {
            is Function -> expression
            else -> return null
        }

        return when (extractFunctionName(function)) {
            "COUNT", "COUNT_BIG" -> false
            else -> null
        }
    }

    /**
     * Generates a property name based on the field name, annotations, and property name generator type.
     *
     * @param fieldName The field name
     * @param annotations The field annotations
     * @param propertyNameGeneratorType The property name generator type
     * @return The generated property name
     */
    private fun generatePropertyName(
        fieldName: String,
        annotations: FieldAnnotationOverrides,
        propertyNameGeneratorType: PropertyNameGeneratorType
    ): String {
        // Use the property name from the annotation if present
        annotations.propertyName?.let { return it }

        // If no explicit property name is provided, apply the property name generator
        return propertyNameGeneratorType.convertToPropertyName(fieldName)
    }

    /**
     * Determines if a property should be nullable based on field annotations and table schema.
     *
     * @param field The field to check
     * @return True if the property should be nullable, false otherwise
     */
    fun determineNullability(field: AnnotatedSelectStatement.Field): Boolean {
        // Check for explicit notNull annotation first (highest priority)
        when (field.annotations.notNull) {
            true -> return false   // notNull=true means not nullable
            false -> return true   // notNull=false means nullable
            null -> {
                // notNull not specified - check table schema
                val column = findColumnForField(field)
                if (column != null) {
                    return column.isNullable()
                }
                inferAggregateNullability(field)?.let { return it }
                // Default to nullable if no explicit annotation or schema constraint is found
                return true
            }
        }
    }

    private fun lookupColumnUsingCandidates(
        table: AnnotatedCreateTableStatement,
        field: AnnotatedSelectStatement.Field?,
        candidates: LinkedHashSet<String>,
        preferredName: String? = null
    ): AnnotatedCreateTableStatement.Column? {
        val searchOrder = LinkedHashSet<String>()
        preferredName?.takeIf { it.isNotBlank() }?.let { searchOrder += it }
        searchOrder += candidates

        searchOrder.forEach { candidate ->
            table.findColumnByName(candidate)?.let { return it }
        }

        val caseInsensitive = CaseInsensitiveSet().apply { addAll(searchOrder) }
        table.columns.firstOrNull { column ->
            caseInsensitive.containsIgnoreCase(column.src.name)
        }?.let { return it }

        field?.annotations?.propertyName?.takeIf { it.isNotBlank() }?.let { propertyName ->
            val propertyLower = propertyName.lowercase()
            table.columns.firstOrNull { column ->
                column.src.name.lowercase() == propertyLower
            }?.let { return it }
        }

        return null
    }

    private fun buildColumnNameCandidates(vararg names: String): LinkedHashSet<String> {
        val candidates = LinkedHashSet<String>()
        names.forEach { name ->
            if (name.isBlank()) return@forEach
            val trimmed = name.trim()
            addNameVariants(trimmed, candidates)
        }
        return candidates
    }

    private fun addNameVariants(name: String, sink: MutableSet<String>) {
        if (name.isBlank()) return
        sink += name

        val withoutSuffix = name.substringBefore(':')
        if (withoutSuffix.isNotBlank()) sink += withoutSuffix

        val afterDot = withoutSuffix.substringAfterLast('.', withoutSuffix)
        if (afterDot.isNotBlank()) sink += afterDot

        val segments = afterDot.split('_').filter { it.isNotBlank() }
        if (segments.size > 1) {
            segments.indices.forEach { index ->
                sink += segments.drop(index).joinToString("_")
            }
        }
    }

    private fun buildNameCandidatesFromAliasPrefix(field: AnnotatedSelectStatement.Field): LinkedHashSet<String> {
        val result = buildColumnNameCandidates(field.src.originalColumnName, field.src.fieldName)
        val aliasPrefix = field.annotations.aliasPrefix
        if (!aliasPrefix.isNullOrBlank()) {
            field.src.fieldName.removePrefixOrNull(aliasPrefix)?.let { result += it }
            field.src.originalColumnName.removePrefixOrNull(aliasPrefix)?.let { result += it }
        }
        field.annotations.sourceTable?.takeIf { it.isNotBlank() }?.let { result += it }
        field.aliasPath.forEach { segment ->
            if (segment.isNotBlank()) result += segment
        }
        field.annotations.propertyName?.takeIf { it.isNotBlank() }?.let { result += it }
        return result
    }

    private fun buildNameCandidates(vararg names: String): LinkedHashSet<String> =
        buildColumnNameCandidates(*names)

    private fun findViewByName(name: String): AnnotatedCreateViewStatement? =
        viewMap[name] ?: viewAliasMap[name]

    private fun findMatchingViewField(
        view: AnnotatedCreateViewStatement,
        field: AnnotatedSelectStatement.Field,
        aliasPrefix: String?,
        sourceAlias: String?
    ): AnnotatedCreateViewStatement.Field? {
        val candidates = buildNameCandidatesFromAliasPrefix(field)
        return view.fields.firstOrNull { viewField ->
            val aliasMatches = sourceAlias.isNullOrBlank() ||
                    viewField.src.tableName.lowercase() == sourceAlias.lowercase()
            val prefixMatches = aliasPrefix.isNullOrBlank() ||
                    viewField.annotations.aliasPrefix?.lowercase() == aliasPrefix.lowercase()
            val candidateSet = CaseInsensitiveSet().apply { addAll(candidates) }
            (aliasMatches || prefixMatches) && (
                    candidateSet.containsIgnoreCase(viewField.src.fieldName) ||
                            candidateSet.containsIgnoreCase(viewField.src.originalColumnName)
                    )
        }
    }

    private fun String.removePrefixOrNull(prefix: String): String? {
        return if (this.startsWith(prefix)) this.removePrefix(prefix)
            .takeIf { it.isNotBlank() } else null
    }
}
