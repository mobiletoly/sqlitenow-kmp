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
package dev.goquick.sqlitenow.gradle.processing

import com.squareup.kotlinpoet.*
import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants.PROPERTY_NAME_GENERATOR
import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter
import dev.goquick.sqlitenow.gradle.util.pascalize
import java.util.Locale
import kotlin.math.max

/**
 * Utility object for shared result type name generation.
 * Centralizes the logic to avoid duplication across multiple files.
 */
object SharedResultTypeUtils {
    /** The name of the shared result container object */
    const val SHARED_RESULT_OBJECT_NAME = "SharedResult"

    /**
     * Creates a TypeName for a shared result class.
     * @param packageName The package name for the generated code
     * @param namespace The namespace (e.g., "person")
     * @param sharedResultName The name of the shared result (e.g., "All")
     */
    fun createSharedResultTypeName(
        packageName: String,
        namespace: String,
        sharedResultName: String
    ): ClassName {
        return ClassName(packageName, "${pascalize(namespace)}Query")
            .nestedClass(SHARED_RESULT_OBJECT_NAME)
            .nestedClass(sharedResultName)
    }

    /**
     * Creates a TypeName for either a queryResult or regular result class.
     * @param packageName The package name for the generated code
     * @param namespace The namespace (e.g., "person")
     * @param statement The SELECT statement to check for queryResult annotation
     */
    fun createResultTypeName(
        packageName: String,
        namespace: String,
        statement: AnnotatedSelectStatement
    ): ClassName {
        return if (statement.annotations.queryResult != null) {
            // With queryResult: generate top-level class with exact specified name
            ClassName(packageName, statement.annotations.queryResult!!)
        } else {
            // Without queryResult: generate top-level class like PersonSelectSomeResult
            val className = statement.getDataClassName()
            val resultClassName = "${pascalize(namespace)}${className}Result"
            ClassName(packageName, resultClassName)
        }
    }

    /**
     * Creates a string representation of the result type for code generation.
     * @param namespace The namespace (e.g., "person")
     * @param statement The SELECT statement to check for queryResult annotation
     */
    fun createResultTypeString(
        namespace: String,
        statement: AnnotatedSelectStatement
    ): String {
        return if (statement.annotations.queryResult != null) {
            // With queryResult: use exact specified name
            statement.annotations.queryResult!!
        } else {
            // Without queryResult: generate name like PersonSelectSomeResult
            val className = statement.getDataClassName()
            "${pascalize(namespace)}${className}Result"
        }
    }

    /**
     * Creates a TypeName for EXECUTE statement result classes.
     * @param packageName The package name for the generated code
     * @param namespace The namespace (e.g., "person")
     * @param statement The EXECUTE statement to check for queryResult annotation
     */
    fun createResultTypeNameForExecute(
        packageName: String,
        namespace: String,
        statement: AnnotatedExecuteStatement
    ): ClassName {
        return if (statement.annotations.queryResult != null) {
            // With queryResult: generate top-level class with exact specified name
            ClassName(packageName, statement.annotations.queryResult!!)
        } else {
            // Without queryResult: generate top-level class like PersonAddResult
            val className = statement.getDataClassName()
            val resultClassName = "${pascalize(namespace)}${className}Result"
            ClassName(packageName, resultClassName)
        }
    }

    /**
     * Creates a string representation of the result type name for EXECUTE statements.
     * @param namespace The namespace (e.g., "person")
     * @param statement The EXECUTE statement to check for queryResult annotation
     */
    fun createResultTypeStringForExecute(
        namespace: String,
        statement: AnnotatedExecuteStatement
    ): String {
        return if (statement.annotations.queryResult != null) {
            // With queryResult: generate top-level class with exact specified name
            statement.annotations.queryResult!!
        } else {
            // Without queryResult: generate top-level class like PersonAddResult
            val className = statement.getDataClassName()
            "${pascalize(namespace)}${className}Result"
        }
    }
}

/**
 * Manages shared result classes across multiple SELECT statements.
 * Handles deduplication and validation of shared result structures.
 */
class SharedResultManager {
    
    /**
     * Represents a shared result class definition.
     */
    data class SharedResult(
        val name: String,
        val namespace: String,
        val fields: List<AnnotatedSelectStatement.Field>,
        val propertyNameGenerator: PropertyNameGeneratorType,
        val statementName: String,
        val sourceFile: String?,
    )
    
    private val sharedResults = mutableMapOf<String, SharedResult>()
    private val structureValidation = mutableMapOf<String, String>() // sharedResultKey -> structureKey
    
    /**
     * Registers a SELECT statement that uses a shared result.
     * Validates that all statements with the same sharedResult name have identical field structure.
     */
    fun registerSharedResult(
        statement: AnnotatedSelectStatement,
        namespace: String
    ): SharedResult? {
        val sharedResultName = statement.annotations.queryResult ?: return null
        
        val sharedResultKey = "${namespace}.${sharedResultName}"
        val structureKey = createStructureKey(statement.fields)
        
        // Validate structure consistency
        val existingStructureKey = structureValidation[sharedResultKey]
        if (existingStructureKey != null && existingStructureKey != structureKey) {
            val existingSharedResult = sharedResults[sharedResultKey]
            val message = buildStructureMismatchMessage(
                sharedResultName = sharedResultName,
                namespace = namespace,
                existingSharedResult = existingSharedResult,
                existingStructureKey = existingStructureKey,
                existingFields = existingSharedResult?.fields ?: emptyList(),
                newStructureKey = structureKey,
                newStatement = statement,
                newFields = statement.fields,
            )
            throw IllegalArgumentException(message)
        }
        
        // Register or retrieve shared result with inheritance support
        val existingSharedResult = sharedResults[sharedResultKey]
        val sharedResult = if (existingSharedResult != null) {
            if (existingSharedResult.propertyNameGenerator != statement.annotations.propertyNameGenerator) {
                throw IllegalArgumentException(
                    "Conflicting $PROPERTY_NAME_GENERATOR annotations for shared result '$sharedResultKey'.\n" +
                        "Existing: ${existingSharedResult.propertyNameGenerator}\n" +
                        "New statement '${statement.name}': ${statement.annotations.propertyNameGenerator}\n" +
                        "All SELECT statements using the same sharedResult must use the same propertyNameGenerator."
                )
            }
            existingSharedResult
        } else {
            val newSharedResult = SharedResult(
                name = sharedResultName,
                namespace = namespace,
                fields = statement.fields,
                propertyNameGenerator = statement.annotations.propertyNameGenerator,
                statementName = statement.name,
                sourceFile = statement.sourceFile,
            )
            sharedResults[sharedResultKey] = newSharedResult
            newSharedResult
        }
        
        structureValidation[sharedResultKey] = structureKey
        return sharedResult
    }

    /**
     * Validates that all statements using the same shared result have consistent annotations.
     */
    /**
     * Gets all shared results grouped by namespace.
     */
    fun getSharedResultsByNamespace(): Map<String, List<SharedResult>> {
        return sharedResults.values.groupBy { it.namespace }
    }
    
    /**
     * Checks if a SELECT statement uses a shared result.
     */
    fun isSharedResult(statement: AnnotatedSelectStatement): Boolean {
        return statement.annotations.queryResult != null
    }
    
    /**
     * Gets the shared result for a SELECT statement, if it uses one.
     */
    fun getSharedResult(statement: AnnotatedSelectStatement, namespace: String): SharedResult? {
        val sharedResultName = statement.annotations.queryResult ?: return null
        val sharedResultKey = "${namespace}.${sharedResultName}"
        return sharedResults[sharedResultKey]
    }

    private fun buildStructureMismatchMessage(
        sharedResultName: String,
        namespace: String,
        existingSharedResult: SharedResult?,
        existingStructureKey: String,
        existingFields: List<AnnotatedSelectStatement.Field>,
        newStructureKey: String,
        newStatement: AnnotatedSelectStatement,
        newFields: List<AnnotatedSelectStatement.Field>,
    ): String {
        val diff = computeFieldDiff(existingFields, newFields)

        val builder = StringBuilder()
        builder.appendLine(
            "Query result '$sharedResultName' in namespace '$namespace' has inconsistent field structure."
        )
        builder.appendLine(
            "All SELECT statements that share the same queryResult must expose identical field aliases, Kotlin types, and overrides."
        )
        builder.appendLine()
        builder.appendLine(
            "Existing statement: ${existingSharedResult?.statementName ?: "unknown"} (${existingSharedResult?.sourceFile
                ?: "unknown file"})"
        )
        builder.appendLine("Existing structure key: $existingStructureKey")
        builder.appendLine(
            "New statement: ${newStatement.name} (${newStatement.sourceFile ?: "unknown file"})"
        )
        builder.appendLine("New structure key: $newStructureKey")
        builder.appendLine()

        if (diff.onlyInExisting.isNotEmpty()) {
            builder.appendLine("Fields missing from the new statement:")
            diff.onlyInExisting.forEach { descriptor ->
                builder.appendLine("  - ${descriptor.alias}: ${descriptor.summary()}")
            }
            builder.appendLine()
        }

        if (diff.onlyInNew.isNotEmpty()) {
            builder.appendLine("New fields not present in the existing statement:")
            diff.onlyInNew.forEach { descriptor ->
                builder.appendLine("  + ${descriptor.alias}: ${descriptor.summary()}")
            }
            builder.appendLine()
        }

        if (diff.changed.isNotEmpty()) {
            builder.appendLine("Fields with conflicting definitions:")
            diff.changed.forEach { (existingDescriptor, newDescriptor) ->
                builder.appendLine("  * ${existingDescriptor.alias}:")
                builder.appendLine("      existing -> ${existingDescriptor.summary()}")
                builder.appendLine("      new      -> ${newDescriptor.summary()}")
            }
            builder.appendLine()
        }

        if (diff.onlyInExisting.isEmpty() && diff.onlyInNew.isEmpty() && diff.changed.isEmpty()) {
            builder.appendLine("No alias-level differences detected, but structure hashes diverge.")
        } else {
            val compared = max(existingFields.size, newFields.size)
            builder.appendLine("Compared $compared field definitions.")
        }

        return builder.toString().trimEnd()
    }

    private fun computeFieldDiff(
        existingFields: List<AnnotatedSelectStatement.Field>,
        newFields: List<AnnotatedSelectStatement.Field>,
    ): FieldDiffResult {
        val existingDescriptors = existingFields.associateBy(
            { it.src.fieldName.lowercase(Locale.ROOT) },
            { describeField(it) }
        )
        val newDescriptors = newFields.associateBy(
            { it.src.fieldName.lowercase(Locale.ROOT) },
            { describeField(it) }
        )

        val onlyExisting = (existingDescriptors.keys - newDescriptors.keys)
            .mapNotNull(existingDescriptors::get)
            .sortedBy { it.alias }

        val onlyNew = (newDescriptors.keys - existingDescriptors.keys)
            .mapNotNull(newDescriptors::get)
            .sortedBy { it.alias }

        val changed = existingDescriptors.keys.intersect(newDescriptors.keys)
            .mapNotNull { alias ->
                val existingDescriptor = existingDescriptors[alias]
                val newDescriptor = newDescriptors[alias]
                if (existingDescriptor != null && newDescriptor != null && existingDescriptor != newDescriptor) {
                    existingDescriptor to newDescriptor
                } else {
                    null
                }
            }
            .sortedBy { it.first.alias }

        return FieldDiffResult(
            onlyInExisting = onlyExisting,
            onlyInNew = onlyNew,
            changed = changed,
        )
    }

    private fun describeField(field: AnnotatedSelectStatement.Field): FieldDescriptor {
        val alias = field.src.fieldName
        val kotlinType = determineKotlinType(field)
        val propertyType = field.annotations.propertyType
        val normalizedNotNull = field.annotations.notNull ?: if (field.annotations.isDynamicField) {
            null
        } else {
            !field.src.isNullable
        }

        return FieldDescriptor(
            alias = alias,
            kotlinType = kotlinType,
            propertyType = propertyType,
            notNull = normalizedNotNull,
            sourceTable = field.src.tableName.takeIf { it.isNotBlank() },
            mappingType = field.annotations.mappingType,
            aliasPrefix = field.annotations.aliasPrefix,
        )
    }

    private fun determineKotlinType(field: AnnotatedSelectStatement.Field): String {
        return field.annotations.propertyType
            ?: SqliteTypeToKotlinCodeConverter.mapSqlTypeToKotlinType(field.src.dataType).toString()
    }

    private data class FieldDescriptor(
        val alias: String,
        val kotlinType: String,
        val propertyType: String?,
        val notNull: Boolean?,
        val sourceTable: String?,
        val mappingType: String?,
        val aliasPrefix: String?,
    ) {
        fun summary(): String {
            val parts = mutableListOf("type=$kotlinType")
            propertyType?.let { parts += "propertyType=$it" }
            notNull?.let { parts += "notNull=$it" }
            sourceTable?.let { parts += "source=$it" }
            mappingType?.let { parts += "mappingType=$it" }
            aliasPrefix?.takeIf { it.isNotBlank() }?.let { parts += "aliasPrefix=$it" }
            return parts.joinToString(", ")
        }
    }

    private data class FieldDiffResult(
        val onlyInExisting: List<FieldDescriptor>,
        val onlyInNew: List<FieldDescriptor>,
        val changed: List<Pair<FieldDescriptor, FieldDescriptor>>,
    )
    
    /**
     * Creates a structure key for field validation.
     * Uses Kotlin types instead of SQL types to handle SQLite's dynamic typing inconsistencies.
     * This ensures that fields with different SQL types (e.g., INTEGER vs NUMERIC) but the same
     * Kotlin type (e.g., kotlin.Long) are considered structurally identical.
     */
    private fun createStructureKey(fields: List<AnnotatedSelectStatement.Field>): String {
        return fields.map { field ->
            // Convert SQL type to Kotlin type for consistent comparison
            val kotlinType = SqliteTypeToKotlinCodeConverter.Companion.mapSqlTypeToKotlinType(field.src.dataType)
            val kotlinTypeString = kotlinType.toString()

            "${field.src.fieldName}:${kotlinTypeString}:${field.annotations.propertyType ?: "default"}"
        }.sorted().joinToString("|")
    }
}
