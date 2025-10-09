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
        val implements: String?,
        val excludeOverrideFields: Set<String>?,
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
            // Validate consistency first
            validateSharedResultConsistency(existingSharedResult, statement, sharedResultKey)

            // Handle excludeOverrideFields inheritance and updating
            val finalExcludeOverrideFields = when {
                // If new statement has excludeOverrideFields but existing doesn't → update existing
                existingSharedResult.excludeOverrideFields == null && statement.annotations.excludeOverrideFields != null -> {
                    statement.annotations.excludeOverrideFields
                }
                // If existing has excludeOverrideFields but new statement doesn't → inherit from existing
                existingSharedResult.excludeOverrideFields != null && statement.annotations.excludeOverrideFields == null -> {
                    existingSharedResult.excludeOverrideFields
                }
                // Otherwise use existing (either both null, both same, or validation already passed)
                else -> {
                    existingSharedResult.excludeOverrideFields
                }
            }

            // Update the shared result if excludeOverrideFields changed
            if (finalExcludeOverrideFields != existingSharedResult.excludeOverrideFields) {
                val updatedSharedResult = existingSharedResult.copy(excludeOverrideFields = finalExcludeOverrideFields)
                sharedResults[sharedResultKey] = updatedSharedResult
                updatedSharedResult
            } else {
                existingSharedResult
            }
        } else {
            // First time registering this shared result
            val newSharedResult = SharedResult(
                name = sharedResultName,
                namespace = namespace,
                fields = statement.fields,
                propertyNameGenerator = statement.annotations.propertyNameGenerator,
                implements = statement.annotations.implements,
                excludeOverrideFields = statement.annotations.excludeOverrideFields,
                statementName = statement.name,
                sourceFile = statement.sourceFile
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
    private fun validateSharedResultConsistency(
        existingSharedResult: SharedResult,
        newStatement: AnnotatedSelectStatement,
        sharedResultKey: String
    ) {
        // Check if implements annotations are consistent
        if (existingSharedResult.implements != newStatement.annotations.implements) {
            val existingImplements = existingSharedResult.implements ?: "null"
            val newImplements = newStatement.annotations.implements ?: "null"

            throw IllegalArgumentException(
                "Conflicting 'implements' annotations for shared result '$sharedResultKey'.\n" +
                "Existing: implements=$existingImplements\n" +
                "New statement '${newStatement.name}': implements=$newImplements\n" +
                "All SELECT statements using the same sharedResult must have identical 'implements' annotations.\n" +
                "Either add the missing 'implements' annotation or use different sharedResult names."
            )
        }

        // Check if propertyNameGenerator is consistent
        if (existingSharedResult.propertyNameGenerator != newStatement.annotations.propertyNameGenerator) {
            throw IllegalArgumentException(
                "Conflicting $PROPERTY_NAME_GENERATOR annotations for shared result '$sharedResultKey'.\n" +
                "Existing: ${existingSharedResult.propertyNameGenerator}\n" +
                "New statement '${newStatement.name}': ${newStatement.annotations.propertyNameGenerator}\n" +
                "All SELECT statements using the same sharedResult must have identical propertyNameGenerator annotations."
            )
        }

        // Check if excludeOverrideFields is consistent or can be inherited
        validateExcludeOverrideFieldsConsistency(existingSharedResult, newStatement, sharedResultKey)
    }

    /**
     * Validates excludeOverrideFields consistency with inheritance support.
     *
     * Logic:
     * - If both have excludeOverrideFields specified → must be identical (consistency check)
     * - If new statement has no excludeOverrideFields → inherit from existing (if any)
     * - If existing has no excludeOverrideFields but new statement does → update existing
     * - If neither has excludeOverrideFields → no action needed
     */
    private fun validateExcludeOverrideFieldsConsistency(
        existingSharedResult: SharedResult,
        newStatement: AnnotatedSelectStatement,
        sharedResultKey: String
    ) {
        val existingExclude = existingSharedResult.excludeOverrideFields
        val newExclude = newStatement.annotations.excludeOverrideFields

        when {
            // Both have excludeOverrideFields specified → must be identical
            existingExclude != null && newExclude != null -> {
                if (existingExclude != newExclude) {
                    val existingStr = existingExclude.joinToString(",")
                    val newStr = newExclude.joinToString(",")

                    throw IllegalArgumentException(
                        "Conflicting 'excludeOverrideFields' annotations for shared result '$sharedResultKey'.\n" +
                        "Existing: excludeOverrideFields=$existingStr\n" +
                        "New statement '${newStatement.name}': excludeOverrideFields=$newStr\n" +
                        "All SELECT statements using the same sharedResult must have identical 'excludeOverrideFields' annotations."
                    )
                }
            }
            // Existing has excludeOverrideFields but new statement doesn't → inherit (handled in registerSharedResult)
            existingExclude != null && newExclude == null -> {
                // This case is handled by inheritance in registerSharedResult
            }
            // New statement has excludeOverrideFields but existing doesn't → update existing (handled in registerSharedResult)
            existingExclude == null && newExclude != null -> {
                // This case is handled by updating the existing shared result
            }
            // Neither has excludeOverrideFields → no action needed
            else -> {
                // Both are null, no action needed
            }
        }
    }

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

    /**
     * Gets the effective excludeOverrideFields for a statement, considering inheritance from shared results.
     * This method should be used by code generation to get the final excludeOverrideFields value.
     */
    fun getEffectiveExcludeOverrideFields(statement: AnnotatedSelectStatement, namespace: String): Set<String>? {
        val sharedResult = getSharedResult(statement, namespace)
        return if (sharedResult != null) {
            // For shared results, use the shared result's excludeOverrideFields (which may have been inherited)
            sharedResult.excludeOverrideFields
        } else {
            // For regular statements, use the statement's own excludeOverrideFields
            statement.annotations.excludeOverrideFields
        }
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
