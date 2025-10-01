package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.AnnotationConstants.MappingType
import dev.goquick.sqlitenow.gradle.inspect.SelectStatement

/**
 * Precomputed view of which fields should surface on generated result classes and mappings.
 * Built once per SELECT statement so data-structure and query generators stay in sync.
 */
data class ResultMappingPlan(
    val mappedColumns: Set<String>,
    val regularFields: List<AnnotatedSelectStatement.Field>,
    val dynamicFieldEntries: List<DynamicFieldEntry>,
    val skippedDynamicFieldNames: Set<String>,
) {
    data class DynamicFieldEntry(
        val field: AnnotatedSelectStatement.Field,
        val mappingType: MappingType?,
        val role: Role,
    )

    enum class Role { COLLECTION, PER_ROW, ENTITY, DEFAULT }

    val includedDynamicEntries: List<DynamicFieldEntry> by lazy {
        dynamicFieldEntries.filter { entry ->
            entry.field.src.fieldName !in skippedDynamicFieldNames
        }
    }

    val includedDynamicFields: List<AnnotatedSelectStatement.Field>
        get() = includedDynamicEntries.map { it.field }

    val includedCollectionEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.COLLECTION }

    val includedCollectionFields: List<AnnotatedSelectStatement.Field>
        get() = includedCollectionEntries.map { it.field }

    val includedPerRowEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.PER_ROW }

    val includedPerRowFields: List<AnnotatedSelectStatement.Field>
        get() = includedPerRowEntries.map { it.field }

    val includedEntityEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.ENTITY }

    val includedEntityFields: List<AnnotatedSelectStatement.Field>
        get() = includedEntityEntries.map { it.field }

    val includedDefaultDynamicEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.DEFAULT }
}

/** Planner that materialises [ResultMappingPlan]s from SELECT metadata. */
object ResultMappingPlanner {

    fun create(
        selectStatement: SelectStatement?,
        fields: List<AnnotatedSelectStatement.Field>,
    ): ResultMappingPlan {
        val dynamicMappings = if (selectStatement != null) {
            DynamicFieldMapper.createDynamicFieldMappings(selectStatement, fields)
        } else {
            emptyList()
        }

        val dynamicAliasPrefixes = fields
            .filter { it.annotations.isDynamicField }
            .associateBy({ it.src.fieldName }, { it.annotations.aliasPrefix })

        val mappedColumns = mutableSetOf<String>()
        dynamicMappings.forEach { mapping ->
            val aliasPrefix = dynamicAliasPrefixes[mapping.fieldName]
            mapping.columns
                .filterNot { DynamicFieldUtils.isNestedAlias(it.fieldName, aliasPrefix) }
                .forEach { column -> mappedColumns += column.fieldName }
        }

        // Treat SQLite's disambiguated duplicate columns (e.g., "category__id:1") as mapped so result
        // data classes don't surface redundant constructor parameters; the joined class still retains them.
        fields.filter { it.src.fieldName.contains(":") }
            .forEach { mappedColumns += it.src.fieldName }

        val skipSet = DynamicFieldUtils.computeSkipSet(fields)

        val aliasPrefixes = fields
            .asSequence()
            .filter { it.annotations.isDynamicField }
            .mapNotNull { it.annotations.aliasPrefix }
            .filter { it.isNotBlank() }
            .toSet()

        val regularFields = fields.filter { field ->
            !field.annotations.isDynamicField &&
                !mappedColumns.contains(field.src.fieldName) &&
                !field.src.fieldName.contains(":") &&
                aliasPrefixes.none { prefix ->
                    DynamicFieldUtils.isNestedAlias(field.src.fieldName, prefix)
                }
        }

        val dynamicEntries = fields.filter { it.annotations.isDynamicField }.map { field ->
            val mappingType = field.annotations.mappingType?.let { MappingType.fromString(it) }
            val role = when (mappingType) {
                MappingType.COLLECTION -> ResultMappingPlan.Role.COLLECTION
                MappingType.PER_ROW -> ResultMappingPlan.Role.PER_ROW
                MappingType.ENTITY -> ResultMappingPlan.Role.ENTITY
                null -> ResultMappingPlan.Role.DEFAULT
            }
            ResultMappingPlan.DynamicFieldEntry(field, mappingType, role)
        }

        return ResultMappingPlan(
            mappedColumns = mappedColumns,
            regularFields = regularFields,
            dynamicFieldEntries = dynamicEntries,
            skippedDynamicFieldNames = skipSet,
        )
    }
}
