package dev.goquick.sqlitenow.gradle.model

import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants.MappingType
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.util.AliasPathUtils
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement

/**
 * Precomputed view of which fields should surface on generated result classes and mappings.
 * Built once per SELECT statement so data-structure and query generators stay in sync.
 */
data class ResultMappingPlan(
    val mappedColumns: Set<String>,
    val regularFields: List<AnnotatedSelectStatement.Field>,
    val dynamicFieldEntries: List<DynamicFieldEntry>,
    val skippedDynamicFieldNames: Set<String>,
    val dynamicMappingsByField: Map<String, DynamicFieldMapper.DynamicFieldMapping>,
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

    val includedCollectionFields: List<AnnotatedSelectStatement.Field>
        get() = includedCollectionEntries.map { it.field }

    val includedCollectionEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.COLLECTION }

    val includedPerRowEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.PER_ROW }

    val includedEntityEntries: List<DynamicFieldEntry>
        get() = includedDynamicEntries.filter { it.role == Role.ENTITY }
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

        val containerEntries = dynamicEntries.filter { entry ->
            entry.role == ResultMappingPlan.Role.ENTITY || entry.role == ResultMappingPlan.Role.PER_ROW
        }

        val dynamicMappingsByField = dynamicMappings.associateBy { it.fieldName }

        val skipZeroColumnDuplicates = containerEntries.asSequence()
            .filter { entry ->
                dynamicMappingsByField[entry.field.src.fieldName]?.columns.isNullOrEmpty()
            }
            .mapNotNull { entry ->
                val aliasPathLower = AliasPathUtils.lowercase(entry.field.aliasPath)
                val hasAnnotatedSibling = containerEntries.any { other ->
                    if (other === entry) return@any false
                    val otherPath = AliasPathUtils.lowercase(other.field.aliasPath)
                    otherPath == aliasPathLower && !dynamicMappingsByField[other.field.src.fieldName]?.columns.isNullOrEmpty()
                }
                if (hasAnnotatedSibling) entry.field.src.fieldName else null
            }
            .toSet()

        val combinedSkipSet = (skipSet) + skipZeroColumnDuplicates

        return ResultMappingPlan(
            mappedColumns = mappedColumns,
            regularFields = regularFields,
            dynamicFieldEntries = dynamicEntries,
            skippedDynamicFieldNames = combinedSkipSet,
            dynamicMappingsByField = dynamicMappingsByField,
        )
    }
}
