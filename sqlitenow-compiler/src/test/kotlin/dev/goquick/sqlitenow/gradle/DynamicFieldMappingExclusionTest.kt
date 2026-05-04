package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.PropertySpec
import dev.goquick.sqlitenow.gradle.generator.data.DataStructPropertyEmitter
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import org.junit.jupiter.api.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse

class DynamicFieldMappingExclusionTest {

    private fun regularField(
        label: String,
        tableName: String,
        originalColumn: String = label,
        dataType: String = "TEXT",
    ): SelectStatement.FieldSource {
        return SelectStatement.FieldSource(
            fieldName = label,
            tableName = tableName,
            originalColumnName = originalColumn,
            dataType = dataType
        )
    }

    private fun dynamicField(
        name: String,
        mappingType: String,
        sourceTableAlias: String,
        aliasPrefix: String? = null,
        propertyType: String,
    ): AnnotatedSelectStatement.Field {
        val src = SelectStatement.FieldSource(
            fieldName = name,
            tableName = "",
            originalColumnName = name,
            dataType = "DYNAMIC"
        )
        val overrides = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = propertyType,
            notNull = true,
            adapter = false,
            isDynamicField = true,
            defaultValue = null,
            aliasPrefix = aliasPrefix,
            mappingType = mappingType,
            sourceTable = sourceTableAlias,
            collectionKey = "joined_package_doc_id",
            suppressProperty = false,
        )
        return AnnotatedSelectStatement.Field(src = src, annotations = overrides)
    }

    private fun buildStatement(
        regularSources: List<SelectStatement.FieldSource>,
        dynamicFields: List<AnnotatedSelectStatement.Field>,
        tableAliases: Map<String, String>
    ): AnnotatedSelectStatement {
        val annotatedRegularFields = regularSources.map { source ->
            AnnotatedSelectStatement.Field(
                src = source,
                annotations = FieldAnnotationOverrides.parse(emptyMap())
            )
        }
        val selectStatement = SelectStatement(
            sql = "SELECT stub",
            fromTable = tableAliases["person"] ?: tableAliases.values.firstOrNull(),
            joinTables = tableAliases.keys.filterNot { it == "person" },
            fields = regularSources,
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            parameterCastTypes = emptyMap(),
            tableAliases = tableAliases,
            joinConditions = emptyList()
        )
        val annotations = StatementAnnotationOverrides(
            name = null,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            queryResult = null,
            collectionKey = null
        )
        return AnnotatedSelectStatement(
            name = "TestStatement",
            src = selectStatement,
            annotations = annotations,
            fields = annotatedRegularFields + dynamicFields
        )
    }

    private fun emitProperties(statement: AnnotatedSelectStatement): List<PropertySpec> {
        val ctor = FunSpec.constructorBuilder()
        val props = mutableListOf<PropertySpec>()
        DataStructPropertyEmitter().emitPropertiesWithInterfaceSupport(
            statement = statement,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            fieldCodeGenerator = SelectFieldCodeGenerator(),
            constructorBuilder = ctor,
        ) { prop -> props += prop }
        return props
    }

    @Test
    fun excludesMappedColumnsFromResult_forCollection() {
        val regularSources = listOf(
            regularField(label = "id", tableName = "person", originalColumn = "id", dataType = "INTEGER"),
            regularField(label = "joined_package_doc_id", tableName = "packages", originalColumn = "doc_id"),
            regularField(label = "joined_package_title", tableName = "packages", originalColumn = "title"),
        )
        val dynamic = dynamicField(
            name = "packageDocs",
            mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
            sourceTableAlias = "pkg",
            aliasPrefix = "joined_package_",
            propertyType = "kotlin.collections.List<ActivityPackageQuery.SharedResult.Row>"
        )

        val statement = buildStatement(
            regularSources = regularSources,
            dynamicFields = listOf(dynamic),
            tableAliases = mapOf("person" to "person", "pkg" to "packages")
        )

        val properties = emitProperties(statement)
        val names = properties.map { it.name }.toSet()

        assertContains(names, "packageDocs")
        assertFalse(names.contains("joinedPackageDocId"))
        assertFalse(names.contains("joinedPackageTitle"))
        assertContains(names, "id")
    }

    @Test
    fun excludesMappedColumns_fromPerRow() {
        val regularSources = listOf(
            regularField(label = "id", tableName = "person", originalColumn = "id", dataType = "INTEGER"),
            regularField(label = "joined_package_doc_id", tableName = "packages", originalColumn = "doc_id"),
            regularField(label = "joined_package_title", tableName = "packages", originalColumn = "title"),
        )
        val dynamic = dynamicField(
            name = "packageDoc",
            mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
            sourceTableAlias = "pkg",
            aliasPrefix = "joined_package_",
            propertyType = "ActivityPackageQuery.SharedResult.Row"
        )

        val statement = buildStatement(
            regularSources = regularSources,
            dynamicFields = listOf(dynamic),
            tableAliases = mapOf("person" to "person", "pkg" to "packages")
        )

        val properties = emitProperties(statement)
        val names = properties.map { it.name }.toSet()

        assertContains(names, "packageDoc")
        assertFalse(names.contains("joinedPackageDocId"))
        assertFalse(names.contains("joinedPackageTitle"))
        assertContains(names, "id")
    }
}
