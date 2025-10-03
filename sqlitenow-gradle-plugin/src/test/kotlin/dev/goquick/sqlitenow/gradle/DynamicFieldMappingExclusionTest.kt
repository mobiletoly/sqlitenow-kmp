package dev.goquick.sqlitenow.gradle

import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.PropertySpec
import dev.goquick.sqlitenow.gradle.generator.data.DataStructCodeGenerator
import dev.goquick.sqlitenow.gradle.generator.data.DataStructPropertyEmitter
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldMapper
import dev.goquick.sqlitenow.gradle.processing.DynamicFieldUtils
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SelectFieldCodeGenerator
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.sql.Connection
import kotlin.test.assertContains
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class DynamicFieldMappingExclusionTest {

    private fun newGenerator(): DataStructCodeGenerator {
        val conn = Mockito.mock(Connection::class.java)
        val tempDir = createTempDir(prefix = "sqlitenow-test-")
        tempDir.deleteOnExit()
        return DataStructCodeGenerator(
            conn = conn,
            queriesDir = tempDir,
            packageName = "dev.test",
            outputDir = tempDir,
            statementExecutors = mutableListOf(),
            providedCreateTableStatements = emptyList()
        )
    }

    private fun field(
        label: String,
        tableName: String,
        originalColumn: String = label,
        dataType: String = "TEXT",
    ): AnnotatedSelectStatement.Field {
        val src = Mockito.mock(SelectStatement.FieldSource::class.java)
        Mockito.`when`(src.fieldName).thenReturn(label)
        Mockito.`when`(src.tableName).thenReturn(tableName)
        Mockito.`when`(src.originalColumnName).thenReturn(originalColumn)
        Mockito.`when`(src.dataType).thenReturn(dataType)
        val ann = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = null,
            notNull = null,
            adapter = false
        )
        return AnnotatedSelectStatement.Field(src = src, annotations = ann)
    }

    private fun dynamicField(
        name: String,
        mappingType: String,
        sourceTableAlias: String,
        aliasPrefix: String? = null,
        propertyType: String = "List<Row>",
    ): AnnotatedSelectStatement.Field {
        val src = Mockito.mock(SelectStatement.FieldSource::class.java)
        Mockito.`when`(src.fieldName).thenReturn(name)
        Mockito.`when`(src.tableName).thenReturn("")
        Mockito.`when`(src.originalColumnName).thenReturn(name)
        Mockito.`when`(src.dataType).thenReturn("DYNAMIC")

        val ann = FieldAnnotationOverrides(
            propertyName = null,
            propertyType = propertyType,
            notNull = true,
            adapter = false,
            isDynamicField = true,
            aliasPrefix = aliasPrefix,
            mappingType = mappingType,
            sourceTable = sourceTableAlias,
            collectionKey = "joined_package_doc_id"
        )
        return AnnotatedSelectStatement.Field(src = src, annotations = ann)
    }

    @Test
    fun excludesMappedColumnsFromResult_forCollection() {
        val gen = newGenerator()

        val fields = listOf(
            field(label = "id", tableName = "person", originalColumn = "id", dataType = "INTEGER"),
            field(label = "joined_package_doc_id", tableName = "packages", originalColumn = "doc_id"),
            field(label = "joined_package_title", tableName = "packages", originalColumn = "title"),
            dynamicField(
                name = "packageDocs",
                mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
                sourceTableAlias = "pkg",
                aliasPrefix = "joined_package_",
                propertyType = "kotlin.collections.List<ActivityPackageQuery.SharedResult.Row>"
            )
        )

        val tableAliases = mapOf("pkg" to "packages")
        val mapped = DynamicFieldMapper.getMappedColumns(fields, tableAliases)
        val skipSet = DynamicFieldUtils.computeSkipSet(fields)
        assertTrue(mapped.contains("joined_package_doc_id"))
        assertTrue(mapped.contains("joined_package_title"))

        val ctor = FunSpec.constructorBuilder()
        val props = mutableListOf<PropertySpec>()
        val emitter = DataStructPropertyEmitter(gen.generatorContext)
        emitter.emitPropertiesWithInterfaceSupport(
            fields = fields,
            mappedColumns = mapped,
            dynamicFieldSkipSet = skipSet,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = null,
            excludeOverrideFields = null,
            fieldCodeGenerator = SelectFieldCodeGenerator(),
            constructorBuilder = ctor
        ) { p -> props += p }

        val names = props.map { it.name }.toSet()
        // Dynamic field itself should be present
        assertContains(names, "packageDocs")
        // Mapped columns should be excluded from main result
        assertFalse(names.contains("joinedPackageDocId"))
        assertFalse(names.contains("joinedPackageTitle"))
        // Unrelated column remains
        assertContains(names, "id")
    }

    @Test
    fun excludesMappedColumns_fromPerRow() {
        val gen = newGenerator()
        val fields = listOf(
            field(label = "id", tableName = "person", originalColumn = "id", dataType = "INTEGER"),
            field(label = "joined_package_doc_id", tableName = "packages", originalColumn = "doc_id"),
            field(label = "joined_package_title", tableName = "packages", originalColumn = "title"),
            dynamicField(
                name = "packageDoc",
                mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                sourceTableAlias = "pkg",
                aliasPrefix = "joined_package_",
                propertyType = "ActivityPackageQuery.SharedResult.Row"
            )
        )

        val tableAliases = mapOf("pkg" to "packages")
        val mapped = DynamicFieldMapper.getMappedColumns(fields, tableAliases)
        val skipSet = DynamicFieldUtils.computeSkipSet(fields)

        val ctor = FunSpec.constructorBuilder()
        val props = mutableListOf<PropertySpec>()
        val emitter = DataStructPropertyEmitter(gen.generatorContext)
        emitter.emitPropertiesWithInterfaceSupport(
            fields = fields,
            mappedColumns = mapped,
            dynamicFieldSkipSet = skipSet,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            implementsInterface = null,
            excludeOverrideFields = null,
            fieldCodeGenerator = SelectFieldCodeGenerator(),
            constructorBuilder = ctor
        ) { p -> props += p }

        val names = props.map { it.name }.toSet()
        assertContains(names, "packageDoc")
        assertFalse(names.contains("joinedPackageDocId"))
        assertFalse(names.contains("joinedPackageTitle"))
        assertContains(names, "id")
    }
}
