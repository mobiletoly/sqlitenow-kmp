package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.generator.query.DynamicFieldInvocation
import dev.goquick.sqlitenow.gradle.generator.query.ResultMappingHelper
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ResultMappingHelperTest {

    @Test
    fun `resolveJoinedPropertyName uses joined-name disambiguation for duplicate columns`() {
        val select = selectStatement(
            fields = listOf(
                regularFieldSource("doc_id", "person", "doc_id"),
                regularFieldSource("doc_id", "package", "doc_id"),
                regularFieldSource("title", "package", "title"),
            ),
            tableAliases = mapOf("p" to "person", "pkg" to "package"),
        )
        val fields = listOf(
            regularField("doc_id", "person", "doc_id"),
            regularField("doc_id", "package", "doc_id"),
            regularField("title", "package", "title"),
            dynamicField(
                fieldName = "packageDoc",
                mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                propertyType = "PackageDoc",
                sourceTable = "pkg",
            ),
        )
        val statement = annotatedSelectStatement(src = select, fields = fields)
        val helper = newHelper(selectStatements = listOf(statement))
        val mapping = statement.mappingPlan.dynamicMappingsByField.getValue("packageDoc")

        val resolved = helper.resolveJoinedPropertyName(
            column = mapping.columns.first { it.originalColumnName == "doc_id" },
            mapping = mapping,
            statement = statement,
            aliasPath = null,
            joinedNameMap = helper.computeJoinedNameMap(statement),
        )

        assertEquals("docId_package", resolved.property)
        assertEquals(false, resolved.suffixed)
    }

    @Test
    fun `generateConstructorArgumentsFromMapping recovers property names from table annotations`() {
        val packageTable = annotatedCreateTable(
            tableName = "package",
            columns = listOf(
                annotatedTableColumn("doc_id", "TEXT", propertyName = "docId"),
                annotatedTableColumn("category_type", "TEXT", propertyName = "kind"),
            ),
        )

        val select = selectStatement(
            fields = listOf(
                regularFieldSource("joined_pkg_doc_id", "package", "doc_id"),
                regularFieldSource("joined_pkg_category_type", "package", "category_type"),
            ),
            tableAliases = mapOf("pkg" to "package"),
        )
        val fields = listOf(
            regularField("joined_pkg_doc_id", "package", "doc_id"),
            regularField("joined_pkg_category_type", "package", "category_type"),
            dynamicField(
                fieldName = "packageDoc",
                mappingType = AnnotationConstants.MAPPING_TYPE_PER_ROW,
                propertyType = "PackageDoc",
                sourceTable = "pkg",
                aliasPrefix = "joined_pkg_",
            ),
        )
        val statement = annotatedSelectStatement(src = select, fields = fields)
        val helper = newHelper(createTables = listOf(packageTable), selectStatements = listOf(statement))
        val mapping = statement.mappingPlan.dynamicMappingsByField.getValue("packageDoc")

        val rendered = helper.generateConstructorArgumentsFromMapping(
            mapping = mapping,
            context = helperContext(
                helper = helper,
                statement = statement,
                fieldName = "packageDoc",
                sourceVar = "joinedData",
            ),
        )

        assertTrue(rendered.contains("docId = joinedData.joinedPkgDocId"))
        assertTrue(rendered.contains("kind = joinedData.joinedPkgCategoryType"))
    }

    @Test
    fun `buildFieldDebugComment includes alias path mapping type and notNull`() {
        val select = selectStatement(
            fields = listOf(regularFieldSource("bundle_id", "bundle", "id")),
            tableAliases = mapOf("b" to "bundle", "pkg" to "package"),
        )
        val packageDocs = dynamicField(
            fieldName = "packageDocs",
            mappingType = AnnotationConstants.MAPPING_TYPE_COLLECTION,
            propertyType = "kotlin.collections.List<PackageDoc>",
            sourceTable = "pkg",
            aliasPrefix = "joined_pkg_",
            aliasPath = listOf("bundle", "pkg"),
            collectionKey = "joined_pkg_doc_id",
            notNull = true,
        )
        val statement = annotatedSelectStatement(
            src = select,
            fields = listOf(regularField("bundle_id", "bundle", "id"), packageDocs),
        )
        val helper = newHelper(selectStatements = listOf(statement))

        val comment = helper.buildFieldDebugComment(
            field = packageDocs,
            selectStatement = statement.src,
            propertyNameGenerator = statement.annotations.propertyNameGenerator,
            includeType = false,
        )

        assertTrue(comment.contains("prefix=joined_pkg_"))
        assertTrue(comment.contains("aliasPath=bundle->pkg"))
        assertTrue(comment.contains("mapping=collection"))
        assertTrue(comment.contains("collectionKey=joined_pkg_doc_id"))
        assertTrue(comment.contains("notNull=true"))
    }

    @Test
    fun `generateDynamicFieldMappingCodeFromJoined rebuilds nested constructors`() {
        val parentSelect = selectStatement(
            fields = listOf(
                regularFieldSource("joined_pkg_doc_id", "package", "doc_id"),
                regularFieldSource("joined_pkg_title", "package", "title"),
                regularFieldSource("joined_pkg_category_doc_id", "category", "doc_id"),
                regularFieldSource("joined_pkg_category_name", "category", "name"),
            ),
            tableAliases = mapOf("pkg" to "package", "cat" to "category"),
        )
        val parentFields = listOf(
            regularField("joined_pkg_doc_id", "package", "doc_id"),
            regularField("joined_pkg_title", "package", "title"),
            regularField("joined_pkg_category_doc_id", "category", "doc_id"),
            regularField("joined_pkg_category_name", "category", "name"),
            dynamicField(
                fieldName = "packageDoc",
                mappingType = AnnotationConstants.MAPPING_TYPE_ENTITY,
                propertyType = "PackageResult",
                sourceTable = "pkg",
                aliasPrefix = "joined_pkg_",
                notNull = true,
            ),
        )
        val parentStatement = annotatedSelectStatement(
            src = parentSelect,
            fields = parentFields,
            queryResult = "ParentResult",
        )

        val packageStatement = annotatedSelectStatement(
            src = parentSelect,
            fields = listOf(
                regularField("joined_pkg_doc_id", "package", "doc_id"),
                regularField("joined_pkg_title", "package", "title"),
                regularField("joined_pkg_category_doc_id", "category", "doc_id"),
                regularField("joined_pkg_category_name", "category", "name"),
                dynamicField(
                    fieldName = "category",
                    mappingType = AnnotationConstants.MAPPING_TYPE_ENTITY,
                    propertyType = "CategoryResult",
                    sourceTable = "cat",
                    aliasPrefix = "joined_pkg_category_",
                    notNull = true,
                ),
            ),
            queryResult = "PackageResult",
        )

        val categoryStatement = annotatedSelectStatement(
            src = selectStatement(
                fields = listOf(
                    regularFieldSource("joined_pkg_category_doc_id", "category", "doc_id"),
                    regularFieldSource("joined_pkg_category_name", "category", "name"),
                ),
                tableAliases = mapOf("cat" to "category"),
            ),
            fields = listOf(
                regularField("joined_pkg_category_doc_id", "category", "doc_id"),
                regularField("joined_pkg_category_name", "category", "name"),
            ),
            queryResult = "CategoryResult",
        )

        val helper = newHelper(
            selectStatements = listOf(parentStatement, packageStatement, categoryStatement),
        )
        val mapping = parentStatement.mappingPlan.dynamicMappingsByField.getValue("packageDoc")

        val rendered = helper.generateDynamicFieldMappingCodeFromJoined(
            request = DynamicFieldInvocation(
                field = parentStatement.fields.first { it.src.fieldName == "packageDoc" },
                statement = parentStatement,
                mapping = mapping,
                sourceVar = "joinedData",
                baseIndentLevel = 0,
            ),
        )
        assertTrue(rendered.contains("PackageResult("))
        assertTrue(rendered.contains("joinedPkgDocId = joinedData.joinedPkgDocId"))
        assertTrue(rendered.contains("category = CategoryResult("), rendered)
        assertTrue(rendered.contains("name = joinedData.joinedPkgCategoryName"), rendered)
    }

    private fun newHelper(
        createTables: List<dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement> = emptyList(),
        selectStatements: List<dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement> = emptyList(),
    ): ResultMappingHelper {
        val context = generatorContext(
            createTableStatements = createTables,
            selectStatements = selectStatements,
        )
        return ResultMappingHelper(
            generatorContext = context,
            selectFieldGenerator = context.selectFieldGenerator,
            adapterConfig = context.adapterConfig,
        )
    }

    private fun helperContext(
        helper: ResultMappingHelper,
        statement: dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement,
        fieldName: String,
        sourceVar: String,
    ): ResultMappingHelper.ConstructorRenderContext {
        val field = statement.fields.first { it.src.fieldName == fieldName }
        val mapping = statement.mappingPlan.dynamicMappingsByField.getValue(fieldName)
        return ResultMappingHelper.ConstructorRenderContext(
            invocation = DynamicFieldInvocation(
                field = field,
                statement = statement,
                mapping = mapping,
                sourceVar = sourceVar,
                baseIndentLevel = 0,
            ),
            additionalIndent = 0,
            enforceNonNull = false,
            rowsVar = null,
            dynamicFieldMapper = { _, _ -> "null" },
        )
    }
}
