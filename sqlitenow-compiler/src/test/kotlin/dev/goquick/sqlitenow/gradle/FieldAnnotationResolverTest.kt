package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateViewStatement
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateViewStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import java.io.File
import java.sql.DriverManager
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class FieldAnnotationResolverTest {

    @Test
    fun propagatesAnnotationsAcrossNestedViews() {
        val schemaDir = createNestedViewFixture().resolve("schema")
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            val inspector = SchemaInspector(schemaDir)
            val tables = inspector.getCreateTableStatements(conn)
            val views = inspector.getCreateViewStatements(conn)

            val resolver = FieldAnnotationResolver(tables, views)

            val base = resolver.getFieldAnnotations("activity_detailed_view", "joined__act__pi__doc_id")
            assertNotNull(base, "activity_detailed_view should expose annotation for joined__act__pi__doc_id")

            val packageView = resolver.getFieldAnnotations("activity_package_with_activities_view", "joined__act__pi__doc_id")
            assertNotNull(packageView, "activity_package_with_activities_view should inherit annotation for joined__act__pi__doc_id")

            val bundleView = resolver.getFieldAnnotations("activity_bundle_with_activities_view", "joined__act__pi__doc_id")
            assertNotNull(bundleView, "activity_bundle_with_activities_view should inherit annotation for joined__act__pi__doc_id")
        }
    }

    @Test
    fun selectStatementInheritsNotNullFromNestedViews() {
        val schemaDir = createNestedViewFixture().resolve("schema")
        val queriesDir = schemaDir.parentFile.resolve("queries")
        assertTrue(queriesDir.exists(), "Queries directory not found at ${queriesDir.absolutePath}")

        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            val inspector = SchemaInspector(schemaDir)
            val tables = inspector.getCreateTableStatements(conn)
            val views = inspector.getCreateViewStatements(conn)
            val resolver = FieldAnnotationResolver(tables, views)
            val helper = StatementProcessingHelper(conn, resolver)

            val nsStatements = helper.processQueriesDirectory(queriesDir)
            val bundleNamespace = nsStatements["activity_bundle"]
            assertNotNull(bundleNamespace, "activity_bundle namespace should be discovered")

            val selectStatement = bundleNamespace
                .filterIsInstance<AnnotatedSelectStatement>()
                .firstOrNull { it.name == "selectAllWithEnabledActivities" }
            assertNotNull(selectStatement, "Expected selectAllWithEnabledActivities statement")

            val field = selectStatement.fields.firstOrNull { it.src.fieldName == "joined__act__pi__doc_id" }
            assertNotNull(field, "Query should expose joined__act__pi__doc_id column")
            assertEquals("joined__act__pi__doc_id", field.src.fieldName)
        }
    }

    @Test
    fun viewAnnotationsOverrideInheritedAnnotationsAndAugmentPropertyType() {
        val root = createTempDirectory("field-annotation-override").toFile()
        val schemaDir = root.resolve("schema").apply { mkdirs() }

        schemaDir.resolve("views.sql").writeText(
            """
            CREATE TABLE person (
                doc_id TEXT PRIMARY KEY NOT NULL
            );

            CREATE VIEW base_person_view AS
            SELECT
                p.doc_id AS doc_id
                /* @@{ field=doc_id, propertyName=baseDocId } */
            FROM person p;

            CREATE VIEW outer_person_view AS
            SELECT
                bp.doc_id AS doc_id
                /* @@{ field=doc_id, propertyName=outerDocId, notNull=true } */
            FROM base_person_view bp;
            """.trimIndent()
        )

        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            val inspector = SchemaInspector(schemaDir)
            val tables = inspector.getCreateTableStatements(conn)
            val views = inspector.getCreateViewStatements(conn)
            val resolver = FieldAnnotationResolver(tables, views)

            val resolved = resolver.getFieldAnnotations("outer_person_view", "doc_id")
            assertNotNull(resolved)
            assertEquals("outerDocId", resolved.propertyName)
            assertEquals(true, resolved.notNull)
            assertEquals("kotlin.String", resolved.propertyType)
        }
    }

    @Test
    fun resolverHandlesViewCyclesWithoutRecursingForever() {
        val resolver = FieldAnnotationResolver(
            createTableStatements = emptyList(),
            createViewStatements = listOf(
                fakeView(
                    viewName = "view_a",
                    aliasTarget = "view_b",
                    fieldAnnotations = FieldAnnotationOverrides(
                        propertyName = "fromA",
                        propertyType = null,
                        notNull = null,
                        adapter = null,
                    ),
                ),
                fakeView(
                    viewName = "view_b",
                    aliasTarget = "view_a",
                    fieldAnnotations = FieldAnnotationOverrides(
                        propertyName = "fromB",
                        propertyType = null,
                        notNull = null,
                        adapter = null,
                    ),
                ),
            )
        )

        val resolved = resolver.getFieldAnnotations("view_a", "value")
        assertNotNull(resolved)
        assertEquals("fromA", resolved.propertyName)
    }

    private fun createNestedViewFixture(): File {
        val root = createTempDirectory("field-annotation-resolver").toFile()
        val schemaDir = root.resolve("schema").apply { mkdirs() }
        val queriesDir = root.resolve("queries/activity_bundle").apply { mkdirs() }

        schemaDir.resolve("activity.sql").writeText(
            """
            CREATE TABLE activity (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL
            );

            CREATE TABLE program_item (
                id TEXT PRIMARY KEY NOT NULL,
                doc_id TEXT NOT NULL UNIQUE,
                activity_id TEXT NOT NULL REFERENCES activity(id)
            );

            CREATE VIEW activity_detailed_view AS
            SELECT
                act.id AS act__id,
                act.name AS act__name,
                pi.doc_id AS joined__act__pi__doc_id
            FROM activity act
            LEFT JOIN program_item pi ON pi.activity_id = act.id;
            """.trimIndent()
        )

        schemaDir.resolve("activity_package.sql").writeText(
            """
            CREATE TABLE activity_package (
                id TEXT PRIMARY KEY NOT NULL,
                activity_id TEXT NOT NULL REFERENCES activity(id)
            );

            CREATE VIEW activity_package_with_activities_view AS
            SELECT
                pkg.id AS pkg__id,
                adv.joined__act__pi__doc_id AS joined__act__pi__doc_id
            FROM activity_package pkg
            JOIN activity_detailed_view adv ON adv.act__id = pkg.activity_id;
            """.trimIndent()
        )

        schemaDir.resolve("activity_bundle.sql").writeText(
            """
            CREATE TABLE activity_bundle (
                id TEXT PRIMARY KEY NOT NULL,
                package_id TEXT NOT NULL REFERENCES activity_package(id)
            );

            CREATE VIEW activity_bundle_with_activities_view AS
            SELECT
                bnd.id AS bnd__id,
                pav.joined__act__pi__doc_id AS joined__act__pi__doc_id
            FROM activity_bundle bnd
            JOIN activity_package_with_activities_view pav ON pav.pkg__id = bnd.package_id;
            """.trimIndent()
        )

        queriesDir.resolve("selectAllWithEnabledActivities.sql").writeText(
            """
            SELECT
                b.joined__act__pi__doc_id
            FROM activity_bundle_with_activities_view b
            ORDER BY b.bnd__id;
            """.trimIndent()
        )

        return root
    }

    private fun fakeView(
        viewName: String,
        aliasTarget: String,
        fieldAnnotations: FieldAnnotationOverrides,
    ): AnnotatedCreateViewStatement {
        val selectStatement = SelectStatement(
            sql = "SELECT value FROM $aliasTarget",
            fromTable = aliasTarget,
            joinTables = emptyList(),
            fields = listOf(
                SelectStatement.FieldSource(
                    fieldName = "value",
                    tableName = aliasTarget,
                    originalColumnName = "value",
                    dataType = "TEXT",
                )
            ),
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            tableAliases = mapOf(aliasTarget to aliasTarget),
        )

        return AnnotatedCreateViewStatement(
            name = viewName,
            src = CreateViewStatement(
                sql = "CREATE VIEW $viewName AS SELECT value FROM $aliasTarget",
                viewName = viewName,
                selectStatement = selectStatement,
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = null,
                collectionKey = null,
            ),
            fields = listOf(
                AnnotatedCreateViewStatement.Field(
                    src = SelectStatement.FieldSource(
                        fieldName = "value",
                        tableName = aliasTarget,
                        originalColumnName = "value",
                        dataType = "TEXT",
                    ),
                    annotations = fieldAnnotations,
                )
            ),
            dynamicFields = emptyList(),
        )
    }
}
