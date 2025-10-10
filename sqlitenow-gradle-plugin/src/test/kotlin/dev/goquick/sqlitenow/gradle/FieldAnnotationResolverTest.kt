package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationResolver
import dev.goquick.sqlitenow.gradle.processing.StatementProcessingHelper
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SchemaInspector
import java.io.File
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class FieldAnnotationResolverTest {

    @Test
    fun propagatesAnnotationsAcrossDayTempoViews() {
        val schemaDir = locateSchemaDir()
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
        val schemaDir = locateSchemaDir()
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

    private fun locateSchemaDir(): File {
        val candidates = listOf(
            "daytempo-kmp/composeApp/src/commonMain/sql/DayTempoDatabase/schema",
            "../daytempo-kmp/composeApp/src/commonMain/sql/DayTempoDatabase/schema",
            "../../daytempo-kmp/composeApp/src/commonMain/sql/DayTempoDatabase/schema"
        )
        val schema = candidates
            .map { File(it).canonicalFile }
            .firstOrNull { it.exists() && it.isDirectory }
        assertTrue(schema != null, "Schema directory not found. Tried: $candidates")
        return schema!!
    }
}
