package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.model.AnnotatedExecuteStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.SelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedSelectStatement
import dev.goquick.sqlitenow.gradle.model.AnnotatedStatement
import dev.goquick.sqlitenow.gradle.processing.PropertyNameGeneratorType
import dev.goquick.sqlitenow.gradle.processing.SharedResultTypeUtils
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.sqlinspect.InsertStatement
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName

class SharedResultTypeUtilsTest {

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct shared result type names")
    fun testCreateSharedResultTypeName() {
        val typeName = SharedResultTypeUtils.createSharedResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            sharedResultName = "All"
        )

        assertEquals("com.example.db.PersonQuery.SharedResult.All", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type names")
    fun testCreateResultTypeName() {
        val cases = listOf(
            ResultNameCase(
                statement = resultNameStatement(name = "SelectAll", queryResult = "All"),
                expectedTypeName = "com.example.db.All"
            ),
            ResultNameCase(
                statement = resultNameStatement(
                    name = "SelectWeird",
                    sql = "SELECT * FROM Person WHERE weird = 1",
                    queryResult = null,
                ),
                expectedTypeName = "com.example.db.PersonSelectWeirdResult"
            ),
            ResultNameCase(
                statement = executeResultNameStatement(name = "Add", queryResult = "Added"),
                expectedTypeName = "com.example.db.Added"
            ),
            ResultNameCase(
                statement = executeResultNameStatement(name = "Add", queryResult = null),
                expectedTypeName = "com.example.db.PersonAddResult"
            )
        )

        cases.forEach { case ->
            val typeName = SharedResultTypeUtils.createResultTypeName(
                packageName = "com.example.db",
                namespace = "person",
                statement = case.statement
            )

            assertEquals(case.expectedTypeName, typeName.toString())
        }
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type strings")
    fun testCreateResultTypeString() {
        val sharedStatement = resultNameStatement(name = "SelectAll", queryResult = "All")

        val sharedResultString = SharedResultTypeUtils.createResultTypeString("person", sharedStatement)
        assertEquals("All", sharedResultString)

        val regularStatement = resultNameStatement(
            name = "SelectWeird",
            sql = "SELECT * FROM Person WHERE weird = 1",
            queryResult = null,
        )

        val regularResultString = SharedResultTypeUtils.createResultTypeString("person", regularStatement)
        assertEquals("PersonSelectWeirdResult", regularResultString)

        val executeResultString = SharedResultTypeUtils.createResultTypeString(
            "person",
            executeResultNameStatement(name = "Add", queryResult = null)
        )
        assertEquals("PersonAddResult", executeResultString)
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates public result types with mapTo overrides")
    fun testCreatePublicResultTypeName() {
        val statement = resultNameStatement(
            name = "SelectSummary",
            queryResult = null,
            mapTo = "fixture.model.PersonSummary",
        )

        val typeName = SharedResultTypeUtils.createPublicResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = statement,
        )
        assertEquals("fixture.model.PersonSummary", typeName.toString())

        val typeString = SharedResultTypeUtils.createPublicResultTypeString(
            namespace = "person",
            statement = statement,
        )
        assertEquals("fixture.model.PersonSummary", typeString)
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct joined result type names")
    fun testCreateJoinedResultTypeName() {
        val joinedTypeName = SharedResultTypeUtils.createJoinedResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = resultNameStatement(name = "SelectAll", queryResult = "All")
        )
        assertEquals("com.example.db.All_Joined", joinedTypeName.toString())

        val joinedTypeString = SharedResultTypeUtils.createJoinedResultTypeString(
            namespace = "person",
            statement = resultNameStatement(
                name = "SelectWeird",
                sql = "SELECT * FROM Person WHERE weird = 1",
                queryResult = null,
            )
        )
        assertEquals("PersonSelectWeirdResult_Joined", joinedTypeString)
    }

    @Test
    @DisplayName("Test that SHARED_RESULT_OBJECT_NAME constant is used consistently")
    fun testSharedResultObjectNameConstant() {
        assertEquals("SharedResult", SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME)

        // Verify that changing the constant would affect all generated type names
        val typeName = SharedResultTypeUtils.createSharedResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            sharedResultName = "All"
        )

        assertTrue(typeName.toString().contains(SharedResultTypeUtils.SHARED_RESULT_OBJECT_NAME))
    }

    private data class ResultNameCase(
        val statement: AnnotatedStatement,
        val expectedTypeName: String,
    )

    private fun resultNameStatement(
        name: String,
        queryResult: String?,
        sql: String = "SELECT * FROM Person",
        mapTo: String? = null,
    ): AnnotatedSelectStatement = AnnotatedSelectStatement(
        name = name,
        src = SelectStatement(
            sql = sql,
            fromTable = "person",
            joinTables = emptyList(),
            namedParameters = emptyList(),
            namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
            fields = emptyList()
        ),
        annotations = StatementAnnotationOverrides(
            name = null,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            queryResult = queryResult,
            collectionKey = null,
            mapTo = mapTo,
        ),
        fields = emptyList()
    )

    private fun executeResultNameStatement(
        name: String,
        queryResult: String?,
    ): AnnotatedExecuteStatement = AnnotatedExecuteStatement(
        name = name,
        src = InsertStatement(
            sql = "INSERT INTO Person DEFAULT VALUES RETURNING id",
            table = "person",
            namedParameters = emptyList(),
            columnNamesAssociatedWithNamedParameters = emptyMap(),
            withSelectStatements = emptyList(),
            parameterCastTypes = emptyMap(),
            hasReturningClause = true,
            returningColumns = listOf("id"),
        ),
        annotations = StatementAnnotationOverrides(
            name = null,
            propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
            queryResult = queryResult,
            collectionKey = null
        )
    )
}
