package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.inspect.SelectStatement
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
        
        assertEquals("com.example.db.Person.SharedResult.All", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type names for shared results")
    fun testCreateResultTypeNameWithSharedResult() {
        val statement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Person",
                fromTable = "Person",
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
                sharedResult = "All",
                implements = null,
                excludeOverrideFields = null
            ),
            fields = emptyList()
        )
        
        val typeName = SharedResultTypeUtils.createResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = statement
        )
        
        assertEquals("com.example.db.Person.SharedResult.All", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type names for regular results")
    fun testCreateResultTypeNameWithRegularResult() {
        val statement = AnnotatedSelectStatement(
            name = "SelectWeird",
            src = SelectStatement(
                sql = "SELECT * FROM Person WHERE weird = 1",
                fromTable = "Person",
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
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            fields = emptyList()
        )
        
        val typeName = SharedResultTypeUtils.createResultTypeName(
            packageName = "com.example.db",
            namespace = "person",
            statement = statement
        )
        
        assertEquals("com.example.db.Person.SelectWeird.Result", typeName.toString())
    }

    @Test
    @DisplayName("Test SharedResultTypeUtils creates correct result type strings")
    fun testCreateResultTypeString() {
        val sharedStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = SelectStatement(
                sql = "SELECT * FROM Person",
                fromTable = "Person",
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
                sharedResult = "All",
                implements = null,
                excludeOverrideFields = null
            ),
            fields = emptyList()
        )
        
        val sharedResultString = SharedResultTypeUtils.createResultTypeString("person", sharedStatement)
        assertEquals("Person.SharedResult.All", sharedResultString)
        
        val regularStatement = AnnotatedSelectStatement(
            name = "SelectWeird",
            src = SelectStatement(
                sql = "SELECT * FROM Person WHERE weird = 1",
                fromTable = "Person",
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
                sharedResult = null,
                implements = null,
                excludeOverrideFields = null
            ),
            fields = emptyList()
        )
        
        val regularResultString = SharedResultTypeUtils.createResultTypeString("person", regularStatement)
        assertEquals("Person.SelectWeird.Result", regularResultString)
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
}
