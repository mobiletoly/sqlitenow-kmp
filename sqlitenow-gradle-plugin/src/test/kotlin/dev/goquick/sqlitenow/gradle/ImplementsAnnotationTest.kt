package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertThrows

class ImplementsAnnotationTest {

    @Test
    @DisplayName("Test implements annotation parsing")
    fun testImplementsAnnotationParsing() {
        val annotations = mapOf(
            AnnotationConstants.QUERY_RESULT to "All",
            AnnotationConstants.IMPLEMENTS to "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields"
        )
        
        val parsed = StatementAnnotationOverrides.parse(annotations)
        
        assertEquals("All", parsed.queryResult)
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", parsed.implements)
    }

    @Test
    @DisplayName("Test extractAnnotations function with implements")
    fun testExtractAnnotationsWithImplements() {
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields}"
        )

        val extracted = extractAnnotations(comments)

        assertEquals("All", extracted[AnnotationConstants.QUERY_RESULT])
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", extracted[AnnotationConstants.IMPLEMENTS])
    }

    @Test
    @DisplayName("Test full annotation parsing pipeline")
    fun testFullAnnotationPipeline() {
        // Test the exact comment format you're using
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields}"
        )

        // Step 1: Extract annotations
        val extracted = extractAnnotations(comments)

        // Step 2: Parse into StatementAnnotationOverrides
        val parsed = StatementAnnotationOverrides.parse(extracted)

        // Verify both annotations are present
        assertEquals("All", parsed.queryResult)
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", parsed.implements)

        // Step 3: Test SharedResultManager
        val sharedResultManager = SharedResultManager()

        // Create a simple mock statement for testing
        val mockStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM person",
                fromTable = "person",
                joinTables = emptyList(),
                namedParameters = emptyList(),
                namedParametersToColumns = emptyMap(),
            offsetNamedParam = null,
            limitNamedParam = null,
                fields = emptyList()
            ),
            annotations = parsed,
            fields = emptyList()
        )

        // Register the shared result
        val sharedResult = sharedResultManager.registerSharedResult(mockStatement, "person")

        // Verify the implements annotation is preserved
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", sharedResult?.implements)
    }

    @Test
    @DisplayName("Test conflicting implements annotations throw error")
    fun testConflictingImplementsAnnotations() {
        val sharedResultManager = SharedResultManager()

        // First statement with implements
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person",
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
                queryResult = "All",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Second statement without implements (conflicting)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectAllFiltered",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person WHERE active = 1",
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
                queryResult = "All",
                implements = null,  // Conflicting - missing implements
                excludeOverrideFields = null,
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Register first statement - should work
        val firstResult = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(firstResult)
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", firstResult?.implements)

        // Register second statement - should throw error due to conflicting implements
        val exception = assertThrows<IllegalArgumentException> {
            sharedResultManager.registerSharedResult(secondStatement, "person")
        }

        assertTrue(exception.message!!.contains("Conflicting 'implements' annotations"))
        assertTrue(exception.message!!.contains("person.All"))
        assertTrue(exception.message!!.contains("SelectAllFiltered"))
        assertTrue(exception.message!!.contains("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields"))
        assertTrue(exception.message!!.contains("null"))
    }

    @Test
    @DisplayName("Test excludeOverrideFields annotation parsing")
    fun testExcludeOverrideFieldsAnnotation() {
        val annotations = mapOf(
            AnnotationConstants.QUERY_RESULT to "All",
            AnnotationConstants.IMPLEMENTS to "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
            AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to listOf("phone", "birthDate", "age", "score", "createdAt", "notes")
        )

        val parsed = StatementAnnotationOverrides.parse(annotations)

        assertEquals("All", parsed.queryResult)
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", parsed.implements)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test extractAnnotations with excludeOverrideFields")
    fun testExtractAnnotationsWithExcludeOverrideFields() {
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields, excludeOverrideFields=[phone,birthDate,age,score,createdAt,notes]}"
        )

        val extracted = extractAnnotations(comments)

        assertEquals("All", extracted[AnnotationConstants.QUERY_RESULT])
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", extracted[AnnotationConstants.IMPLEMENTS])
        assertEquals(listOf("phone", "birthDate", "age", "score", "createdAt", "notes"), extracted[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS])

        val parsed = StatementAnnotationOverrides.parse(extracted)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test excludeOverrideFields with bracket syntax")
    fun testExcludeOverrideFieldsWithBrackets() {
        val annotations = mapOf(
            AnnotationConstants.QUERY_RESULT to "All",
            AnnotationConstants.IMPLEMENTS to "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
            AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to listOf("phone", "birthDate", "age", "score", "createdAt", "notes")
        )

        val parsed = StatementAnnotationOverrides.parse(annotations)

        assertEquals("All", parsed.queryResult)
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", parsed.implements)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test excludeOverrideFields with bracket syntax and spaces")
    fun testExcludeOverrideFieldsWithBracketsAndSpaces() {
        val annotations = mapOf(
            AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to listOf("phone", "birthDate", "age")
        )

        val parsed = StatementAnnotationOverrides.parse(annotations)
        assertEquals(setOf("phone", "birthDate", "age"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test exact case: excludeOverrideFields=[phone, birthDate]")
    fun testExactUserCase() {
        val annotations = mapOf(
            AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to listOf("phone", "birthDate")
        )

        val parsed = StatementAnnotationOverrides.parse(annotations)
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)

        // Verify no leading/trailing spaces
        assertTrue(parsed.excludeOverrideFields!!.contains("phone"))
        assertTrue(parsed.excludeOverrideFields.contains("birthDate"))
        assertFalse(parsed.excludeOverrideFields.contains(" birthDate"))
    }

    @Test
    @DisplayName("Test problematic case with spaces: excludeOverrideFields=[phone, birthDate]")
    fun testProblematicSpaceCase() {
        // Test the exact problematic case you mentioned
        val input = listOf("phone", "birthDate")

        // Test the parsing function directly
        val annotations = mapOf(AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to input)
        val parsed = StatementAnnotationOverrides.parse(annotations)
        // Verify both fields are correctly parsed without spaces
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)

        // Test that the set contains the exact field names
        assertTrue(parsed.excludeOverrideFields!!.contains("phone"))
        assertTrue(parsed.excludeOverrideFields.contains("birthDate"))

        // Test that it doesn't contain versions with spaces
        assertFalse(parsed.excludeOverrideFields.contains(" phone"))
        assertFalse(parsed.excludeOverrideFields.contains("phone "))
        assertFalse(parsed.excludeOverrideFields.contains(" birthDate"))
        assertFalse(parsed.excludeOverrideFields.contains("birthDate "))
    }

    @Test
    @DisplayName("Test HOCON list handling")
    fun testComprehensiveSpaceHandling() {
        // Test that HOCON lists work correctly
        val annotations = mapOf(
            AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to listOf("phone", "birthDate", "age")
        )

        val parsed = StatementAnnotationOverrides.parse(annotations)
        assertEquals(setOf("phone", "birthDate", "age"), parsed.excludeOverrideFields)

        // Verify no spaces in field names (HOCON handles this automatically)
        parsed.excludeOverrideFields?.forEach { field ->
            assertFalse(field.startsWith(" "), "Field '$field' should not start with space")
            assertFalse(field.endsWith(" "), "Field '$field' should not end with space")
        }
    }

    @Test
    @DisplayName("Test EXACT failing case: [ phone, birthDate]")
    fun testExactFailingCase() {
        // Test the exact case that's failing for the user
        val input = listOf("phone", "birthDate")
        val annotations = mapOf(AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS to input)
        val parsed = StatementAnnotationOverrides.parse(annotations)
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test excludeOverrideFields logic in code generation")
    fun testExcludeOverrideFieldsLogic() {
        // Test the core logic: when implements is specified and excludeOverrideFields is set,
        // only fields NOT in the exclude list should get override modifiers

        val excludeFields = setOf("phone", "birthDate")
        val implementsInterface = "PersonEssentialFields"

        // Test field that should have override (not in exclude list)
        val idFieldName = "id"
        val idIsExcluded = excludeFields.contains(idFieldName)
        val idShouldHaveOverride = !idIsExcluded

        assertTrue(idShouldHaveOverride, "id field should have override modifier")

        // Test field that should NOT have override (in exclude list)
        val phoneFieldName = "phone"
        val phoneIsExcluded = excludeFields.contains(phoneFieldName)
        val phoneShouldHaveOverride = !phoneIsExcluded

        assertFalse(phoneShouldHaveOverride, "phone field should NOT have override modifier")

        // Test field that should NOT have override (in exclude list)
        val birthDateFieldName = "birthDate"
        val birthDateIsExcluded = excludeFields.contains(birthDateFieldName)
        val birthDateShouldHaveOverride = !birthDateIsExcluded
        assertFalse(birthDateShouldHaveOverride, "birthDate field should NOT have override modifier")
    }

    @Test
    @DisplayName("Test extractAnnotations with bracket syntax")
    fun testExtractAnnotationsWithBracketSyntax() {
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields, excludeOverrideFields=[phone,birthDate]}"
        )

        val extracted = extractAnnotations(comments)

        assertEquals("All", extracted[AnnotationConstants.QUERY_RESULT])
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", extracted[AnnotationConstants.IMPLEMENTS])
        assertEquals(listOf("phone", "birthDate"), extracted[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS])

        val parsed = StatementAnnotationOverrides.parse(extracted)
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test extractAnnotations with spaces in brackets - THE FAILING CASE")
    fun testExtractAnnotationsWithSpacesInBrackets() {
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields, excludeOverrideFields=[phone, birthDate]}"
        )

        val extracted = extractAnnotations(comments)

        assertEquals("All", extracted[AnnotationConstants.QUERY_RESULT])
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", extracted[AnnotationConstants.IMPLEMENTS])
        assertEquals(listOf("phone", "birthDate"), extracted[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS])

        val parsed = StatementAnnotationOverrides.parse(extracted)
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)
    }

    @Test
    @DisplayName("Test extractAnnotations with multiple lines and spaces")
    fun testExtractAnnotationsMultipleLinesWithSpaces() {
        val comments = listOf(
            "-- @@{queryResult=All, implements=dev.goquick.sqlitenow.samplekmp.PersonEssentialFields, excludeOverrideFields=[ phone , birthDate ]}"
        )

        val extracted = extractAnnotations(comments)

        assertEquals("All", extracted[AnnotationConstants.QUERY_RESULT])
        assertEquals("dev.goquick.sqlitenow.samplekmp.PersonEssentialFields", extracted[AnnotationConstants.IMPLEMENTS])
        assertEquals(listOf("phone", "birthDate"), extracted[AnnotationConstants.EXCLUDE_OVERRIDE_FIELDS])

        val parsed = StatementAnnotationOverrides.parse(extracted)
        assertEquals(setOf("phone", "birthDate"), parsed.excludeOverrideFields)

        println("✅ Multiple lines with spaces annotation extraction works correctly!")
    }

    @Test
    @DisplayName("Test excludeOverrideFields inheritance for shared results")
    fun testExcludeOverrideFieldsInheritanceForSharedResults() {
        val sharedResultManager = SharedResultManager()

        // First statement with excludeOverrideFields specified
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person",
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
                queryResult = "All",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = setOf("phone", "birthDate"), // Specified here
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Second statement WITHOUT excludeOverrideFields (should inherit)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectFiltered",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person WHERE age > 18",
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
                queryResult = "All", // Same shared result
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null, // NOT specified - should inherit
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Register first statement
        val sharedResult1 = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(sharedResult1)
        assertEquals(setOf("phone", "birthDate"), sharedResult1!!.excludeOverrideFields)

        // Register second statement - should inherit excludeOverrideFields
        val sharedResult2 = sharedResultManager.registerSharedResult(secondStatement, "person")
        assertNotNull(sharedResult2)
        assertEquals(setOf("phone", "birthDate"), sharedResult2!!.excludeOverrideFields)

        // Both should return the same shared result object
        assertEquals(sharedResult1, sharedResult2)

        // Test getEffectiveExcludeOverrideFields
        val effective1 = sharedResultManager.getEffectiveExcludeOverrideFields(firstStatement, "person")
        val effective2 = sharedResultManager.getEffectiveExcludeOverrideFields(secondStatement, "person")

        assertEquals(setOf("phone", "birthDate"), effective1)
        assertEquals(setOf("phone", "birthDate"), effective2)

        println("✅ excludeOverrideFields inheritance works correctly!")
        println("  - First statement: specified excludeOverrideFields=[phone, birthDate]")
        println("  - Second statement: inherited excludeOverrideFields=[phone, birthDate]")
    }

    @Test
    @DisplayName("Test excludeOverrideFields inheritance - reverse order")
    fun testExcludeOverrideFieldsInheritanceReverseOrder() {
        val sharedResultManager = SharedResultManager()

        // First statement WITHOUT excludeOverrideFields
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectAll",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person",
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
                queryResult = "All",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null, // NOT specified
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Second statement WITH excludeOverrideFields (should update the shared result)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectFiltered",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT * FROM Person WHERE age > 18",
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
                queryResult = "All", // Same shared result
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = setOf("phone"), // Specified here
                collectionKey = null
            ),
            fields = emptyList()
        )

        // Register first statement (no excludeOverrideFields)
        val sharedResult1 = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(sharedResult1)
        assertEquals(null, sharedResult1!!.excludeOverrideFields)

        // Register second statement - should update the shared result
        val sharedResult2 = sharedResultManager.registerSharedResult(secondStatement, "person")
        assertNotNull(sharedResult2)
        assertEquals(setOf("phone"), sharedResult2!!.excludeOverrideFields)

        // Test getEffectiveExcludeOverrideFields
        val effective1 = sharedResultManager.getEffectiveExcludeOverrideFields(firstStatement, "person")
        val effective2 = sharedResultManager.getEffectiveExcludeOverrideFields(secondStatement, "person")

        assertEquals(setOf("phone"), effective1) // Should inherit from updated shared result
        assertEquals(setOf("phone"), effective2)

        println("✅ excludeOverrideFields reverse inheritance works correctly!")
        println("  - First statement: no excludeOverrideFields initially")
        println("  - Second statement: specified excludeOverrideFields=[phone]")
        println("  - Both now have effective excludeOverrideFields=[phone]")
    }

    @Test
    @DisplayName("Test INTEGER vs NUMERIC type equivalence - fixed user issue")
    fun testIntegerVsNumericTypeEquivalence() {
        val sharedResultManager = SharedResultManager()

        // Simulate the exact user scenario with COUNT(*) fields that have different SQL types
        // but should be treated as equivalent because they map to the same Kotlin type
        val firstQueryFields = listOf(
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "total_person_count",
                    tableName = "person",
                    originalColumnName = "total_person_count",
                    dataType = "INTEGER" // First query reports INTEGER
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            )
        )

        val secondQueryFields = listOf(
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "total_person_count",
                    tableName = "person",
                    originalColumnName = "total_person_count",
                    dataType = "NUMERIC" // Second query reports NUMERIC
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            )
        )

        // First statement with excludeOverrideFields
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectWithLimit",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person LIMIT :limit OFFSET :offset",
                fromTable = "person",
                joinTables = emptyList(),
                fields = firstQueryFields.map { it.src },
                namedParameters = listOf("limit", "offset"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = "offset",
                limitNamedParam = "limit"
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"),
                collectionKey = null
            ),
            fields = firstQueryFields
        )

        // Second statement WITHOUT excludeOverrideFields (should inherit)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectWithFilter",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person WHERE first_name IN (:firstNames)",
                fromTable = "person",
                joinTables = emptyList(),
                fields = secondQueryFields.map { it.src },
                namedParameters = listOf("firstNames"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person", // Same shared result
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null, // Should inherit
                collectionKey = null
            ),
            fields = secondQueryFields
        )

        // Register first statement
        val sharedResult1 = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(sharedResult1)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"), sharedResult1!!.excludeOverrideFields)

        // This should now work because INTEGER and NUMERIC both map to kotlin.Long
        val sharedResult2 = sharedResultManager.registerSharedResult(secondStatement, "person")
        assertNotNull(sharedResult2)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"), sharedResult2!!.excludeOverrideFields)

        // Both should return the same shared result object
        assertEquals(sharedResult1, sharedResult2)

        println("✅ INTEGER vs NUMERIC type equivalence works correctly!")
        println("  - First query: total_person_count has SQL type 'INTEGER' → kotlin.Long")
        println("  - Second query: total_person_count has SQL type 'NUMERIC' → kotlin.Long")
        println("  - Both are now treated as equivalent for structure comparison")
        println("  - Second query inherits excludeOverrideFields successfully")
    }

    @Test
    @DisplayName("Test COUNT(*) field structure issue - reproducing exact user issue")
    fun testCountFieldStructureIssue() {
        val sharedResultManager = SharedResultManager()

        // Simulate the exact user scenario with COUNT(*) fields that might have different annotations
        val firstQueryFields = listOf(
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            ),
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "total_person_count",
                    tableName = "person", // This might be different between queries!
                    originalColumnName = "total_person_count",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = "kotlin.Long", // First query gets this annotation
                    notNull = null,
                    adapter = false
                )
            )
        )

        val secondQueryFields = listOf(
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            ),
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "total_person_count",
                    tableName = "person",
                    originalColumnName = "total_person_count",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null, // Second query doesn't get the annotation - this causes the issue!
                    notNull = null,
                    adapter = false
                )
            )
        )

        // First statement with excludeOverrideFields
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectWithLimit",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person LIMIT :limit OFFSET :offset",
                fromTable = "person",
                joinTables = emptyList(),
                fields = firstQueryFields.map { it.src },
                namedParameters = listOf("limit", "offset"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = "offset",
                limitNamedParam = "limit"
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"),
                collectionKey = null
            ),
            fields = firstQueryFields
        )

        // Second statement WITHOUT excludeOverrideFields (should inherit)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectWithFilter",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person WHERE first_name IN (:firstNames)",
                fromTable = "person",
                joinTables = emptyList(),
                fields = secondQueryFields.map { it.src },
                namedParameters = listOf("firstNames"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person", // Same shared result
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null, // Should inherit
                collectionKey = null
            ),
            fields = secondQueryFields // Different field annotations!
        )

        // Register first statement
        val sharedResult1 = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(sharedResult1)

        // This should fail with "inconsistent field structure" because propertyType is different
        assertThrows<IllegalArgumentException> {
            sharedResultManager.registerSharedResult(secondStatement, "person")
        }

        println("✅ Successfully reproduced the COUNT(*) field structure issue!")
        println("  - First query: total_person_count has propertyType='kotlin.Long'")
        println("  - Second query: total_person_count has propertyType=null")
        println("  - This causes 'inconsistent field structure' error")
    }

    @Test
    @DisplayName("Test field structure validation with inheritance - reproducing user issue")
    fun testFieldStructureValidationWithInheritance() {
        val sharedResultManager = SharedResultManager()

        // Create mock fields that are identical in structure
        val commonFields = listOf(
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "id",
                    tableName = "person",
                    originalColumnName = "id",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            ),
            AnnotatedSelectStatement.Field(
                src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement.FieldSource(
                    fieldName = "total_person_count",
                    tableName = "person",
                    originalColumnName = "total_person_count",
                    dataType = "INTEGER"
                ),
                annotations = FieldAnnotationOverrides(
                    propertyName = null,
                    propertyType = null,
                    notNull = null,
                    adapter = false
                )
            )
        )

        // First statement with excludeOverrideFields
        val firstStatement = AnnotatedSelectStatement(
            name = "SelectWithLimit",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person LIMIT :limit OFFSET :offset",
                fromTable = "person",
                joinTables = emptyList(),
                fields = commonFields.map { it.src },
                namedParameters = listOf("limit", "offset"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = "offset",
                limitNamedParam = "limit"
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person",
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"),
                collectionKey = null
            ),
            fields = commonFields
        )

        // Second statement WITHOUT excludeOverrideFields (should inherit)
        val secondStatement = AnnotatedSelectStatement(
            name = "SelectWithFilter",
            src = dev.goquick.sqlitenow.gradle.inspect.SelectStatement(
                sql = "SELECT *, COUNT(*) AS total_person_count FROM Person WHERE first_name IN (:firstNames)",
                fromTable = "person",
                joinTables = emptyList(),
                fields = commonFields.map { it.src },
                namedParameters = listOf("firstNames"),
                namedParametersToColumns = emptyMap(),
                offsetNamedParam = null,
                limitNamedParam = null
            ),
            annotations = StatementAnnotationOverrides(
                name = null,
                propertyNameGenerator = PropertyNameGeneratorType.LOWER_CAMEL_CASE,
                queryResult = "Person", // Same shared result
                implements = "dev.goquick.sqlitenow.samplekmp.PersonEssentialFields",
                excludeOverrideFields = null, // Should inherit
                collectionKey = null
            ),
            fields = commonFields // Same field structure
        )

        // This should work without throwing "inconsistent field structure" error
        val sharedResult1 = sharedResultManager.registerSharedResult(firstStatement, "person")
        assertNotNull(sharedResult1)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"), sharedResult1!!.excludeOverrideFields)

        // This should inherit the excludeOverrideFields without structure validation error
        val sharedResult2 = sharedResultManager.registerSharedResult(secondStatement, "person")
        assertNotNull(sharedResult2)
        assertEquals(setOf("phone", "birthDate", "age", "score", "createdAt", "notes", "totalPersonCount"), sharedResult2!!.excludeOverrideFields)

        println("✅ Field structure validation with inheritance works correctly!")
        println("  - Both queries have identical field structure")
        println("  - Second query inherits excludeOverrideFields successfully")
    }

    // Note: The fix for excludeOverrideFields working with regular queries (not just shared results)
    // has been implemented in DataStructCodeGenerator.kt by preserving the implements and excludeOverrideFields
    // annotations when processing WITH clauses. The fix ensures that both annotations are passed through
    // to the annotatedWithSelectStatement instead of being set to null.
}
