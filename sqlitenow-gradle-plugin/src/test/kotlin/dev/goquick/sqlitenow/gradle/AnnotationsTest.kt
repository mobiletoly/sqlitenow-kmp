package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.SqliteTypeToKotlinCodeConverter.Companion.KOTLIN_STDLIB_TYPES
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class AnnotationsTest {

    @Test
    fun `test basic annotation syntax`() {
        val comment = """-- @@{field=birth_date, adapter="", propertyType=kotlinx.datetime.LocalDate}"""
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("birth_date", annotations["field"])
        assertEquals("", annotations["adapter"]) // Empty string stays empty string
        assertEquals("kotlinx.datetime.LocalDate", annotations["propertyType"])
    }

    @Test
    fun `test multiline annotation syntax`() {
        val comments = listOf(
            """-- @@{field=created_at, adapter="",""",
            "--     propertyType=kotlinx.datetime.LocalDateTime",
            "-- }"
        )
        val annotations = extractAnnotations(comments)

        assertEquals("created_at", annotations["field"])
        assertEquals("", annotations["adapter"])
        assertEquals("kotlinx.datetime.LocalDateTime", annotations["propertyType"])
    }

    @Test
    fun `test annotation syntax with spaces`() {
        val comment = """-- @@{ field = birth_date , adapter = "" , propertyType = kotlinx.datetime.LocalDate }"""
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("birth_date", annotations["field"])
        assertEquals("", annotations["adapter"])
        assertEquals("kotlinx.datetime.LocalDate", annotations["propertyType"])
    }

    @Test
    fun `test annotation syntax with array values`() {
        val comment = "-- @@{field=tags, propertyType=List<String>, excludeOverrideFields=[id, name]}"
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("tags", annotations["field"])
        assertEquals("List<String>", annotations["propertyType"])
        assertEquals(listOf("id", "name"), annotations["excludeOverrideFields"])
    }

    @Test
    fun `test annotation syntax with quoted values`() {
        val comment = """-- @@{field="user name", propertyType="String?"}"""
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("user name", annotations["field"]) // HOCON unquotes strings
        assertEquals("String?", annotations["propertyType"]) // HOCON unquotes strings
    }

    @Test
    fun `test empty annotation block`() {
        val comment = "-- @@{}"
        val annotations = extractAnnotations(listOf(comment))

        assertEquals(0, annotations.size)
    }

    @Test
    fun `test single annotation without value`() {
        val comment = """-- @@{adapter=""}"""
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("", annotations["adapter"])
    }

    @Test
    fun `test single annotation with value`() {
        val comment = "-- @@{field=user_id}"
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("user_id", annotations["field"])
    }

    @Test
    fun `test explicit null values`() {
        val comment = "-- @@{field=user_id, adapter=null, propertyType=\"String?\"}"
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("user_id", annotations["field"])
        assertEquals(null, annotations["adapter"]) // Explicit null
        assertEquals("String?", annotations["propertyType"])
    }

    @Test
    fun `test empty vs null distinction`() {
        val comment = """-- @@{field=user_id, emptyValue="", nullValue=null}"""
        val annotations = extractAnnotations(listOf(comment))

        assertEquals("user_id", annotations["field"])
        assertEquals("", annotations["emptyValue"]) // Empty string
        assertEquals(null, annotations["nullValue"]) // Explicit null
    }

    @Test
    fun `test multiple annotation blocks`() {
        val comments = listOf(
            "-- @@{field=first_name, propertyName=myFirstName}",
            "-- @@{field=last_name, propertyName=myLastName}",
            "-- @@{field=birth_date, adapter=custom, propertyType=kotlinx.datetime.LocalDate}"
        )

        val fieldAnnotations = extractFieldAssociatedAnnotations(comments)

        assertEquals(3, fieldAnnotations.size)

        // Check first_name annotations
        val firstNameAnnotations = fieldAnnotations["first_name"]
        assertEquals("myFirstName", firstNameAnnotations?.get("propertyName"))

        // Check last_name annotations
        val lastNameAnnotations = fieldAnnotations["last_name"]
        assertEquals("myLastName", lastNameAnnotations?.get("propertyName"))

        // Check birth_date annotations
        val birthDateAnnotations = fieldAnnotations["birth_date"]
        assertEquals("custom", birthDateAnnotations?.get("adapter"))
        assertEquals("kotlinx.datetime.LocalDate", birthDateAnnotations?.get("propertyType"))
    }

    @Test
    fun `test field-associated annotations`() {
        val comments = listOf(
            """-- @@{field=user_id, nullable="", default=0}"""
        )

        val result = extractFieldAssociatedAnnotations(comments)

        // Check the number of fields
        assertEquals(1, result.size)

        // Check user_id field annotations
        val userIdAnnotations = result["user_id"]
        assertEquals(2, userIdAnnotations?.size)
        assertEquals("", userIdAnnotations?.get("nullable"))
        assertEquals("0", userIdAnnotations?.get("default"))
    }

    @Test
    fun `test real-world CREATE TABLE scenario`() {
        // This matches the exact scenario from the troubleshooting case
        val comments = listOf(
            "-- @@{ field=first_name, propertyName=myFirstName }",
            "-- @@{ field=last_name, propertyName=myLastName }",
            "-- @@{",
            "-- field=birth_date, adapter=custom,",
            "-- propertyType=kotlinx.datetime.LocalDate",
            "-- }",
            "-- @@{ field = created_at, adapter=custom, propertyType=kotlinx.datetime.LocalDateTime }",
            "-- @@{ field=notes, adapter=custom, propertyType=dev.goquick.sqlitenow.samplekmp.model.PersonNote }"
        )

        val fieldAnnotations = extractFieldAssociatedAnnotations(comments)

        // Should have 5 fields with annotations
        assertEquals(5, fieldAnnotations.size)

        // Verify each field has the correct annotations
        assertEquals("myFirstName", fieldAnnotations["first_name"]?.get("propertyName"))
        assertEquals("myLastName", fieldAnnotations["last_name"]?.get("propertyName"))

        val birthDateAnnotations = fieldAnnotations["birth_date"]
        assertEquals("custom", birthDateAnnotations?.get("adapter"))
        assertEquals("kotlinx.datetime.LocalDate", birthDateAnnotations?.get("propertyType"))

        val createdAtAnnotations = fieldAnnotations["created_at"]
        assertEquals("custom", createdAtAnnotations?.get("adapter"))
        assertEquals("kotlinx.datetime.LocalDateTime", createdAtAnnotations?.get("propertyType"))

        val notesAnnotations = fieldAnnotations["notes"]
        assertEquals("custom", notesAnnotations?.get("adapter"))
        assertEquals("dev.goquick.sqlitenow.samplekmp.model.PersonNote", notesAnnotations?.get("propertyType"))
    }

    @Test
    fun `test adapter inference for built-in types`() {
        // Built-in types should NOT need adapters (adapter=false)
        val builtInTypes = KOTLIN_STDLIB_TYPES

        builtInTypes.forEach { type ->
            val annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to type)
            val parsed = FieldAnnotationOverrides.parse(annotations)
            if (parsed.adapter != false) {
                println("Adapter for $type: ${parsed.adapter}")
            }
            assertEquals(false, parsed.adapter, "Built-in type $type should not need adapter")
        }
    }

    @Test
    fun `test adapter inference for custom types`() {
        // Custom types should need adapters (adapter=true)
        val customTypes = listOf(
            "kotlinx.datetime.LocalDate",
            "dev.example.CustomType",
            "List<String>",
            "MyClass"
        )

        customTypes.forEach { type ->
            val annotations = mapOf(AnnotationConstants.PROPERTY_TYPE to type)
            val parsed = FieldAnnotationOverrides.parse(annotations)
            assertEquals(true, parsed.adapter, "Custom type $type should need adapter")
        }
    }

    @Test
    fun `test explicit adapter overrides inference`() {
        // Explicit adapter should override inference
        val annotations = mapOf(
            AnnotationConstants.PROPERTY_TYPE to "kotlin.String", // Built-in type
            AnnotationConstants.ADAPTER to "custom" // Explicit adapter
        )

        val parsed = FieldAnnotationOverrides.parse(annotations)
        assertEquals(true, parsed.adapter, "Explicit adapter should override inference")
    }

    @Test
    fun `test no propertyType means no adapter`() {
        // No propertyType should mean no adapter
        val annotations = emptyMap<String, Any?>()
        val parsed = FieldAnnotationOverrides.parse(annotations)
        assertEquals(false, parsed.adapter, "No propertyType should mean no adapter")
    }

    @Test
    fun `test CREATE TABLE scenario - custom type without explicit adapter`() {
        // This simulates: @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}
        // Should automatically infer adapter=true for custom type
        val annotations = mapOf(
            AnnotationConstants.FIELD to "birth_date",
            AnnotationConstants.PROPERTY_TYPE to "kotlinx.datetime.LocalDate"
        )

        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}"
        ))

        val birthDateAnnotations = fieldAnnotations["birth_date"]
        assertEquals(true, birthDateAnnotations != null, "birth_date annotations should exist")
        assertEquals("kotlinx.datetime.LocalDate", birthDateAnnotations?.get("propertyType"))

        // Parse the annotations to check adapter inference
        val parsed = FieldAnnotationOverrides.parse(birthDateAnnotations!!)
        assertEquals(true, parsed.adapter, "Custom type should automatically infer adapter=true")
        assertEquals("kotlinx.datetime.LocalDate", parsed.propertyType)
    }

    @Test
    fun `test CREATE TABLE scenario - built-in type without explicit adapter`() {
        // This simulates: @@{field=name, propertyType=kotlin.String}
        // Should automatically infer adapter=false for built-in type
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=name, propertyType=kotlin.String}"
        ))

        val nameAnnotations = fieldAnnotations["name"]
        assertEquals(true, nameAnnotations != null, "name annotations should exist")
        assertEquals("kotlin.String", nameAnnotations?.get("propertyType"))

        // Parse the annotations to check adapter inference
        val parsed = FieldAnnotationOverrides.parse(nameAnnotations!!)
        assertEquals(false, parsed.adapter, "Built-in type should automatically infer adapter=false")
        assertEquals("kotlin.String", parsed.propertyType)
    }

    @Test
    fun `test adapter key added to raw annotations map for custom types`() {
        // This tests that the ADAPTER key is actually added to the raw annotations map
        // which is what AdapterConfig.hasAdapterAnnotation() checks
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}"
        ))

        val birthDateAnnotations = fieldAnnotations["birth_date"]
        assertEquals(true, birthDateAnnotations != null, "birth_date annotations should exist")

        // Check that the ADAPTER key was automatically added to the raw annotations map
        assertEquals(true, birthDateAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "ADAPTER key should be automatically added for custom types")
        assertEquals("custom", birthDateAnnotations[AnnotationConstants.ADAPTER],
            "ADAPTER value should be custom")
        assertEquals("kotlinx.datetime.LocalDate", birthDateAnnotations[AnnotationConstants.PROPERTY_TYPE])
    }

    @Test
    fun `test adapter key NOT added to raw annotations map for built-in types`() {
        // This tests that the ADAPTER key is NOT added for built-in types
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=name, propertyType=kotlin.String}"
        ))

        val nameAnnotations = fieldAnnotations["name"]
        assertEquals(true, nameAnnotations != null, "name annotations should exist")

        // Check that the ADAPTER key was NOT added to the raw annotations map
        assertEquals(false, nameAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "ADAPTER key should NOT be added for built-in types")
        assertEquals("kotlin.String", nameAnnotations[AnnotationConstants.PROPERTY_TYPE])
    }

    @Test
    fun `test new adapter system - adapter=default with built-in type`() {
        // adapter=default + built-in type → no custom adapter
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=name, adapter=default, propertyType=kotlin.String}"
        ))

        val nameAnnotations = fieldAnnotations["name"]
        assertEquals(false, nameAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "adapter=default with built-in type should not generate custom adapter")

        val parsed = FieldAnnotationOverrides.parse(nameAnnotations)
        assertEquals(false, parsed.adapter, "adapter should be false for default + built-in type")
    }

    @Test
    fun `test new adapter system - adapter=default with custom type`() {
        // adapter=default + custom type → custom adapter (same as adapter=custom)
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=birth_date, adapter=default, propertyType=kotlinx.datetime.LocalDate}"
        ))

        val birthDateAnnotations = fieldAnnotations["birth_date"]
        assertEquals(true, birthDateAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "adapter=default with custom type should generate custom adapter")
        assertEquals("custom", birthDateAnnotations[AnnotationConstants.ADAPTER])

        val parsed = FieldAnnotationOverrides.parse(birthDateAnnotations)
        assertEquals(true, parsed.adapter, "adapter should be true for default + custom type")
    }

    @Test
    fun `test new adapter system - adapter=custom always generates adapter`() {
        // adapter=custom → always custom adapter, regardless of propertyType
        val testCases = listOf(
            "-- @@{field=name, adapter=custom, propertyType=kotlin.String}",  // built-in type
            "-- @@{field=birth_date, adapter=custom, propertyType=kotlinx.datetime.LocalDate}",  // custom type
            "-- @@{field=data, adapter=custom}"  // no propertyType
        )

        testCases.forEachIndexed { index, testCase ->
            val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(testCase))
            val fieldName = when(index) {
                0 -> "name"
                1 -> "birth_date"
                else -> "data"
            }

            val annotations = fieldAnnotations[fieldName]
            assertEquals(true, annotations!!.containsKey(AnnotationConstants.ADAPTER),
                "adapter=custom should always generate custom adapter for case: $testCase")
            assertEquals("custom", annotations[AnnotationConstants.ADAPTER])

            val parsed = FieldAnnotationOverrides.parse(annotations)
            assertEquals(true, parsed.adapter, "adapter should be true for custom adapter in case: $testCase")
        }
    }

    @Test
    fun `test new adapter system - no adapter specified defaults to adapter=default`() {
        // No adapter specified → same as adapter=default
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=name, propertyType=kotlin.String}"  // built-in type
        ))
        val nameAnnotations = fieldAnnotations1["name"]
        assertEquals(false, nameAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "no adapter + built-in type should not generate custom adapter")

        val fieldAnnotations2 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}"  // custom type
        ))
        val birthDateAnnotations = fieldAnnotations2["birth_date"]
        assertEquals(true, birthDateAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "no adapter + custom type should generate custom adapter")
        assertEquals("custom", birthDateAnnotations[AnnotationConstants.ADAPTER])
    }

    @Test
    fun `test new adapter system - adapter=default with no propertyType`() {
        // adapter=default + no propertyType → no custom adapter
        val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=data, adapter=default}"
        ))

        val dataAnnotations = fieldAnnotations["data"]
        assertEquals(false, dataAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "adapter=default with no propertyType should not generate custom adapter")

        val parsed = FieldAnnotationOverrides.parse(dataAnnotations)
        assertEquals(false, parsed.adapter, "adapter should be false for default + no propertyType")
    }

    @Test
    fun `test adapter validation - invalid value throws exception`() {
        try {
            extractFieldAssociatedAnnotations(listOf(
                "-- @@{field=test, adapter=invalid}"
            ))
            assertEquals(true, false, "Should have thrown exception for invalid adapter value")
        } catch (e: IllegalArgumentException) {
            assertTrue(true)
        }
    }

    @Test
    fun `test adapter validation - null value throws exception`() {
        try {
            val annotations = mapOf(
                AnnotationConstants.FIELD to "test",
                AnnotationConstants.ADAPTER to null
            )
            FieldAnnotationOverrides.parse(annotations)
            assertEquals(true, false, "Should have thrown exception for null adapter value")
        } catch (e: IllegalArgumentException) {
            assertTrue(true)
        }
    }

    @Test
    fun `test comprehensive no adapter specified behavior`() {
        // Test all scenarios when no adapter is specified (should behave as adapter=default)

        // Case 1: No adapter + no propertyType → no adapter
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=data}"
        ))
        val dataAnnotations = fieldAnnotations1["data"]
        assertEquals(false, dataAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "No adapter + no propertyType should not generate adapter")

        val parsed1 = FieldAnnotationOverrides.parse(dataAnnotations)
        assertEquals(false, parsed1.adapter, "No adapter + no propertyType should have adapter=false")

        // Case 2: No adapter + built-in type → no adapter (uses default conversion)
        val builtInTypes = listOf("kotlin.String", "Int", "Long", "Double", "Boolean")
        builtInTypes.forEach { type ->
            val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
                "-- @@{field=test_field, propertyType=$type}"
            ))
            val annotations = fieldAnnotations["test_field"]
            assertEquals(false, annotations!!.containsKey(AnnotationConstants.ADAPTER),
                "No adapter + built-in type $type should not generate adapter")

            val parsed = FieldAnnotationOverrides.parse(annotations)
            assertEquals(false, parsed.adapter, "No adapter + built-in type $type should have adapter=false")
        }

        // Case 3: No adapter + custom type → custom adapter (same as adapter=custom)
        val customTypes = listOf(
            "kotlinx.datetime.LocalDate",
            "kotlinx.datetime.LocalDateTime",
            "dev.example.CustomType",
            "List<String>",
            "MyCustomClass"
        )
        customTypes.forEach { type ->
            val fieldAnnotations = extractFieldAssociatedAnnotations(listOf(
                "-- @@{field=test_field, propertyType=$type}"
            ))
            val annotations = fieldAnnotations["test_field"]
            assertEquals(true, annotations!!.containsKey(AnnotationConstants.ADAPTER),
                "No adapter + custom type $type should generate custom adapter")
            assertEquals("custom", annotations[AnnotationConstants.ADAPTER],
                "No adapter + custom type $type should have adapter=custom")

            val parsed = FieldAnnotationOverrides.parse(annotations)
            assertEquals(true, parsed.adapter, "No adapter + custom type $type should have adapter=true")
        }
    }

    @Test
    fun `test new notNull annotation system`() {
        // Test notNull=true (enforce non-nullable)
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=name, notNull=true}"
        ))
        val nameAnnotations = fieldAnnotations1["name"]
        assertEquals(true, nameAnnotations!!.containsKey(AnnotationConstants.NOT_NULL))
        assertEquals(true, nameAnnotations[AnnotationConstants.NOT_NULL]) // HOCON parses as string

        val parsed1 = FieldAnnotationOverrides.parse(nameAnnotations)
        assertEquals(true, parsed1.notNull, "notNull=true should be parsed correctly")

        // Test notNull=false (enforce nullable)
        val fieldAnnotations2 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=age, notNull=false}"
        ))
        val ageAnnotations = fieldAnnotations2["age"]
        assertEquals(true, ageAnnotations!!.containsKey(AnnotationConstants.NOT_NULL))
        assertEquals(false, ageAnnotations[AnnotationConstants.NOT_NULL]) // HOCON parses as string

        val parsed2 = FieldAnnotationOverrides.parse(ageAnnotations)
        assertEquals(false, parsed2.notNull, "notNull=false should be parsed correctly")

        // Test notNull not specified (inherit from table structure)
        val fieldAnnotations3 = extractFieldAssociatedAnnotations(listOf(
            "-- @@{field=email, propertyType=kotlin.String}"
        ))
        val emailAnnotations = fieldAnnotations3["email"]
        assertEquals(false, emailAnnotations!!.containsKey(AnnotationConstants.NOT_NULL))

        val parsed3 = FieldAnnotationOverrides.parse(emailAnnotations)
        assertEquals(null, parsed3.notNull, "notNull not specified should be null")
    }

    @Test
    fun `test notNull annotation parsing logic`() {
        // Test notNull=true parsing
        val annotations1 = mapOf(AnnotationConstants.NOT_NULL to true)
        val parsed1 = FieldAnnotationOverrides.parse(annotations1)
        assertEquals(true, parsed1.notNull, "notNull=true should be parsed correctly")

        // Test notNull=false parsing
        val annotations2 = mapOf(AnnotationConstants.NOT_NULL to false)
        val parsed2 = FieldAnnotationOverrides.parse(annotations2)
        assertEquals(false, parsed2.notNull, "notNull=false should be parsed correctly")

        // Test notNull not specified (should be null)
        val annotations3 = mapOf(AnnotationConstants.PROPERTY_TYPE to "kotlin.String")
        val parsed3 = FieldAnnotationOverrides.parse(annotations3)
        assertEquals(null, parsed3.notNull, "notNull not specified should be null")

        // Test that the logic correctly identifies when notNull is specified vs not specified
        assertEquals(true, annotations1.containsKey(AnnotationConstants.NOT_NULL), "Should detect notNull key presence")
        assertEquals(true, annotations2.containsKey(AnnotationConstants.NOT_NULL), "Should detect notNull key presence")
        assertEquals(false, annotations3.containsKey(AnnotationConstants.NOT_NULL), "Should detect notNull key absence")
    }

    @Test
    fun `test field annotation is required`() {
        // Test that annotation blocks without 'field' annotation throw an exception
        try {
            extractFieldAssociatedAnnotations(listOf(
                "-- @@{propertyType=kotlin.String, adapter=custom}" // Missing field annotation
            ))
            assertEquals(true, false, "Should have thrown exception for missing field annotation")
        } catch (e: IllegalArgumentException) {
            assertEquals(true, e.message?.contains("must contain either a 'field' or 'dynamicField' annotation with a non-empty value"),
                "Should mention missing field or dynamicField annotation error")
        }
    }

    @Test
    fun `test field annotation with empty value throws exception`() {
        // Test that field annotation with empty string value throws an exception
        try {
            extractFieldAssociatedAnnotations(listOf(
                "-- @@{field=\"\", propertyType=kotlin.String}" // Empty field name
            ))
            assertEquals(true, false, "Should have thrown exception for empty field name")
        } catch (e: IllegalArgumentException) {
            assertEquals(true, e.message?.contains("must contain either a 'field' or 'dynamicField' annotation with a non-empty value"),
                "Should mention missing field or dynamicField annotation error")
        }
    }

    @Test
    fun testDynamicFieldAnnotation() {
        val comments = listOf(
            "-- @@{ dynamicField=addresses, propertyType=List<String>, defaultValue=listOf() }"
        )

        val result = extractFieldAssociatedAnnotations(comments)

        assertTrue(result.containsKey("addresses"))
        val fieldAnnotations = result["addresses"]!!
        assertEquals("List<String>", fieldAnnotations[AnnotationConstants.PROPERTY_TYPE])
        assertEquals("listOf()", fieldAnnotations[AnnotationConstants.DEFAULT_VALUE])
        assertEquals(true, fieldAnnotations[AnnotationConstants.IS_DYNAMIC_FIELD])
    }

    @Test
    fun testDynamicFieldRequiresPropertyType() {
        val comments = listOf(
            "-- @@{ dynamicField=addresses }"
        )

        try {
            extractFieldAssociatedAnnotations(comments)
            fail("Should have thrown IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message?.contains("dynamicField annotation requires a 'propertyType'") == true)
        }
    }

    @Test
    fun testCannotHaveBothFieldAndDynamicField() {
        val comments = listOf(
            "-- @@{ field=name, dynamicField=addresses, propertyType=String }"
        )

        try {
            extractFieldAssociatedAnnotations(comments)
            fail("Should have thrown IllegalArgumentException")
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message?.contains("cannot contain both 'field' and 'dynamicField'") == true)
        }
    }

    @Test
    fun testFieldAnnotationOverridesWithDynamicField() {
        val annotations = mapOf(
            AnnotationConstants.IS_DYNAMIC_FIELD to true,
            AnnotationConstants.PROPERTY_TYPE to "List<String>",
            AnnotationConstants.DEFAULT_VALUE to "listOf()"
        )

        val result = FieldAnnotationOverrides.parse(annotations)

        assertEquals(true, result.isDynamicField)
        assertEquals("List<String>", result.propertyType)
        assertEquals("listOf()", result.defaultValue)
    }
}
