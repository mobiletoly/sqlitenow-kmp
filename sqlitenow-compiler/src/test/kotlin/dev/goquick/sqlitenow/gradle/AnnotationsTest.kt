package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.util.SqliteTypeToKotlinCodeConverter.Companion.KOTLIN_STDLIB_TYPES
import dev.goquick.sqlitenow.gradle.processing.AnnotationConstants
import dev.goquick.sqlitenow.gradle.processing.FieldAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationOverrides
import dev.goquick.sqlitenow.gradle.processing.StatementAnnotationContext
import dev.goquick.sqlitenow.gradle.processing.extractAnnotations
import dev.goquick.sqlitenow.gradle.processing.extractFieldAssociatedAnnotations
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AnnotationsTest {

    @TestFactory
    fun `raw annotation syntax cases`(): List<DynamicTest> = annotationExtractionTests(
        listOf(
            annotationExtractionCase(
                displayName = "basic inline syntax",
                comment = """-- @@{field=birth_date, adapter="", propertyType=kotlinx.datetime.LocalDate}""",
                expected = mapOf(
                    "field" to "birth_date",
                    "adapter" to "",
                    "propertyType" to "kotlinx.datetime.LocalDate",
                ),
            ),
            AnnotationExtractionCase(
                displayName = "multiline syntax",
                comments = listOf(
                    """-- @@{field=created_at, adapter="",""",
                    "--     propertyType=kotlinx.datetime.LocalDateTime",
                    "-- }"
                ),
                expected = mapOf(
                    "field" to "created_at",
                    "adapter" to "",
                    "propertyType" to "kotlinx.datetime.LocalDateTime",
                ),
            ),
            annotationExtractionCase(
                displayName = "spacing syntax",
                comment = """-- @@{ field = birth_date , adapter = "" , propertyType = kotlinx.datetime.LocalDate }""",
                expected = mapOf(
                    "field" to "birth_date",
                    "adapter" to "",
                    "propertyType" to "kotlinx.datetime.LocalDate",
                ),
            ),
            annotationExtractionCase(
                displayName = "quoted values",
                comment = """-- @@{field="user name", propertyType="String?"}""",
                expected = mapOf(
                    "field" to "user name",
                    "propertyType" to "String?",
                ),
            ),
            annotationExtractionCase(
                displayName = "empty block",
                comment = "-- @@{}",
                expected = emptyMap(),
            ),
            annotationExtractionCase(
                displayName = "single annotation without value",
                comment = """-- @@{adapter=""}""",
                expected = mapOf("adapter" to ""),
            ),
            annotationExtractionCase(
                displayName = "single annotation with value",
                comment = "-- @@{field=user_id}",
                expected = mapOf("field" to "user_id"),
            ),
        )
    )

    @TestFactory
    fun `null and empty annotation values are preserved`(): List<DynamicTest> = annotationExtractionTests(
        listOf(
            annotationExtractionCase(
                displayName = "explicit null values",
                comment = "-- @@{field=user_id, adapter=null, propertyType=\"String?\"}",
                expected = mapOf(
                    "field" to "user_id",
                    "adapter" to null,
                    "propertyType" to "String?",
                ),
            ),
            annotationExtractionCase(
                displayName = "empty string and null remain distinct",
                comment = """-- @@{field=user_id, emptyValue="", nullValue=null}""",
                expected = mapOf(
                    "field" to "user_id",
                    "emptyValue" to "",
                    "nullValue" to null,
                ),
            ),
        )
    )

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

    @TestFactory
    fun `create table annotations infer adapter requirement from property type`(): List<DynamicTest> =
        listOf(
            CreateTableAdapterInferenceCase(
                displayName = "custom type without explicit adapter",
                fieldName = "birth_date",
                propertyType = "kotlinx.datetime.LocalDate",
                expectedAdapter = true,
            ),
            CreateTableAdapterInferenceCase(
                displayName = "built-in type without explicit adapter",
                fieldName = "name",
                propertyType = "kotlin.String",
                expectedAdapter = false,
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val fieldAnnotations = extractFieldAssociatedAnnotations(
                    listOf("-- @@{field=${case.fieldName}, propertyType=${case.propertyType}}")
                )

                val annotations = fieldAnnotations[case.fieldName]
                assertEquals(true, annotations != null, "${case.fieldName} annotations should exist")
                assertEquals(case.propertyType, annotations?.get("propertyType"))

                val parsed = FieldAnnotationOverrides.parse(annotations!!)
                assertEquals(case.expectedAdapter, parsed.adapter)
                assertEquals(case.propertyType, parsed.propertyType)
            }
        }

    @Test
    fun `test adapter key added to raw annotations map for custom types`() {
        // This tests that the ADAPTER key is actually added to the raw annotations map
        // which is what AdapterConfig.hasAdapterAnnotation() checks
        val fieldAnnotations = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}"
            )
        )

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
        val fieldAnnotations = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=name, propertyType=kotlin.String}"
            )
        )

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
        val fieldAnnotations = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=name, adapter=default, propertyType=kotlin.String}"
            )
        )

        val nameAnnotations = fieldAnnotations["name"]
        assertEquals(false, nameAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "adapter=default with built-in type should not generate custom adapter")

        val parsed = FieldAnnotationOverrides.parse(nameAnnotations)
        assertEquals(false, parsed.adapter, "adapter should be false for default + built-in type")
    }

    @Test
    fun `test new adapter system - adapter=default with custom type`() {
        // adapter=default + custom type → custom adapter (same as adapter=custom)
        val fieldAnnotations = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=birth_date, adapter=default, propertyType=kotlinx.datetime.LocalDate}"
            )
        )

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
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=name, propertyType=kotlin.String}"  // built-in type
            )
        )
        val nameAnnotations = fieldAnnotations1["name"]
        assertEquals(false, nameAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "no adapter + built-in type should not generate custom adapter")

        val fieldAnnotations2 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=birth_date, propertyType=kotlinx.datetime.LocalDate}"  // custom type
            )
        )
        val birthDateAnnotations = fieldAnnotations2["birth_date"]
        assertEquals(true, birthDateAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "no adapter + custom type should generate custom adapter")
        assertEquals("custom", birthDateAnnotations[AnnotationConstants.ADAPTER])
    }

    @Test
    fun `test new adapter system - adapter=default with no propertyType`() {
        // adapter=default + no propertyType → no custom adapter
        val fieldAnnotations = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=data, adapter=default}"
            )
        )

        val dataAnnotations = fieldAnnotations["data"]
        assertEquals(false, dataAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "adapter=default with no propertyType should not generate custom adapter")

        val parsed = FieldAnnotationOverrides.parse(dataAnnotations)
        assertEquals(false, parsed.adapter, "adapter should be false for default + no propertyType")
    }

    @Test
    fun `test adapter validation - invalid value throws exception`() {
        try {
            extractFieldAssociatedAnnotations(
                listOf(
                    "-- @@{field=test, adapter=invalid}"
                )
            )
            assertEquals(true, false, "Should have thrown exception for invalid adapter value")
        } catch (_: IllegalArgumentException) {
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
        } catch (_: IllegalArgumentException) {
            assertTrue(true)
        }
    }

    @Test
    fun `test comprehensive no adapter specified behavior`() {
        // Test all scenarios when no adapter is specified (should behave as adapter=default)

        // Case 1: No adapter + no propertyType → no adapter
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=data}"
            )
        )
        val dataAnnotations = fieldAnnotations1["data"]
        assertEquals(false, dataAnnotations!!.containsKey(AnnotationConstants.ADAPTER),
            "No adapter + no propertyType should not generate adapter")

        val parsed1 = FieldAnnotationOverrides.parse(dataAnnotations)
        assertEquals(false, parsed1.adapter, "No adapter + no propertyType should have adapter=false")

        // Case 2: No adapter + built-in type → no adapter (uses default conversion)
        val builtInTypes = listOf("kotlin.String", "Int", "Long", "Double", "Boolean")
        builtInTypes.forEach { type ->
            val fieldAnnotations = extractFieldAssociatedAnnotations(
                listOf(
                    "-- @@{field=test_field, propertyType=$type}"
                )
            )
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
            val fieldAnnotations = extractFieldAssociatedAnnotations(
                listOf(
                    "-- @@{field=test_field, propertyType=$type}"
                )
            )
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
        val fieldAnnotations1 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=name, notNull=true}"
            )
        )
        val nameAnnotations = fieldAnnotations1["name"]
        assertEquals(true, nameAnnotations!!.containsKey(AnnotationConstants.NOT_NULL))
        assertEquals(true, nameAnnotations[AnnotationConstants.NOT_NULL]) // HOCON parses as string

        val parsed1 = FieldAnnotationOverrides.parse(nameAnnotations)
        assertEquals(true, parsed1.notNull, "notNull=true should be parsed correctly")

        // Test notNull=false (enforce nullable)
        val fieldAnnotations2 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=age, notNull=false}"
            )
        )
        val ageAnnotations = fieldAnnotations2["age"]
        assertEquals(true, ageAnnotations!!.containsKey(AnnotationConstants.NOT_NULL))
        assertEquals(false, ageAnnotations[AnnotationConstants.NOT_NULL]) // HOCON parses as string

        val parsed2 = FieldAnnotationOverrides.parse(ageAnnotations)
        assertEquals(false, parsed2.notNull, "notNull=false should be parsed correctly")

        // Test notNull not specified (inherit from table structure)
        val fieldAnnotations3 = extractFieldAssociatedAnnotations(
            listOf(
                "-- @@{field=email, propertyType=kotlin.String}"
            )
        )
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
            extractFieldAssociatedAnnotations(
                listOf(
                    "-- @@{propertyType=kotlin.String, adapter=custom}" // Missing field annotation
                )
            )
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
            extractFieldAssociatedAnnotations(
                listOf(
                    "-- @@{field=\"\", propertyType=kotlin.String}" // Empty field name
                )
            )
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

    @TestFactory
    fun `field associated annotations reject invalid dynamic field cases`(): List<DynamicTest> =
        listOf(
            FieldAssociatedValidationCase(
                displayName = "dynamicField requires propertyType",
                comments = listOf("-- @@{ dynamicField=addresses }"),
                expectedMessage = "dynamicField annotation requires a 'propertyType'",
            ),
            FieldAssociatedValidationCase(
                displayName = "field and dynamicField cannot both be set",
                comments = listOf("-- @@{ field=name, dynamicField=addresses, propertyType=String }"),
                expectedMessage = "cannot contain both 'field' and 'dynamicField'",
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val exception = assertThrows<IllegalArgumentException> {
                    extractFieldAssociatedAnnotations(case.comments)
                }
                assertTrue(exception.message!!.contains(case.expectedMessage))
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

    @Test
    fun testFieldAnnotationOverridesWithMappingType() {
        val annotations = mapOf<String, Any>(
            AnnotationConstants.IS_DYNAMIC_FIELD to true,
            AnnotationConstants.PROPERTY_TYPE to "PersonAddressQuery.SharedResult.Row",
            AnnotationConstants.MAPPING_TYPE to "perRow",
            AnnotationConstants.SOURCE_TABLE to "a",
            AnnotationConstants.ALIAS_PREFIX to "address_"
        )

        val result = FieldAnnotationOverrides.parse(annotations)

        assertEquals(true, result.isDynamicField)
        assertEquals("PersonAddressQuery.SharedResult.Row", result.propertyType)
        assertEquals("perRow", result.mappingType)
        assertEquals("a", result.sourceTable)
        assertEquals("address_", result.aliasPrefix)
    }

    @TestFactory
    fun `field annotation overrides reject invalid mapping type cases`(): List<DynamicTest> =
        listOf(
            FieldOverrideValidationCase(
                displayName = "mappingType requires sourceTable",
                annotations = mapOf(
                    AnnotationConstants.IS_DYNAMIC_FIELD to true,
                    AnnotationConstants.MAPPING_TYPE to "perRow",
                ),
                expectedMessage = "When annotation 'mappingType' is specified, 'sourceTable' is required",
            ),
            FieldOverrideValidationCase(
                displayName = "mappingType requires dynamicField",
                annotations = mapOf(
                    AnnotationConstants.MAPPING_TYPE to "perRow",
                    AnnotationConstants.SOURCE_TABLE to "a",
                ),
                expectedMessage = "can only be used with dynamic fields",
            ),
            FieldOverrideValidationCase(
                displayName = "mappingType value must be supported",
                annotations = mapOf(
                    AnnotationConstants.IS_DYNAMIC_FIELD to true,
                    AnnotationConstants.MAPPING_TYPE to "invalidType",
                    AnnotationConstants.SOURCE_TABLE to "a",
                ),
                expectedMessage = "Currently supported: 'perRow'",
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val exception = assertThrows<IllegalArgumentException> {
                    FieldAnnotationOverrides.parse(case.annotations)
                }
                assertTrue(exception.message!!.contains(case.expectedMessage))
            }
        }

    @Test
    fun `statement annotations capture mapTo`() {
        val overrides = StatementAnnotationOverrides.parse(
            mapOf(AnnotationConstants.MAP_TO to "com.example.Target")
        )
        assertEquals("com.example.Target", overrides.mapTo)
    }

    @Test
    fun `field annotations reject unknown keys`() {
        val exception = assertThrows<IllegalArgumentException> {
            FieldAnnotationOverrides.parse(mapOf("unknown" to "value"))
        }
        assertTrue(exception.message!!.contains("Unsupported field annotation(s)"))
        assertTrue(exception.message!!.contains("Supported keys"))
    }

    @TestFactory
    fun `statement annotations reject invalid overrides`(): List<DynamicTest> =
        listOf(
            StatementOverrideValidationCase(
                displayName = "select annotations reject unknown keys",
                annotations = mapOf("mystery" to "x"),
                context = StatementAnnotationContext.SELECT,
                expectedMessages = listOf("Unsupported annotation(s)", "SELECT statement"),
            ),
            StatementOverrideValidationCase(
                displayName = "mapTo not allowed for create table",
                annotations = mapOf(AnnotationConstants.MAP_TO to "com.example.Target"),
                context = StatementAnnotationContext.CREATE_TABLE,
                expectedMessages = listOf("Unsupported annotation(s)", "CREATE TABLE statement"),
            ),
            StatementOverrideValidationCase(
                displayName = "cascadeNotify rejects unsupported keys",
                annotations = mapOf(
                    AnnotationConstants.CASCADE_NOTIFY to mapOf(
                        "refresh" to listOf("person"),
                    ),
                ),
                context = StatementAnnotationContext.CREATE_TABLE,
                expectedMessages = listOf("Unsupported key(s) refresh"),
            ),
            StatementOverrideValidationCase(
                displayName = "cascadeNotify rejects non list values",
                annotations = mapOf(
                    AnnotationConstants.CASCADE_NOTIFY to mapOf(
                        "update" to "person",
                    ),
                ),
                context = StatementAnnotationContext.CREATE_TABLE,
                expectedMessages = listOf("must be a list of table names"),
            ),
        ).map { case ->
            DynamicTest.dynamicTest(case.displayName) {
                val exception = assertThrows<IllegalArgumentException> {
                    StatementAnnotationOverrides.parse(case.annotations, context = case.context)
                }
                case.expectedMessages.forEach { expectedMessage ->
                    assertTrue(exception.message!!.contains(expectedMessage))
                }
            }
        }

    @Test
    fun `extract annotations keeps nested objects and lists intact`() {
        val annotations = extractAnnotations(
            listOf(
                "-- @@{ cascadeNotify = { update = [person, address], delete = [comment] } }"
            )
        )

        val cascadeNotify = annotations[AnnotationConstants.CASCADE_NOTIFY] as Map<*, *>
        assertEquals(listOf("person", "address"), cascadeNotify["update"])
        assertEquals(listOf("comment"), cascadeNotify["delete"])
    }

    @Test
    fun `create table annotations parse cascadeNotify`() {
        val overrides = StatementAnnotationOverrides.parse(
            mapOf(
                AnnotationConstants.CASCADE_NOTIFY to mapOf(
                    "insert" to listOf("person"),
                    "update" to listOf("address"),
                    "delete" to listOf("comment"),
                )
            ),
            context = StatementAnnotationContext.CREATE_TABLE
        )

        assertEquals(setOf("person"), overrides.cascadeNotify?.insert)
        assertEquals(setOf("address"), overrides.cascadeNotify?.update)
        assertEquals(setOf("comment"), overrides.cascadeNotify?.delete)
    }

    private data class AnnotationExtractionCase(
        val displayName: String,
        val comments: List<String>,
        val expected: Map<String, String?>,
    )

    private data class FieldAssociatedValidationCase(
        val displayName: String,
        val comments: List<String>,
        val expectedMessage: String,
    )

    private data class FieldOverrideValidationCase(
        val displayName: String,
        val annotations: Map<String, Any>,
        val expectedMessage: String,
    )

    private data class CreateTableAdapterInferenceCase(
        val displayName: String,
        val fieldName: String,
        val propertyType: String,
        val expectedAdapter: Boolean,
    )

    private data class StatementOverrideValidationCase(
        val displayName: String,
        val annotations: Map<String, Any>,
        val context: StatementAnnotationContext,
        val expectedMessages: List<String>,
    )

    private fun annotationExtractionCase(
        displayName: String,
        comment: String,
        expected: Map<String, String?>,
    ): AnnotationExtractionCase = AnnotationExtractionCase(
        displayName = displayName,
        comments = listOf(comment),
        expected = expected,
    )

    private fun annotationExtractionTests(cases: List<AnnotationExtractionCase>): List<DynamicTest> = cases.map { case ->
        DynamicTest.dynamicTest(case.displayName) {
            val annotations = extractAnnotations(case.comments)

            assertEquals(case.expected.size, annotations.size)
            case.expected.forEach { (key, value) ->
                assertEquals(value, annotations[key])
            }
        }
    }
}
