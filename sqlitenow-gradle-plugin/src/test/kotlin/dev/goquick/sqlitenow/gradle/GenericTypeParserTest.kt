/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.util.GenericTypeParser
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class GenericTypeParserTest {

    @Test
    fun testIsGenericType() {
        // Generic types
        assertTrue(GenericTypeParser.isGenericType("List<String>"))
        assertTrue(GenericTypeParser.isGenericType("Map<String, Int>"))
        assertTrue(GenericTypeParser.isGenericType("List<Map<String, Int>>"))
        assertTrue(GenericTypeParser.isGenericType("com.example.CustomList<String>"))
        
        // Non-generic types
        assertFalse(GenericTypeParser.isGenericType("String"))
        assertFalse(GenericTypeParser.isGenericType("Int"))
        assertFalse(GenericTypeParser.isGenericType("com.example.CustomClass"))
        assertFalse(GenericTypeParser.isGenericType(""))
        
        // Edge cases
        assertFalse(GenericTypeParser.isGenericType("List<"))
        assertFalse(GenericTypeParser.isGenericType(">String"))
        assertFalse(GenericTypeParser.isGenericType("List>String<"))
    }

    @Test
    fun testExtractRawTypeName() {
        // Generic types
        assertEquals("List", GenericTypeParser.extractRawTypeName("List<String>"))
        assertEquals("Map", GenericTypeParser.extractRawTypeName("Map<String, Int>"))
        assertEquals("List", GenericTypeParser.extractRawTypeName("List<Map<String, Int>>"))
        assertEquals("com.example.CustomList", GenericTypeParser.extractRawTypeName("com.example.CustomList<String>"))
        
        // Non-generic types
        assertEquals("String", GenericTypeParser.extractRawTypeName("String"))
        assertEquals("Int", GenericTypeParser.extractRawTypeName("Int"))
        assertEquals("com.example.CustomClass", GenericTypeParser.extractRawTypeName("com.example.CustomClass"))
        
        // With whitespace
        assertEquals("List", GenericTypeParser.extractRawTypeName("  List<String>  "))
        assertEquals("String", GenericTypeParser.extractRawTypeName("  String  "))
    }

    @Test
    fun testExtractFirstTypeArgument() {
        // Simple generic types
        assertEquals("String", GenericTypeParser.extractFirstTypeArgument("List<String>"))
        assertEquals("Int", GenericTypeParser.extractFirstTypeArgument("Set<Int>"))
        assertEquals("String", GenericTypeParser.extractFirstTypeArgument("Map<String, Int>"))
        
        // Nested generic types
        assertEquals("Map<String, Int>", GenericTypeParser.extractFirstTypeArgument("List<Map<String, Int>>"))
        assertEquals("List<String>", GenericTypeParser.extractFirstTypeArgument("Map<List<String>, Int>"))
        
        // Non-generic types
        assertEquals("String", GenericTypeParser.extractFirstTypeArgument("String"))
        assertEquals("Int", GenericTypeParser.extractFirstTypeArgument("Int"))
        
        // Custom types
        assertEquals("com.example.Person", GenericTypeParser.extractFirstTypeArgument("List<com.example.Person>"))
        assertEquals("ActivityQuery.SharedResult.Row", GenericTypeParser.extractFirstTypeArgument("List<ActivityQuery.SharedResult.Row>"))
        
        // With whitespace
        assertEquals("String", GenericTypeParser.extractFirstTypeArgument("  List<String>  "))
    }

    @Test
    fun testExtractTypeArguments() {
        // Single type argument
        assertEquals(listOf("String"), GenericTypeParser.extractTypeArguments("List<String>"))
        assertEquals(listOf("Int"), GenericTypeParser.extractTypeArguments("Set<Int>"))
        
        // Multiple type arguments
        assertEquals(listOf("String", "Int"), GenericTypeParser.extractTypeArguments("Map<String, Int>"))
        assertEquals(listOf("String", "Int", "Boolean"), GenericTypeParser.extractTypeArguments("Triple<String, Int, Boolean>"))
        
        // Nested generic types
        assertEquals(listOf("Map<String, Int>"), GenericTypeParser.extractTypeArguments("List<Map<String, Int>>"))
        assertEquals(listOf("List<String>", "Int"), GenericTypeParser.extractTypeArguments("Map<List<String>, Int>"))
        assertEquals(listOf("Map<String, List<Int>>", "Boolean"), GenericTypeParser.extractTypeArguments("Pair<Map<String, List<Int>>, Boolean>"))
        
        // Non-generic types
        assertEquals(emptyList(), GenericTypeParser.extractTypeArguments("String"))
        assertEquals(emptyList(), GenericTypeParser.extractTypeArguments("Int"))
        
        // Custom types
        assertEquals(listOf("com.example.Person"), GenericTypeParser.extractTypeArguments("List<com.example.Person>"))
        assertEquals(listOf("ActivityQuery.SharedResult.Row"), GenericTypeParser.extractTypeArguments("List<ActivityQuery.SharedResult.Row>"))
        
        // With whitespace
        assertEquals(listOf("String", "Int"), GenericTypeParser.extractTypeArguments("Map< String , Int >"))
        assertEquals(listOf("String"), GenericTypeParser.extractTypeArguments("  List<String>  "))
        
        // Empty type arguments (edge case)
        assertEquals(emptyList(), GenericTypeParser.extractTypeArguments("List<>"))
    }

    @Test
    fun testComplexNestedGenerics() {
        // Very complex nested generics
        val complexType = "Map<String, List<Pair<Int, Map<String, Set<Boolean>>>>>"
        
        assertTrue(GenericTypeParser.isGenericType(complexType))
        assertEquals("Map", GenericTypeParser.extractRawTypeName(complexType))
        assertEquals("String", GenericTypeParser.extractFirstTypeArgument(complexType))
        
        val typeArgs = GenericTypeParser.extractTypeArguments(complexType)
        assertEquals(2, typeArgs.size)
        assertEquals("String", typeArgs[0])
        assertEquals("List<Pair<Int, Map<String, Set<Boolean>>>>", typeArgs[1])
        
        // Test the nested type argument
        val nestedType = typeArgs[1]
        assertEquals("List", GenericTypeParser.extractRawTypeName(nestedType))
        assertEquals("Pair<Int, Map<String, Set<Boolean>>>", GenericTypeParser.extractFirstTypeArgument(nestedType))
    }

    @Test
    fun testRealWorldExamples() {
        // Examples from the actual codebase
        assertEquals("ActivityQuery.SharedResult.ActivityDetailedDoc",
            GenericTypeParser.extractFirstTypeArgument("List<ActivityQuery.SharedResult.ActivityDetailedDoc>"))

        assertEquals("ReviewQuery.SharedResult.DetailedRow",
            GenericTypeParser.extractFirstTypeArgument("List<ReviewQuery.SharedResult.DetailedRow>"))

        assertEquals("String",
            GenericTypeParser.extractFirstTypeArgument("List<String>"))

        // Non-generic should return itself
        assertEquals("String",
            GenericTypeParser.extractFirstTypeArgument("String"))
    }

    @Test
    fun testEdgeCasesFromQueryCodeGenerator() {
        // Test cases that were previously handled by hardcoded string manipulation

        // Case 1: Nested generics that were problematic with substringAfter/substringBefore
        assertEquals("Map<String, Int>",
            GenericTypeParser.extractFirstTypeArgument("List<Map<String, Int>>"))

        // Case 2: Multiple levels of nesting
        assertEquals("List<Map<String, Set<Boolean>>>",
            GenericTypeParser.extractFirstTypeArgument("Optional<List<Map<String, Set<Boolean>>>>"))

        // Case 3: Complex real-world type from codebase - first type argument should be "String"
        assertEquals("String",
            GenericTypeParser.extractFirstTypeArgument("Map<String, Pair<ActivityQuery.SharedResult.Row, List<String>>>"))

        // Case 3b: Test extracting all type arguments from complex type
        val complexTypeArgs = GenericTypeParser.extractTypeArguments("Map<String, Pair<ActivityQuery.SharedResult.Row, List<String>>>")
        assertEquals(2, complexTypeArgs.size)
        assertEquals("String", complexTypeArgs[0])
        assertEquals("Pair<ActivityQuery.SharedResult.Row, List<String>>", complexTypeArgs[1])

        // Case 4: Ensure we handle the edge cases properly
        assertEquals("", GenericTypeParser.extractFirstTypeArgument(""))
        assertEquals("", GenericTypeParser.extractFirstTypeArgument("   ")) // Trimmed to empty string
    }
}
