package dev.goquick.sqlitenow.gradle

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class AnnotationsTest {

    @Test
    @DisplayName("Test extracting basic annotations")
    fun testExtractAnnotations() {
        val comments = listOf(
            "-- This is a comment with @@annotation1 and @@annotation2=value2",
            "-- Another comment with @@annotation3=value3"
        )

        val result = extractAnnotations(comments)

        assertEquals(3, result.size)
        assertEquals("", result["annotation1"])
        assertEquals("value2", result["annotation2"])
        assertEquals("value3", result["annotation3"])
    }

    @Test
    @DisplayName("Test extracting field-associated annotations")
    fun testExtractFieldAssociatedAnnotations() {
        val comments = listOf(
            "-- This is a comment with @@field=user_id @@nullable @@default=0",
            "-- Another comment with @@field=username @@unique",
            "-- A comment without field association @@ignored",
            "-- Back to @@field=email @@required"
        )

        val result = extractFieldAssociatedAnnotations(comments)

        // Check the number of fields
        assertEquals(3, result.size)

        // Check user_id field annotations
        val userIdAnnotations = result["user_id"]
        assertNotNull(userIdAnnotations)
        assertEquals(2, userIdAnnotations.size)
        assertNull(userIdAnnotations["nullable"], "nullable annotation should have null value")
        assertEquals("0", userIdAnnotations["default"], "default annotation should have value '0'")

        // Check username field annotations
        val usernameAnnotations = result["username"]
        assertNotNull(usernameAnnotations)
        assertEquals(2, usernameAnnotations.size, "username should have 2 annotations")
        assertNull(usernameAnnotations["unique"], "unique annotation should have null value")
        assertNull(usernameAnnotations["ignored"], "ignored annotation should have null value")

        // Check email field annotations
        val emailAnnotations = result["email"]
        assertNotNull(emailAnnotations)
        assertEquals(1, emailAnnotations.size)
        assertNull(emailAnnotations["required"])
    }

    @Test
    @DisplayName("Test extracting field-associated annotations with multiple annotations per field")
    fun testExtractFieldAssociatedAnnotationsMultiple() {
        val comments = listOf(
            "-- @@field=id @@primaryKey @@autoIncrement",
            "-- @@field=name @@notNull @@unique @@default='Unknown'"
        )

        val result = extractFieldAssociatedAnnotations(comments)

        // Check the number of fields
        assertEquals(2, result.size)

        // Check id field annotations
        val idAnnotations = result["id"]
        assertNotNull(idAnnotations)
        assertEquals(2, idAnnotations.size)
        assertNull(idAnnotations["primaryKey"])
        assertNull(idAnnotations["autoIncrement"])

        // Check name field annotations
        val nameAnnotations = result["name"]
        assertNotNull(nameAnnotations)
        assertEquals(3, nameAnnotations.size)
        assertNull(nameAnnotations["notNull"])
        assertNull(nameAnnotations["unique"])
        assertEquals("'Unknown'", nameAnnotations["default"])
    }

    @Test
    @DisplayName("Test extracting field-associated annotations with empty comments")
    fun testExtractFieldAssociatedAnnotationsEmpty() {
        val comments = listOf<String>()

        val result = extractFieldAssociatedAnnotations(comments)

        // Result should be empty
        assertEquals(0, result.size)
    }

    @Test
    @DisplayName("Test extracting field-associated annotations with no annotations")
    fun testExtractFieldAssociatedAnnotationsNoAnnotations() {
        val comments = listOf(
            "-- This is a comment without annotations",
            "-- Another comment without annotations"
        )

        val result = extractFieldAssociatedAnnotations(comments)

        // Result should be empty
        assertEquals(0, result.size)
    }
}
