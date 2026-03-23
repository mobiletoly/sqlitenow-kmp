package dev.goquick.sqlitenow.core.util

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DateTimeTest {

    @Suppress("DEPRECATION")
    @Test
    fun testBasicConversion() {
        // Test basic round-trip conversion
        val dateTime = LocalDateTime(2025, 5, 26, 19, 43, 13)
        val timestamp = dateTime.toSqliteTimestamp()
        val parsed = LocalDateTime.fromSqliteTimestamp(timestamp)

        assertEquals("2025-05-26 19:43:13", timestamp)
        assertEquals(dateTime, parsed)
    }

    @Suppress("DEPRECATION")
    @Test
    fun testZeroPadding() {
        // Test that single digits are properly zero-padded
        val dateTime = LocalDateTime(2025, 1, 5, 9, 3, 7)
        val timestamp = dateTime.toSqliteTimestamp()
        assertEquals("2025-01-05 09:03:07", timestamp)
    }

    @Suppress("DEPRECATION")
    @Test
    fun testInvalidFormats() {
        // Test a few key invalid formats that our error handling should catch
        val invalidFormats = listOf(
            "",                      // Empty string
            "2025-05-26",           // Missing time
            "2025/05/26 19:43:13",  // Wrong separator
            "not-a-date"            // Completely invalid
        )

        invalidFormats.forEach { invalidFormat ->
            assertFailsWith<IllegalArgumentException> {
                LocalDateTime.fromSqliteTimestamp(invalidFormat)
            }
        }
    }

    @Suppress("DEPRECATION")
    @Test
    fun testRoundTripConversion() {
        // Test a few different dates to ensure data integrity
        val testDates = listOf(
            LocalDateTime(2025, 1, 1, 0, 0, 0),      // New Year
            LocalDateTime(2024, 2, 29, 12, 30, 45),  // Leap year
            LocalDateTime(2025, 12, 31, 23, 59, 59)  // End of year
        )

        testDates.forEach { original ->
            val timestamp = original.toSqliteTimestamp()
            val parsed = LocalDateTime.fromSqliteTimestamp(timestamp)
            assertEquals(original, parsed, "Round-trip failed for: $original")
        }
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testInstantToRfc3339String() {
        val instant = Instant.parse("2025-05-26T19:43:13Z")
        assertEquals("2025-05-26T19:43:13Z", instant.toRfc3339String())
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testInstantFromRfc3339String() {
        val parsed = Instant.fromRfc3339String("2025-05-26T19:43:13Z")
        assertEquals(Instant.parse("2025-05-26T19:43:13Z"), parsed)
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testInstantRfc3339RoundTripWithFractionalSeconds() {
        val original = Instant.parse("2025-05-26T19:43:13.123456Z")
        val serialized = original.toRfc3339String()
        val parsed = Instant.fromRfc3339String(serialized)

        assertEquals("2025-05-26T19:43:13.123456Z", serialized)
        assertEquals(original, parsed)
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testInstantFromRfc3339StringInvalidFormat() {
        assertFailsWith<IllegalArgumentException> {
            Instant.fromRfc3339String("2025-05-26 19:43:13")
        }
    }

    // LocalDate tests
    @Test
    fun testBasicDateConversion() {
        // Test basic round-trip conversion for LocalDate
        val date = LocalDate(2025, 5, 26)
        val dateString = date.toSqliteDate()
        val parsed = LocalDate.fromSqliteDate(dateString)

        assertEquals("2025-05-26", dateString)
        assertEquals(date, parsed)
    }

    @Test
    fun testDateZeroPadding() {
        // Test that single digits are properly zero-padded for dates
        val date = LocalDate(2025, 1, 5)
        val dateString = date.toSqliteDate()
        assertEquals("2025-01-05", dateString)
    }

    @Test
    fun testInvalidDateFormats() {
        // Test a few key invalid date formats
        val invalidFormats = listOf(
            "",                    // Empty string
            "2025-05",            // Missing day
            "2025/05/26",         // Wrong separator
            "not-a-date"          // Completely invalid
        )

        invalidFormats.forEach { invalidFormat ->
            assertFailsWith<IllegalArgumentException> {
                LocalDate.fromSqliteDate(invalidFormat)
            }
        }
    }

    @Test
    fun testDateRoundTripConversion() {
        // Test a few different dates to ensure data integrity
        val testDates = listOf(
            LocalDate(2025, 1, 1),      // New Year
            LocalDate(2024, 2, 29),     // Leap year
            LocalDate(2025, 12, 31)     // End of year
        )

        testDates.forEach { original ->
            val dateString = original.toSqliteDate()
            val parsed = LocalDate.fromSqliteDate(dateString)
            assertEquals(original, parsed, "Round-trip failed for: $original")
        }
    }

    // LocalTime tests
    @Test
    fun testBasicTimeConversion() {
        // Test basic round-trip conversion for LocalTime
        val time = LocalTime(19, 43, 13)
        val timeString = time.toSqliteTime()
        val parsed = LocalTime.fromSqliteTime(timeString)

        assertEquals("19:43:13", timeString)
        assertEquals(time, parsed)
    }

    @Test
    fun testTimeZeroPadding() {
        // Test that single digits are properly zero-padded for times
        val time = LocalTime(9, 3, 7)
        val timeString = time.toSqliteTime()
        assertEquals("09:03:07", timeString)
    }

    @Test
    fun testInvalidTimeFormats() {
        // Test a few key invalid time formats
        val invalidFormats = listOf(
            "",                    // Empty string
            "19:43",              // Missing seconds
            "19-43-13",           // Wrong separator
            "not-a-time"          // Completely invalid
        )

        invalidFormats.forEach { invalidFormat ->
            assertFailsWith<IllegalArgumentException> {
                LocalTime.fromSqliteTime(invalidFormat)
            }
        }
    }

    @Test
    fun testTimeRoundTripConversion() {
        // Test a few different times to ensure data integrity
        val testTimes = listOf(
            LocalTime(0, 0, 0),        // Midnight
            LocalTime(12, 0, 0),       // Noon
            LocalTime(23, 59, 59)      // End of day
        )

        testTimes.forEach { original ->
            val timeString = original.toSqliteTime()
            val parsed = LocalTime.fromSqliteTime(timeString)
            assertEquals(original, parsed, "Round-trip failed for: $original")
        }
    }
}
