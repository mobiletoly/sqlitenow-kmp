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
package dev.goquick.sqlitenow.core.util

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DateTimeTest {

    @Test
    fun testBasicConversion() {
        // Test basic round-trip conversion
        val dateTime = LocalDateTime(2025, 5, 26, 19, 43, 13)
        val timestamp = dateTime.toSqliteTimestamp()
        val parsed = LocalDateTime.fromSqliteTimestamp(timestamp)

        assertEquals("2025-05-26 19:43:13", timestamp)
        assertEquals(dateTime, parsed)
    }

    @Test
    fun testZeroPadding() {
        // Test that single digits are properly zero-padded
        val dateTime = LocalDateTime(2025, 1, 5, 9, 3, 7)
        val timestamp = dateTime.toSqliteTimestamp()
        assertEquals("2025-01-05 09:03:07", timestamp)
    }

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
