package dev.goquick.sqlitenow.core.util

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.format.FormatStringsInDatetimeFormats
import kotlinx.datetime.format.byUnicodePattern

/**
 * SQLite datetime format pattern: "YYYY-MM-DD HH:MM:SS"
 * This matches SQLite's default datetime() function output format.
 */
@OptIn(FormatStringsInDatetimeFormats::class)
private val SQLITE_DATETIME_FORMAT = LocalDateTime.Format {
    byUnicodePattern("yyyy-MM-dd HH:mm:ss")
}

/**
 * SQLite date format pattern: "YYYY-MM-DD"
 * This matches SQLite's default date() function output format.
 */
@OptIn(FormatStringsInDatetimeFormats::class)
private val SQLITE_DATE_FORMAT = LocalDate.Format {
    byUnicodePattern("yyyy-MM-dd")
}

/**
 * SQLite time format pattern: "HH:MM:SS"
 * This matches SQLite's default time() function output format.
 */
@OptIn(FormatStringsInDatetimeFormats::class)
private val SQLITE_TIME_FORMAT = LocalTime.Format {
    byUnicodePattern("HH:mm:ss")
}

/**
 * Convert LocalDateTime to SQLite UTC timestamp such as "2025-05-26 19:43:13"
 *
 * @return SQLite-compatible datetime string in format "YYYY-MM-DD HH:MM:SS"
 */
fun LocalDateTime.toSqliteTimestamp(): String {
    return SQLITE_DATETIME_FORMAT.format(this)
}

/**
 * Convert SQLite UTC timestamp such as "2025-05-26 19:43:13" to LocalDateTime
 *
 * @param timestamp SQLite datetime string in format "YYYY-MM-DD HH:MM:SS"
 * @return LocalDateTime instance
 * @throws IllegalArgumentException if the timestamp format is invalid
 */
fun LocalDateTime.Companion.fromSqliteTimestamp(timestamp: String): LocalDateTime {
    return try {
        SQLITE_DATETIME_FORMAT.parse(timestamp)
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid SQLite timestamp format: '$timestamp'. Expected format: 'YYYY-MM-DD HH:MM:SS'", e)
    }
}

/**
 * Convert LocalDate to SQLite date string such as "2025-05-26"
 *
 * @return SQLite-compatible date string in format "YYYY-MM-DD"
 */
fun LocalDate.toSqliteDate(): String {
    return SQLITE_DATE_FORMAT.format(this)
}

/**
 * Convert SQLite date string such as "2025-05-26" to LocalDate
 *
 * @param date SQLite date string in format "YYYY-MM-DD"
 * @return LocalDate instance
 * @throws IllegalArgumentException if the date format is invalid
 */
fun LocalDate.Companion.fromSqliteDate(date: String): LocalDate {
    return try {
        SQLITE_DATE_FORMAT.parse(date)
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid SQLite date format: '$date'. Expected format: 'YYYY-MM-DD'", e)
    }
}

/**
 * Convert LocalTime to SQLite time string such as "19:43:13"
 *
 * @return SQLite-compatible time string in format "HH:MM:SS"
 */
fun LocalTime.toSqliteTime(): String {
    return SQLITE_TIME_FORMAT.format(this)
}

/**
 * Convert SQLite time string such as "19:43:13" to LocalTime
 *
 * @param time SQLite time string in format "HH:MM:SS"
 * @return LocalTime instance
 * @throws IllegalArgumentException if the time format is invalid
 */
fun LocalTime.Companion.fromSqliteTime(time: String): LocalTime {
    return try {
        SQLITE_TIME_FORMAT.parse(time)
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid SQLite time format: '$time'. Expected format: 'HH:MM:SS'", e)
    }
}
