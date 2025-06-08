package dev.goquick.sqlitenow.gradle.sqlite

/**
 * Translates a SQL statement into a Kotlin string with margin prefix.
 * This formats the SQL statement to be used in a Kotlin multi-line string with trimMargin().
 *
 * @param sql The SQL statement to translate
 * @return The SQL statement formatted as a Kotlin string with margin prefix
 */
fun translateSqliteStatementToKotlin(sql: String): String {
    // Normalize line endings to \n
    val normalizedSql = sql.replace("\r\n", "\n").replace("\r", "\n")

    // Split the SQL into lines
    val lines = normalizedSql.split("\n")

    // Process each line: trim whitespace and add margin prefix
    val formattedSql = lines
        .map { it.trim() }
        .filter { it.isNotEmpty() }
        .joinToString("\n|", "|")

    return formattedSql
}
