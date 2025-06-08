package dev.goquick.sqlitenow.gradle.sqlite

/**
 * Represents a single SQL statement with its associated comments.
 */
data class SqlSingleStatement(
    /** List of comments that appear before the SQL statement */
    val topComments: List<String>,

    /** List of comments that appear inside the SQL statement */
    val innerComments: List<String>,

    /** The original SQL statement text */
    val sql: String
)
