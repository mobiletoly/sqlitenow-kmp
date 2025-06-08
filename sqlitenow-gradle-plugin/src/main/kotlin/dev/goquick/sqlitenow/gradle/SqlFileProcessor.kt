package dev.goquick.sqlitenow.gradle

import dev.goquick.sqlitenow.gradle.sqlite.SqlSingleStatement
import dev.goquick.sqlitenow.gradle.sqlite.parseSqlStatements
import java.io.File

/**
 * Utility class for common SQL file processing operations.
 * Eliminates duplicate SQL file parsing logic across multiple inspector classes.
 */
object SqlFileProcessor {

    /**
     * Finds all .sql files in a directory, sorted by name.
     * Centralizes the common pattern used across all inspector classes.
     *
     * @param directory The directory to scan for SQL files
     * @return List of SQL files sorted by name, empty if directory doesn't exist
     */
    fun findSqlFiles(directory: File): List<File> {
        return directory.listFiles { file ->
            file.isFile && file.extension.equals("sql", ignoreCase = true)
        }?.sortedBy { it.name } ?: emptyList()
    }

    /**
     * Parses all SQL files and combines their statements.
     * Centralizes the common pattern of reading files and parsing SQL statements.
     *
     * @param sqlFiles List of SQL files to parse
     * @return Combined list of all SQL statements from all files
     */
    fun parseAllSqlFiles(sqlFiles: List<File>): List<SqlSingleStatement> {
        val allStatements = mutableListOf<SqlSingleStatement>()
        sqlFiles.forEach { sqlFile ->
            val sqlContent = sqlFile.readText()
            val fileStatements = parseSqlStatements(sqlContent)
            allStatements.addAll(fileStatements)
        }
        return allStatements
    }

    /**
     * Parses all SQL files in a directory and returns combined statements.
     * Convenience method that combines findSqlFiles and parseAllSqlFiles.
     *
     * @param directory The directory containing SQL files
     * @return Combined list of all SQL statements from all files in the directory
     */
    fun parseAllSqlFilesInDirectory(directory: File): List<SqlSingleStatement> {
        val sqlFiles = findSqlFiles(directory)
        return parseAllSqlFiles(sqlFiles)
    }

    /**
     * Validates that a directory exists and is actually a directory.
     * Centralizes the common validation pattern used across all inspector classes.
     *
     * @param directory The directory to validate
     * @param name Descriptive name for error messages (e.g., "Schema", "Migration")
     * @throws IllegalArgumentException if directory doesn't exist or isn't a directory
     */
    fun validateDirectory(directory: File, name: String, mandatory: Boolean) {
        if (!mandatory && !directory.exists()) {
            return
        }
        if (!directory.exists()) {
            throw IllegalArgumentException("$name directory does not exist: ${directory.absolutePath}")
        }
        if (!directory.isDirectory) {
            throw IllegalArgumentException("Path is not a directory: ${directory.absolutePath}")
        }
    }
}
