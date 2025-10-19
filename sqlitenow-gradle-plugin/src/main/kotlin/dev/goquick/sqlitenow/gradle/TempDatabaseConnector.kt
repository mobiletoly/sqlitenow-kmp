/*
 * Copyright 2025 Toly Pochkin
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

import java.sql.Connection
import java.sql.DriverManager
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging

/**
 * Specifies the type of storage to use for the database.
 */
internal sealed class MigratorTempStorage {
    /** Use an in-memory database */
    internal object Memory : MigratorTempStorage()

    /** Use a file-backed database */
    internal data class File(val file: java.io.File) : MigratorTempStorage()
}

internal class TempDatabaseConnector(
    storage: MigratorTempStorage,
    logger: Logger = Logging.getLogger(TempDatabaseConnector::class.java)
) {
    val connection: Connection

    init {
        connection = when (storage) {
            is MigratorTempStorage.Memory -> {
                // Create an in-memory database
                createDatabaseWithJdbcUrl("jdbc:sqlite::memory:")
            }
            is MigratorTempStorage.File -> {
                val dbFile = storage.file

                // Delete the file if it exists
                if (dbFile.exists()) {
                    dbFile.delete()
                }

                logger.lifecycle("Creating database file: ${dbFile.absolutePath}")

                // Create a file-backed database
                createDatabaseWithJdbcUrl("jdbc:sqlite:${dbFile.absolutePath}")
            }
        }
    }

    private fun createDatabaseWithJdbcUrl(jdbcUrl: String): Connection {
        // Load the SQLite JDBC driver
        Class.forName("org.sqlite.JDBC")

        // Create a connection to the database
        val conn: Connection = DriverManager.getConnection(jdbcUrl)
        conn.autoCommit = false

        // Enable foreign keys
        conn.createStatement().use { stmt ->
            stmt.execute("PRAGMA foreign_keys = ON;")
        }

        return conn
    }

    fun close() {
        connection.close()
    }
}
