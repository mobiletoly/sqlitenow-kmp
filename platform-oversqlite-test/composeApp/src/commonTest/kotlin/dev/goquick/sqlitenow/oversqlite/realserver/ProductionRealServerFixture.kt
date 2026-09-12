package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.platform.PlatformTestDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase

internal val productionRealServerSyncTables = RealServerGeneratedDatabase.syncTables

internal suspend fun createProductionRealServerTables(db: SafeSQLiteConnection) {
    createProductionRealServerTables { statement -> db.execSQL(statement) }
}

internal suspend fun createProductionRealServerTables(db: PlatformTestDatabase) {
    createProductionRealServerTables { statement -> db.execSQL(statement) }
}

private suspend fun createProductionRealServerTables(exec: suspend (String) -> Unit) {
    exec(
        """
        CREATE TABLE users (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE categories (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE teams (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            captain_member_id TEXT REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE team_members (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE files (
            id BLOB PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            data BLOB NOT NULL
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE file_reviews (
            id BLOB PRIMARY KEY NOT NULL,
            review TEXT NOT NULL,
            file_id BLOB NOT NULL REFERENCES files(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE typed_rows (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            note TEXT NULL,
            count_value INTEGER NULL,
            small_count INTEGER NULL,
            medium_count INTEGER NULL,
            exact_amount TEXT NULL,
            enabled_flag INTEGER NOT NULL,
            rating REAL NULL,
            float4_value REAL NULL,
            data BLOB NULL,
            created_at TEXT NULL
        )
        """.trimIndent(),
    )
    exec(
        """
        CREATE TABLE posts (
            id TEXT PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            author_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """.trimIndent(),
    )
}
