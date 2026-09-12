package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.sqlite.SQLiteStatement

internal actual fun observeR1LiveReplaySteps(
    statement: SQLiteStatement,
    onStep: () -> Unit,
): SQLiteStatement = object : SQLiteStatement by statement {
    override suspend fun step(): Boolean {
        onStep()
        return statement.step()
    }
}
