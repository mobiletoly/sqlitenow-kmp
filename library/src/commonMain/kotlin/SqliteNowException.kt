package dev.goquick.sqlitenow.core

class SqliteNowException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(
    message,
    cause
)
