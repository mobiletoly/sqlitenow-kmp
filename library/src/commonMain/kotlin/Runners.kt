package dev.goquick.sqlitenow.core

import kotlinx.coroutines.flow.Flow

interface SelectRunners<T : Any> {
    suspend fun asList(): List<T>
    suspend fun asOne(): T
    suspend fun asOneOrNull(): T?
    fun asFlow(): Flow<List<T>>
}

interface ExecuteRunners {
    suspend fun execute()
}
