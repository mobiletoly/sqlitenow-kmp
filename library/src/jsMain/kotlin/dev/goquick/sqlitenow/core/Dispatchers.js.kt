package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

internal actual fun sqliteConnectionDispatcher(): CoroutineDispatcher = Dispatchers.Default
internal actual fun sqliteNetworkDispatcher(): CoroutineDispatcher = Dispatchers.Default
