package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO

internal actual fun sqliteConnectionDispatcher(): CoroutineDispatcher =
    Dispatchers.IO.limitedParallelism(1)

internal actual fun sqliteNetworkDispatcher(): CoroutineDispatcher = Dispatchers.IO
