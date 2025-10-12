package dev.goquick.sqlitenow.core

import kotlinx.coroutines.CoroutineDispatcher

internal expect fun sqliteConnectionDispatcher(): CoroutineDispatcher
internal expect fun sqliteNetworkDispatcher(): CoroutineDispatcher
