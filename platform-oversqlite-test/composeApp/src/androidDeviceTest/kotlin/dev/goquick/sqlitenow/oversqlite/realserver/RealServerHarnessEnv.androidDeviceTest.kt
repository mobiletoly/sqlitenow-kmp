package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.test.platform.app.InstrumentationRegistry

internal actual fun realServerEnv(name: String): String? =
    InstrumentationRegistry.getArguments().getString(name) ?: System.getenv(name)
