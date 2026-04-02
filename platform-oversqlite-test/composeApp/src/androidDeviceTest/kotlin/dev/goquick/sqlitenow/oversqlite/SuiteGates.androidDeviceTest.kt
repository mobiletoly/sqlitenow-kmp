package dev.goquick.sqlitenow.oversqlite

import androidx.test.platform.app.InstrumentationRegistry

internal actual fun suiteEnv(name: String): String? =
    InstrumentationRegistry.getArguments().getString(name) ?: System.getenv(name)
