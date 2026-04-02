@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite

import kotlinx.cinterop.toKString
import platform.posix.getenv

internal actual fun suiteEnv(name: String): String? =
    getenv(name)?.toKString()
