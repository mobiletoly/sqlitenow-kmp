@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.oversqlite

import kotlin.JsFun

@JsFun(
    """
    (name) => {
      const g = globalThis;
      if (typeof process !== 'undefined' && process?.env && process.env[name] != null) {
        return String(process.env[name]);
      }
      if (g && g[name] != null) {
        return String(g[name]);
      }
      return null;
    }
    """,
)
private external fun readWasmSmokeEnv(name: String): String?

internal actual fun realServerEnv(name: String): String? = readWasmSmokeEnv(name)
