package dev.goquick.sqlitenow.oversqlite

internal actual fun realServerSmokeEnv(name: String): String? {
    val global = js("globalThis")
    val process = global.process
    val processValue = if (process != null && process.env != null) process.env[name] else null
    if (processValue != null) {
        return processValue.toString()
    }
    val globalValue = global[name]
    return globalValue?.toString()
}
