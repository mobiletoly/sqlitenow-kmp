package dev.goquick.sqlitenow.oversqlite

internal actual fun realServerEnv(name: String): String? {
    val global = js("globalThis")
    val process = global.process
    val processValue = if (process != null && process.env != null) process.env[name] else null
    if (processValue != null) {
        return processValue.toString()
    }
    val globalValue = global[name]
    if (globalValue != null) return globalValue.toString()
    val settings = global.__karma__?.config?.baselineLiveSettings
    val karmaValue = if (settings != null) settings[name] else null
    return karmaValue?.toString()
}
