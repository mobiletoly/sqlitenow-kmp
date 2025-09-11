package dev.goquick.sqlitenow.samplesynckmp

import com.russhwolf.settings.Settings

// Simple auth preferences backed by Multiplatform Settings
object AuthPrefs {
    private val settings: Settings = Settings()

    fun get(key: String): String? = settings.getStringOrNull(key)
    fun set(key: String, value: String) { settings.putString(key, value) }
    fun remove(key: String) { settings.remove(key) }
}

object AuthKeys {
    const val Username = "username"
    const val Token = "token"
    const val DeviceId = "device_id"
}
