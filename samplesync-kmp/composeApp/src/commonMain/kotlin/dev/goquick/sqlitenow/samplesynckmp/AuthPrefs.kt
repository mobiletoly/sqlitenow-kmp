/*
 * Copyright 2025 Anatoliy Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
