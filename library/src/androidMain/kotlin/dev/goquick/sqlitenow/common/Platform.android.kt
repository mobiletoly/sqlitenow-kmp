/*
 * Copyright 2025 Toly Pochkin
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
package dev.goquick.sqlitenow.common

import android.content.Context
import java.io.File

private lateinit var appContext: Context

fun setupAndroidAppContext(context: Context) {
    appContext = context
}

/**
 * Returns the path where the database should be stored on Android.
 * Uses the app's database directory.
 *
 * @param appName Ignored on Android (app sandbox already scopes the path; used on JVM only).
 */
actual fun resolveDatabasePath(dbName: String, appName: String): String {
    return appContext.getDatabasePath(dbName).absolutePath
}

internal actual fun validateFileExists(path: String): Boolean {
    return File(path).exists()
}

actual fun platform(): PlatformType {
    return PlatformType.ANDROID
}
