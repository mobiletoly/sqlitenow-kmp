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
package dev.goquick.sqlitenow.common

import kotlinx.cinterop.ExperimentalForeignApi
import platform.Foundation.NSDocumentDirectory
import platform.Foundation.NSFileManager
import platform.Foundation.NSUserDomainMask

@OptIn(ExperimentalForeignApi::class)
actual fun resolveDatabasePath(dbName: String): String {
    val url = NSFileManager.defaultManager
        .URLForDirectory(
            NSDocumentDirectory, NSUserDomainMask,
            null,        // no specific domain
            true, null   // create, no error pointer
        )!!
        .URLByAppendingPathComponent(dbName)

    return url!!.path!!
}

internal actual fun validateFileExists(path: String): Boolean {
    return NSFileManager.defaultManager.fileExistsAtPath(path)
}

actual fun platform(): PlatformType {
    return PlatformType.IOS
}
