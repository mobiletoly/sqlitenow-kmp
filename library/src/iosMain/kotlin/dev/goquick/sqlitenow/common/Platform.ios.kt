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

internal actual fun platform(): PlatformType {
    return PlatformType.IOS
}
