package dev.goquick.sqlitenow.common

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PlatformPathResolutionTest {

    @Test
    fun resolveDatabasePathUsesExpectedShape() {
        val dbName = "platform-path-test.db"
        val appName = "SqliteNowCommonTest"
        val path = resolveDatabasePath(dbName, appName)
        val normalizedPath = path.replace('\\', '/')

        when (platform()) {
            PlatformType.JS -> assertEquals(dbName, path)
            PlatformType.ANDROID, PlatformType.IOS ->
                assertTrue(normalizedPath.endsWith("/$dbName") || normalizedPath == dbName)

            PlatformType.JVM, PlatformType.MACOS, PlatformType.LINUX ->
                assertTrue(normalizedPath.endsWith("/$appName/$dbName"))
        }
    }
}
