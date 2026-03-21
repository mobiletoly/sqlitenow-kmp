package dev.goquick.sqlitenow.common

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PlatformPathResolutionTest {

    @Test
    fun resolveDatabasePathUsesExpectedShape() {
        initializePlatformTestContext()
        val dbName = "platform-path-test.db"
        val appName = "SqliteNowCommonTest"
        val path = resolveDatabasePath(dbName, appName)
        val normalizedPath = path.replace('\\', '/')

        when (platform()) {
            PlatformType.JS -> assertEquals(dbName, path)
            PlatformType.ANDROID, PlatformType.IOS -> {
                assertTrue(normalizedPath.endsWith("/$dbName"))
                assertFalse(normalizedPath.contains("/$appName/"))
                assertFalse(normalizedPath == dbName)
            }

            PlatformType.JVM, PlatformType.MACOS, PlatformType.LINUX -> {
                assertTrue(normalizedPath.endsWith("/$appName/$dbName"))
                assertTrue(normalizedPath.contains("/$appName/"))
            }
        }
    }
}
