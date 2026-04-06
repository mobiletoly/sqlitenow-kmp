package dev.goquick.sqlitenow.common

import androidx.test.platform.app.InstrumentationRegistry

internal actual fun initializePlatformTestContext() {
    setupAndroidAppContext(InstrumentationRegistry.getInstrumentation().targetContext.applicationContext)
}
