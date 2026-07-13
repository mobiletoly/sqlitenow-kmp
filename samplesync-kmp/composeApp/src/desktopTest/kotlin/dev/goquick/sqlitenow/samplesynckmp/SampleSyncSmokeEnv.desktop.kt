package dev.goquick.sqlitenow.samplesynckmp

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class SampleSyncNumericSmokeTest {
    @Test
    fun generatedFactory_syncsNonNullSsnScoreAndIsActive() = runTest {
        SampleSyncNumericSmokeSupport.run(
            baseUrl = System.getenv("SAMPLESYNC_REAL_SERVER_BASE_URL") ?: "http://localhost:8080",
            platform = "desktop",
        )
    }
}
