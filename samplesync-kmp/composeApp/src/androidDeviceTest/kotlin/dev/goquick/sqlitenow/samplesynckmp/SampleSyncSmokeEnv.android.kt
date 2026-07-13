package dev.goquick.sqlitenow.samplesynckmp

import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class SampleSyncNumericSmokeTest {
    @Test
    fun generatedFactory_syncsNonNullSsnScoreAndIsActive() = runTest {
        SampleSyncNumericSmokeSupport.run(
            baseUrl = "http://10.0.2.2:8080",
            platform = "android",
        )
    }
}
