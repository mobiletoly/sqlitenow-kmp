package dev.goquick.sqlitenow.core

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFailsWith

class BundledOpenJvmHostTest {

    @Test
    fun intelMacHostsAreRejected() {
        val scenarios = listOf(
            HostScenario("Mac OS X x86_64", "Mac OS X", "x86_64"),
            HostScenario("macOS amd64", "macOS", "amd64"),
            HostScenario("Darwin x86", "Darwin", "x86"),
        )

        scenarios.forEach { scenario ->
            val failure = assertFailsWith<UnsupportedOperationException>(scenario.name) {
                requireSupportedJvmHost(scenario.osName, scenario.osArch)
            }
            assertContains(
                failure.message.orEmpty(),
                "does not support Intel macOS",
                message = scenario.name,
            )
        }
    }

    @Test
    fun supportedJvmHostsAreAccepted() {
        val scenarios = listOf(
            HostScenario("Mac OS X arm64", "Mac OS X", "arm64"),
            HostScenario("Darwin aarch64", "Darwin", "aarch64"),
            HostScenario("Linux x86_64", "Linux", "x86_64"),
            HostScenario("Windows amd64", "Windows 11", "amd64"),
        )

        scenarios.forEach { scenario ->
            requireSupportedJvmHost(scenario.osName, scenario.osArch)
        }
    }

    private data class HostScenario(
        val name: String,
        val osName: String,
        val osArch: String,
    )
}
