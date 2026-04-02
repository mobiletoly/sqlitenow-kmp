import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsEnvSpec
import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsRootPlugin
import org.gradle.api.tasks.Exec

plugins {
    alias(libs.plugins.androidLibrary) apply false
    alias(libs.plugins.androidApplication) apply false
    alias(libs.plugins.androidKotlinMultiplatformLibrary) apply false
    alias(libs.plugins.jetbrainsCompose) apply false
    alias(libs.plugins.composeCompiler) apply false
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.mavenPublish) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.serialization) apply false
    alias(libs.plugins.kotlin.android) apply false
}

// Kotlin/JS uses Yarn v1 internally; with Node 24+ it emits `url.parse()` deprecation warnings during `kotlinNpmInstall`.
// Pin Node to LTS for quieter, reproducible builds.
plugins.withType<NodeJsRootPlugin> {
    extensions.configure<NodeJsEnvSpec> {
        version.convention("22.0.0")
    }
}

fun registerOversqliteExecTask(
    name: String,
    description: String,
    env: Map<String, String> = emptyMap(),
    arguments: List<String>,
) {
    tasks.register<Exec>(name) {
        group = "verification"
        this.description = description
        workingDir = rootDir
        executable = rootProject.file("gradlew").absolutePath
        args(arguments + "--no-daemon")
        environment(env)
    }
}

fun registerCorePlatformExecTask(
    name: String,
    description: String,
    arguments: List<String>,
) {
    tasks.register<Exec>(name) {
        group = "verification"
        this.description = description
        workingDir = rootDir
        executable = rootProject.file("gradlew").absolutePath
        args(arguments + "--no-daemon")
    }
}

tasks.register("oversqliteComprehensive") {
    group = "verification"
    description = "Runs the host-side oversqlite comprehensive suite."
    dependsOn(":library:oversqliteComprehensiveJvm")
}

tasks.register("oversqlitePlatformAll") {
    group = "verification"
    description = "Runs the oversqlite platform suite across all configured runtime surfaces."
    dependsOn(
        "oversqlitePlatformAndroid",
        "oversqlitePlatformJvm",
        "oversqlitePlatformIosSimulatorArm64",
        "oversqlitePlatformMacosArm64",
        "oversqlitePlatformJsNode",
        "oversqlitePlatformWasmBrowser",
    )
}

tasks.register("corePlatformAll") {
    group = "verification"
    description = "Runs the local-only SQLiteNow core platform harness across all configured runtime surfaces."
    dependsOn(
        "corePlatformAndroid",
        "corePlatformJvm",
        "corePlatformIosSimulatorArm64",
        "corePlatformMacosArm64",
        "corePlatformJsNode",
        "corePlatformWasmBrowser",
    )
}

registerCorePlatformExecTask(
    name = "corePlatformAndroid",
    description = "Runs the Android SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:connectedAndroidDeviceTest"),
)

registerCorePlatformExecTask(
    name = "corePlatformJvm",
    description = "Runs the JVM SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:jvmTest"),
)

registerCorePlatformExecTask(
    name = "corePlatformIosSimulatorArm64",
    description = "Runs the iOS simulator SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:iosSimulatorArm64Test"),
)

registerCorePlatformExecTask(
    name = "corePlatformMacosArm64",
    description = "Runs the macOS SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:macosArm64Test"),
)

registerCorePlatformExecTask(
    name = "corePlatformJsNode",
    description = "Runs the JS Node SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:jsNodeTest"),
)

registerCorePlatformExecTask(
    name = "corePlatformWasmBrowser",
    description = "Runs the Wasm browser SQLiteNow core platform harness.",
    arguments = listOf(":platform-core-test:harness:wasmJsBrowserTest"),
)

tasks.register("oversqliteRealserverJvm") {
    group = "verification"
    description = "Runs the shared JVM oversqlite realserver suite."
    dependsOn(":library:oversqliteRealserverJvm")
}

tasks.register("oversqliteRealserverJvmHeavy") {
    group = "verification"
    description = "Runs the JVM heavy oversqlite realserver scenario."
    dependsOn(":library:jvmRealServerSharedConnectionStress")
}

tasks.register("oversqliteRealserverAll") {
    group = "verification"
    description = "Runs the oversqlite realserver suite across all configured runtime surfaces."
    dependsOn(
        "oversqliteRealserverJvm",
        "oversqliteRealserverAndroid",
        "oversqliteRealserverJvmHarness",
        "oversqliteRealserverIosSimulatorArm64",
        "oversqliteRealserverMacosArm64",
        "oversqliteRealserverJsNode",
        "oversqliteRealserverWasmBrowser",
    )
}

tasks.register("oversqliteRealserverAllHeavy") {
    group = "verification"
    description = "Runs the oversqlite realserver suite in heavy mode across all configured runtime surfaces."
    dependsOn(
        "oversqliteRealserverJvm",
        "oversqliteRealserverJvmHeavy",
        "oversqliteRealserverAndroidHeavy",
        "oversqliteRealserverJvmHarnessHeavy",
        "oversqliteRealserverIosSimulatorArm64Heavy",
        "oversqliteRealserverMacosArm64Heavy",
        "oversqliteRealserverJsNodeHeavy",
        "oversqliteRealserverWasmBrowserHeavy",
    )
}

registerOversqliteExecTask(
    name = "oversqlitePlatformAndroid",
    description = "Runs the Android oversqlite platform suite.",
    arguments =
        listOf(
            ":platform-oversqlite-test:composeApp:connectedAndroidDeviceTest",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_PLATFORM_TESTS=true",
        ),
)

registerOversqliteExecTask(
    name = "oversqlitePlatformJvm",
    description = "Runs the JVM oversqlite platform suite.",
    env = mapOf("OVERSQLITE_PLATFORM_TESTS" to "true"),
    arguments = listOf(":platform-oversqlite-test:composeApp:jvmTest"),
)

registerOversqliteExecTask(
    name = "oversqlitePlatformIosSimulatorArm64",
    description = "Runs the iOS simulator oversqlite platform suite.",
    env = mapOf("SIMCTL_CHILD_OVERSQLITE_PLATFORM_TESTS" to "true"),
    arguments = listOf(":platform-oversqlite-test:composeApp:iosSimulatorArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqlitePlatformMacosArm64",
    description = "Runs the macOS oversqlite platform suite.",
    env = mapOf("OVERSQLITE_PLATFORM_TESTS" to "true"),
    arguments = listOf(":platform-oversqlite-test:composeApp:macosArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqlitePlatformJsNode",
    description = "Runs the JS Node oversqlite platform suite.",
    env = mapOf("OVERSQLITE_PLATFORM_TESTS" to "true"),
    arguments = listOf(":platform-oversqlite-test:composeApp:jsNodeTest"),
)

registerOversqliteExecTask(
    name = "oversqlitePlatformWasmBrowser",
    description = "Runs the Wasm browser oversqlite platform suite.",
    env = mapOf("OVERSQLITE_PLATFORM_TESTS" to "true"),
    arguments = listOf(":platform-oversqlite-test:composeApp:wasmJsBrowserTest"),
)

val hostRealserverBaseUrl = System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL") ?: "http://localhost:8080"
val androidRealserverBaseUrl =
    System.getenv("OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL") ?: "http://10.0.2.2:8080"

registerOversqliteExecTask(
    name = "oversqliteRealserverAndroid",
    description = "Runs the Android oversqlite realserver suite.",
    arguments =
        listOf(
            ":platform-oversqlite-test:composeApp:connectedAndroidDeviceTest",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REALSERVER_TESTS=true",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=$androidRealserverBaseUrl",
        ),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverAndroidHeavy",
    description = "Runs the Android oversqlite realserver suite in heavy mode.",
    arguments =
        listOf(
            ":platform-oversqlite-test:composeApp:connectedAndroidDeviceTest",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REALSERVER_TESTS=true",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REALSERVER_HEAVY=true",
            "-Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=$androidRealserverBaseUrl",
        ),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverJvmHarness",
    description = "Runs the JVM oversqlite realserver harness suite.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:jvmTest"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverJvmHarnessHeavy",
    description = "Runs the JVM oversqlite realserver harness suite in heavy mode.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REALSERVER_HEAVY" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:jvmTest"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverIosSimulatorArm64",
    description = "Runs the iOS simulator oversqlite realserver suite.",
    env =
        mapOf(
            "SIMCTL_CHILD_OVERSQLITE_REALSERVER_TESTS" to "true",
            "SIMCTL_CHILD_OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:iosSimulatorArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverIosSimulatorArm64Heavy",
    description = "Runs the iOS simulator oversqlite realserver suite in heavy mode.",
    env =
        mapOf(
            "SIMCTL_CHILD_OVERSQLITE_REALSERVER_TESTS" to "true",
            "SIMCTL_CHILD_OVERSQLITE_REALSERVER_HEAVY" to "true",
            "SIMCTL_CHILD_OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:iosSimulatorArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverMacosArm64",
    description = "Runs the macOS oversqlite realserver suite.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:macosArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverMacosArm64Heavy",
    description = "Runs the macOS oversqlite realserver suite in heavy mode.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REALSERVER_HEAVY" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:macosArm64Test"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverJsNode",
    description = "Runs the JS Node oversqlite realserver suite.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:jsNodeTest"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverJsNodeHeavy",
    description = "Runs the JS Node oversqlite realserver suite in heavy mode.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REALSERVER_HEAVY" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:jsNodeTest"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverWasmBrowser",
    description = "Runs the Wasm browser oversqlite realserver suite.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:wasmJsBrowserTest"),
)

registerOversqliteExecTask(
    name = "oversqliteRealserverWasmBrowserHeavy",
    description = "Runs the Wasm browser oversqlite realserver suite in heavy mode.",
    env =
        mapOf(
            "OVERSQLITE_REALSERVER_TESTS" to "true",
            "OVERSQLITE_REALSERVER_HEAVY" to "true",
            "OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL" to hostRealserverBaseUrl,
        ),
    arguments = listOf(":platform-oversqlite-test:composeApp:wasmJsBrowserTest"),
)
