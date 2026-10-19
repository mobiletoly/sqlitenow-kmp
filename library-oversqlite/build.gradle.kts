@file:OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)

import java.time.Duration

import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.androidKotlinMultiplatformLibrary.get().pluginId)
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id(libs.plugins.mavenPublish.get().pluginId)
}

group = providers.gradleProperty("sqlitenow.group").get()
version = providers.gradleProperty("sqlitenow.version").get()

val isPublishingToMavenLocal =
    gradle.startParameter.taskNames.any { it.contains("publishToMavenLocal", ignoreCase = true) }
val hasSigningCredentials =
    providers.environmentVariable("SIGNING_KEY").isPresent ||
        providers.environmentVariable("SIGNING_KEY_ID").isPresent ||
        providers.environmentVariable("SIGNING_PASSWORD").isPresent ||
        providers.gradleProperty("signingInMemoryKey").isPresent ||
        providers.gradleProperty("signing.keyId").isPresent ||
        providers.gradleProperty("signing.password").isPresent ||
        providers.gradleProperty("signing.secretKeyRingFile").isPresent ||
        providers.gradleProperty("signing.gnupg.keyName").isPresent

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()
    jvm()

    android {
        namespace = "dev.goquick.sqlitenow.oversqlite"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }

        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.addAll("-Xexpect-actual-classes")
    }

    js {
        browser()
        nodejs {
            testTask {
                useMocha {
                    timeout = "30s"
                }
            }
        }
        binaries.library()
    }

    wasmJs {
        browser()
        binaries.library()
    }

    iosArm64()
    iosSimulatorArm64()
    macosArm64()
    linuxArm64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            api(project(":library-core"))
            api(libs.kotlinx.coroutines.core)
            api(libs.kotlinx.datetime)
            api(libs.kotlinx.serialization.json)
            api(libs.kermit)
            api(libs.ktor.client.core)
            api(libs.ktor.client.content.negotiation)
            implementation("io.ktor:ktor-client-encoding:${libs.versions.ktor.get()}")
            api(libs.ktor.client.auth)
            api(libs.ktor.serialization.kotlinx.json)
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
            implementation("io.ktor:ktor-client-mock:${libs.versions.ktor.get()}")
//                implementation(libs.kotlin.test)
//                implementation(libs.kotlinx.coroutines.test)
//                implementation(kotlin("test"))
//            }
        }

        val nativeMain by getting {
            dependencies {
                implementation(libs.sqlite.bundled)
            }
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.bundled)
        }

        jvmTest {
            kotlin.srcDir("src/jvmAndroidTest/kotlin")
            dependencies {
                implementation("io.ktor:ktor-client-cio:${libs.versions.ktor.get()}")
                implementation(libs.ktor.client.okhttp)
                implementation("org.yaml:snakeyaml:2.7")
            }
        }

        androidMain.dependencies {
            implementation(libs.sqlite.bundled)
            implementation(libs.ktor.client.okhttp)
        }

        getByName("androidDeviceTest") {
            kotlin.srcDir("src/jvmAndroidTest/kotlin")
            dependencies {
                implementation(libs.androidx.test.runner)
                implementation(libs.androidx.test.rules)
                implementation(libs.androidx.junit)
                implementation(libs.kotlinx.coroutines.test)
            }
        }

        iosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }

        macosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }

        val linuxMain by getting {
            dependencies {
                implementation(libs.ktor.client.curl)
            }
        }

        jsMain.dependencies {
            implementation(libs.ktor.client.js)
            implementation(libs.kotlinx.browser)
        }

        val wasmJsMain by getting {
            languageSettings {
                optIn("kotlin.js.ExperimentalWasmJsInterop")
            }
            dependencies {
                implementation(libs.kotlinx.coroutines.core.wasm.js)
            }
            resources.srcDir("src/wasmJsMain/resources")
        }
    }
}

val sqliteWorkerResourceNamespace = "sqlitenow-worker-v1"
mapOf(
    "jsTestProcessResources" to "jsProcessResources",
    "wasmJsTestProcessResources" to "wasmJsProcessResources",
).forEach { (testResourcesTaskName, coreResourcesTaskName) ->
    val coreResources =
        project(":library-core").tasks.named<ProcessResources>(coreResourcesTaskName)

    tasks.named<ProcessResources>(testResourcesTaskName) {
        dependsOn(coreResources)
        from(coreResources.map { it.destinationDir }) {
            include("$sqliteWorkerResourceNamespace/**")
            // The private test copy has W1 telemetry; published Core resources stay intact.
            exclude("$sqliteWorkerResourceNamespace/client.mjs")
        }
    }
}

val w1TestFixtureDir = layout.buildDirectory.dir("generated/w1-test-fixtures")
val generateW1TestFixtures by tasks.registering(Exec::class) {
    dependsOn(project(":library-core").tasks.named("copySqliteWorkerPublicationAssets"))
    inputs.dir(layout.projectDirectory.dir("src/webTest/fixtures"))
    inputs.dir(project(":library-core").layout.projectDirectory.dir("src/webMain/resources/sqlitenow-worker-v1"))
    inputs.dir(project(":library-core").layout.buildDirectory.dir("generated/sqlite-worker/resources"))
    outputs.dir(w1TestFixtureDir)
    commandLine("node", project.file("src/webTest/fixtures/generate-w1-fixtures.mjs"))
}
listOf("jsTestProcessResources", "wasmJsTestProcessResources").forEach { taskName ->
    tasks.named<ProcessResources>(taskName) {
        dependsOn(generateW1TestFixtures)
        from(w1TestFixtureDir)
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

val jvmTest by tasks.existing(Test::class)

tasks.withType<Test>().configureEach {
    systemProperty("coordinatedopening.jvmTest.classpath", jvmTest.get().classpath.asPath)
}

jvmTest.configure {
    filter {
        excludeTestsMatching("dev.goquick.sqlitenow.oversqlite.GoSwaggerConformanceTest")
    }
}

tasks.register<Test>("oversqliteComprehensiveJvm") {
    group = "verification"
    description = "Runs the host-side oversqlite comprehensive suite on the JVM target."

    dependsOn(tasks.named("jvmTestClasses"))

    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()

    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.*")
        excludeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServer*")
        excludeTestsMatching("dev.goquick.sqlitenow.oversqlite.GoSwaggerConformanceTest")
    }

    outputs.upToDateWhen { false }
    shouldRunAfter(jvmTest)
}

tasks.register<Test>("oversqliteCrossRepoConformanceJvm") {
    group = "verification"
    description = "Runs the named JVM consumer of Go-owned normative Swagger."

    dependsOn(tasks.named("jvmTestClasses"))
    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()
    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.GoSwaggerConformanceTest")
        isFailOnNoMatchingTests = true
    }
    outputs.upToDateWhen { false }
}

data class SnapshotMemoryProfile(
    val taskName: String,
    val rows: Int,
    val rowBytes: Int,
    val runLabel: String,
    val profileSet: String,
)

tasks.register<Test>("oversqliteSnapshotMemorySamplerControl") {
    group = "verification"
    description = "Runs the local-only five-second snapshot sampler control in a fresh JVM."
    maxHeapSize = "128m"
    dependsOn(tasks.named("jvmTestClasses"))
    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()
    filter {
        includeTestsMatching(
            "dev.goquick.sqlitenow.oversqlite.SnapshotMemoryBaselineTest.snapshotMemorySamplerOnlyControl",
        )
    }
    environment("OVERSQLITE_MEMORY_SAMPLER_CONTROL", "true")
    testLogging.showStandardStreams = true
    outputs.upToDateWhen { false }
}

val snapshotMemoryClosureProfiles = listOf(
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure10k256", 10_000, 256, "closure-10k-256", "closure"),
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure10k1k", 10_000, 1024, "closure-10k-1k", "closure"),
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure50k256", 50_000, 256, "closure-50k-256", "closure"),
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure50k1k", 50_000, 1024, "closure-50k-1k", "closure"),
)
val snapshotMemoryFutureProfiles = listOf(
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure100k256", 100_000, 256, "closure-100k-256", "future"),
    SnapshotMemoryProfile("oversqliteSnapshotMemoryClosure100k1k", 100_000, 1024, "closure-100k-1k", "future"),
)
val snapshotMemoryExtendedProfiles = listOf(
    SnapshotMemoryProfile("oversqliteSnapshotMemory10k256", 10_000, 256, "representative-10k-256", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory10k1k", 10_000, 1024, "representative-10k-1k", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Warmup", 100_000, 256, "warmup-100k-256", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run1", 100_000, 256, "measured-100k-256-1", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run2", 100_000, 256, "measured-100k-256-2", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run3", 100_000, 256, "measured-100k-256-3", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k1k", 100_000, 1024, "measured-100k-1k", "extended"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory1m256", 1_000_000, 256, "measured-1m-256", "extended"),
)
val snapshotMemoryProfiles = snapshotMemoryClosureProfiles + snapshotMemoryFutureProfiles + snapshotMemoryExtendedProfiles
val snapshotMemoryResultDirectory = layout.buildDirectory.dir("snapshot-memory-results")

val previousSnapshotMemoryTaskBySet = mutableMapOf<String, String>()
snapshotMemoryProfiles.forEach { profile ->
    val predecessor = previousSnapshotMemoryTaskBySet[profile.profileSet]
    val resultFile = snapshotMemoryResultDirectory.map {
        it.file("${profile.profileSet}/${profile.runLabel}.properties")
    }
    tasks.register<Test>(profile.taskName) {
        group = "verification"
        description = "Runs the local-heavy ${profile.runLabel} compact snapshot profile in a fresh JVM."
        maxHeapSize = "128m"
        timeout.set(Duration.ofHours(2))
        dependsOn(tasks.named("jvmTestClasses"))
        testClassesDirs = jvmTest.get().testClassesDirs
        classpath = jvmTest.get().classpath
        useJUnit()
        filter {
            includeTestsMatching(
                "dev.goquick.sqlitenow.oversqlite.SnapshotMemoryBaselineTest.snapshotMemoryBaseline",
            )
        }
        environment("OVERSQLITE_MEMORY_BASELINE_ROWS", profile.rows.toString())
        environment("OVERSQLITE_MEMORY_ROW_BYTES", profile.rowBytes.toString())
        environment("OVERSQLITE_MEMORY_RUN_LABEL", profile.runLabel)
        environment("OVERSQLITE_SNAPSHOT_PROFILE_SET", profile.profileSet)
        environment("OVERSQLITE_MEMORY_RESULT_FILE", resultFile.get().asFile.absolutePath)
        environment("OVERSQLITE_MEMORY_SERVER_CLASSPATH", jvmTest.get().classpath.asPath)
        doFirst {
            resultFile.get().asFile.delete()
        }
        outputs.file(resultFile)
        testLogging.showStandardStreams = true
        outputs.upToDateWhen { false }
        predecessor?.let { mustRunAfter(tasks.named(it)) }
    }
    previousSnapshotMemoryTaskBySet[profile.profileSet] = profile.taskName
}

val compactPullMemoryResult = snapshotMemoryResultDirectory.map {
    it.file("pull/large-pull-25k-1k.properties")
}
tasks.register<Test>("oversqliteCompactPullMemoryJvm") {
    group = "verification"
    description = "Measures one large compact pull response in a fresh bounded JVM."
    maxHeapSize = "128m"
    timeout.set(Duration.ofHours(2))
    dependsOn(tasks.named("jvmTestClasses"))
    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()
    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.SnapshotMemoryBaselineTest.pullMemoryBaseline")
    }
    environment("OVERSQLITE_PULL_MEMORY_ROWS", "25000")
    environment("OVERSQLITE_PULL_MEMORY_ROW_BYTES", "1024")
    environment("OVERSQLITE_PULL_MEMORY_RUN_LABEL", "large-pull-25k-1k")
    environment("OVERSQLITE_PULL_MEMORY_RESULT_FILE", compactPullMemoryResult.get().asFile.absolutePath)
    environment("OVERSQLITE_MEMORY_SERVER_CLASSPATH", jvmTest.get().classpath.asPath)
    doFirst { compactPullMemoryResult.get().asFile.delete() }
    outputs.file(compactPullMemoryResult)
    testLogging.showStandardStreams = true
    outputs.upToDateWhen { false }
}

val verifySnapshotMemoryClosureResults = tasks.register<JavaExec>("verifyOversqliteSnapshotMemoryClosureResults") {
    group = "verification"
    description = "Verifies the mandatory schema-evolution closure snapshot-memory profile set and RSS ceiling."
    dependsOn(tasks.named("jvmTestClasses"))
    snapshotMemoryClosureProfiles.forEach { profile ->
        mustRunAfter(tasks.named(profile.taskName))
    }
    classpath = jvmTest.get().classpath
    mainClass.set("dev.goquick.sqlitenow.oversqlite.SnapshotMemoryResultVerifier")
    args(snapshotMemoryResultDirectory.get().dir("closure").asFile.absolutePath, "closure")
    outputs.upToDateWhen { false }
}

val verifySnapshotMemoryExtendedResults = tasks.register<JavaExec>("verifyOversqliteSnapshotMemoryResults") {
    group = "verification"
    description = "Verifies the optional extended one-million-row and repeatability/scaling snapshot-memory gates."
    dependsOn(tasks.named("jvmTestClasses"))
    snapshotMemoryExtendedProfiles.forEach { profile ->
        mustRunAfter(tasks.named(profile.taskName))
    }
    classpath = jvmTest.get().classpath
    mainClass.set("dev.goquick.sqlitenow.oversqlite.SnapshotMemoryResultVerifier")
    args(snapshotMemoryResultDirectory.get().dir("extended").asFile.absolutePath, "extended")
    outputs.upToDateWhen { false }
}

tasks.register("oversqliteSnapshotMemoryJvmClosure") {
    group = "verification"
    description = "Runs mandatory compact snapshot KMP JVM memory profiles; never run in GitHub Actions."
    dependsOn(tasks.named("oversqliteSnapshotMemorySamplerControl"))
    dependsOn(snapshotMemoryClosureProfiles.map { tasks.named(it.taskName) })
    dependsOn(verifySnapshotMemoryClosureResults)
}

tasks.named("oversqliteCompactPullMemoryJvm") {
    mustRunAfter(tasks.named("oversqliteSnapshotMemoryJvmClosure"))
}

tasks.register("oversqliteCompactMemoryJvm") {
    group = "verification"
    description = "Runs compact snapshot closure and large-pull JVM memory evidence."
    dependsOn(tasks.named("oversqliteSnapshotMemoryJvmClosure"))
    dependsOn(tasks.named("oversqliteCompactPullMemoryJvm"))
}

tasks.register("oversqliteSnapshotMemoryJvmExtended") {
    group = "verification"
    description = "Runs the optional extended KMP JVM repeatability and one-million-row benchmark."
    dependsOn(tasks.named("oversqliteSnapshotMemorySamplerControl"))
    dependsOn(snapshotMemoryExtendedProfiles.map { tasks.named(it.taskName) })
    dependsOn(verifySnapshotMemoryExtendedResults)
}

tasks.register("oversqliteSnapshotMemoryJvmLocalHeavy") {
    group = "verification"
    description = "Compatibility alias for the optional extended KMP JVM snapshot-memory benchmark."
    dependsOn(tasks.named("oversqliteSnapshotMemoryJvmExtended"))
}

val localOnlySnapshotMemoryTaskPaths =
    (snapshotMemoryProfiles.map { it.taskName } +
        listOf(
            "oversqliteSnapshotMemorySamplerControl",
            "verifyOversqliteSnapshotMemoryClosureResults",
            "verifyOversqliteSnapshotMemoryResults",
            "oversqliteSnapshotMemoryJvmClosure",
            "oversqliteSnapshotMemoryJvmExtended",
            "oversqliteSnapshotMemoryJvmLocalHeavy",
            "oversqliteCompactPullMemoryJvm",
            "oversqliteCompactMemoryJvm",
        ))
        .map { ":library-oversqlite:$it" }
        .toSet()

gradle.taskGraph.whenReady {
    if (System.getenv("GITHUB_ACTIONS") != "true") return@whenReady
    val requestedMemoryTask = allTasks.firstOrNull { it.path in localOnlySnapshotMemoryTaskPaths }
    if (requestedMemoryTask != null) {
        throw GradleException(
            "${requestedMemoryTask.path} is a local-only snapshot-memory task and must not run with GITHUB_ACTIONS=true",
        )
    }
}

tasks.register<Test>("oversqliteRealserverJvm") {
    group = "verification"
    description = "Runs the shared JVM realserver oversqlite suite."

    dependsOn(tasks.named("jvmTestClasses"))

    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()

    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerComprehensiveTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerBundleChangeWatchTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerRichSchemaTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.SettlementLiveTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.A1LiveSurvivorsTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerDependencyPlanningTest.publicSync*")
    }

    environment("OVERSQLITE_REALSERVER_TESTS", "true")
    val baseUrlOverride = System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
    if (!baseUrlOverride.isNullOrBlank()) {
        environment("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL", baseUrlOverride)
    }

    inputs.property("oversqliteRealServerSmokeBaseUrl", baseUrlOverride ?: "")
    testLogging.showStandardStreams = true
    outputs.upToDateWhen { false }
    shouldRunAfter(jvmTest)
}

tasks.register<Test>("jvmRealServerSharedConnectionStress") {
    group = "verification"
    description = "Runs the local-only shared realserver stress and snapshot profile tests on the JVM target."

    dependsOn(tasks.named("jvmTestClasses"))

    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()

    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerSharedConnectionStressTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerSnapshotProfileJvmTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.SettlementLiveTest.backlogBound")
    }

    environment("OVERSQLITE_REALSERVER_TESTS", "true")
    environment("OVERSQLITE_REALSERVER_HEAVY", "true")
    val baseUrlOverride = System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
    if (!baseUrlOverride.isNullOrBlank()) {
        environment("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL", baseUrlOverride)
    }

    inputs.property("oversqliteRealServerSmokeBaseUrl", baseUrlOverride ?: "")
    testLogging.showStandardStreams = true
    outputs.upToDateWhen { false }
    shouldRunAfter(jvmTest)
}

tasks.register("jvmRealServerSharedStress") {
    group = "verification"
    description = "Compatibility alias for jvmRealServerSharedConnectionStress."
    dependsOn(tasks.named("jvmRealServerSharedConnectionStress"))
}

mavenPublishing {
    publishToMavenCentral()

    if (!isPublishingToMavenLocal && hasSigningCredentials) {
        signAllPublications()
    }

    coordinates(group.toString(), "oversqlite", version.toString())

    pom {
        name = "SQLiteNow Oversqlite Multiplatform Library"
        description = "SQLiteNow oversqlite multiplatform runtime"
        inceptionYear = "2025"
        url = "https://github.com/mobiletoly/sqlitenow-kmp/"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "mobiletoly"
                name = "Toly Pochkin"
                url = "https://github.com/mobiletoly"
            }
        }
        scm {
            url = "https://github.com/mobiletoly/sqlitenow-kmp"
            connection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
            developerConnection = "scm:git:git://github.com/mobiletoly/sqlitenow-kmp.git"
        }
    }
}
