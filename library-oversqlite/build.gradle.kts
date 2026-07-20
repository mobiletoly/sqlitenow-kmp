@file:OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)

import org.gradle.api.file.DuplicatesStrategy
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

        withHostTestBuilder {
            sourceSetTreeName = "test"
        }

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

        jvmTest.dependencies {
            implementation("io.ktor:ktor-client-cio:${libs.versions.ktor.get()}")
        }

        androidMain.dependencies {
            implementation(libs.sqlite.bundled)
            implementation(libs.ktor.client.okhttp)
        }

        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.rules)
            implementation(libs.androidx.junit)
            implementation(libs.kotlinx.coroutines.test)
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

        val webMain by getting {
            dependencies {
                implementation(npm("sql.js", "1.13.0"))
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

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

val libraryCoreProject = project(":library-core")
val librarySqlJsResource = libraryCoreProject.layout.buildDirectory.file("processedResources/wasmJs/main/sqlitenow-sqljs.js")
val librarySqlWasmBinary = libraryCoreProject.layout.buildDirectory.file("processedResources/wasmJs/main/sql-wasm.wasm")
val libraryIndexedDbResource =
    libraryCoreProject.layout.buildDirectory.file("processedResources/wasmJs/main/sqlitenow-indexeddb.js")

tasks.named<ProcessResources>("wasmJsProcessResources") {
    dependsOn(libraryCoreProject.tasks.named("wasmJsProcessResources"))
    from(librarySqlJsResource)
    from(librarySqlWasmBinary)
    from(libraryIndexedDbResource)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val jvmTest by tasks.existing(Test::class)

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
)

tasks.register<Test>("oversqliteSnapshotMemorySamplerControl") {
    group = "verification"
    description = "Runs the local-only five-second snapshot sampler control in a fresh JVM."
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

val snapshotMemoryProfiles = listOf(
    SnapshotMemoryProfile("oversqliteSnapshotMemory10k256", 10_000, 256, "representative-10k-256"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory10k1k", 10_000, 1024, "representative-10k-1k"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Warmup", 100_000, 256, "warmup-100k-256"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run1", 100_000, 256, "measured-100k-256-1"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run2", 100_000, 256, "measured-100k-256-2"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k256Run3", 100_000, 256, "measured-100k-256-3"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory100k1k", 100_000, 1024, "measured-100k-1k"),
    SnapshotMemoryProfile("oversqliteSnapshotMemory1m256", 1_000_000, 256, "measured-1m-256"),
)
val snapshotMemoryResultDirectory = layout.buildDirectory.dir("snapshot-memory-results")

var previousSnapshotMemoryTask: String? = null
snapshotMemoryProfiles.forEach { profile ->
    val predecessor = previousSnapshotMemoryTask
    val resultFile = snapshotMemoryResultDirectory.map { it.file("${profile.runLabel}.properties") }
    tasks.register<Test>(profile.taskName) {
        group = "verification"
        description = "Runs the local-heavy ${profile.runLabel} bounded snapshot profile in a fresh JVM."
        maxHeapSize = "128m"
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
    previousSnapshotMemoryTask = profile.taskName
}

val verifySnapshotMemoryResults = tasks.register<JavaExec>("verifyOversqliteSnapshotMemoryResults") {
    group = "verification"
    description = "Verifies completeness, the one-million-row ceiling, and snapshot-memory scaling gates."
    dependsOn(tasks.named("jvmTestClasses"))
    snapshotMemoryProfiles.forEach { profile ->
        mustRunAfter(tasks.named(profile.taskName))
    }
    classpath = jvmTest.get().classpath
    mainClass.set("dev.goquick.sqlitenow.oversqlite.SnapshotMemoryResultVerifier")
    args(snapshotMemoryResultDirectory.get().asFile.absolutePath)
    outputs.upToDateWhen { false }
}

tasks.register("oversqliteSnapshotMemoryJvmLocalHeavy") {
    group = "verification"
    description = "Runs all required local-heavy Phase 3 KMP JVM memory profiles; never run in GitHub Actions."
    dependsOn(snapshotMemoryProfiles.map { tasks.named(it.taskName) })
    dependsOn(verifySnapshotMemoryResults)
}

val localOnlySnapshotMemoryTaskPaths =
    (snapshotMemoryProfiles.map { it.taskName } +
        listOf(
            "oversqliteSnapshotMemorySamplerControl",
            "verifyOversqliteSnapshotMemoryResults",
            "oversqliteSnapshotMemoryJvmLocalHeavy",
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
    description = "Runs the local-only shared realserver stress and Phase 5 profile tests on the JVM target."

    dependsOn(tasks.named("jvmTestClasses"))

    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()

    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerSharedConnectionStressTest")
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerSnapshotProfileJvmTest")
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
