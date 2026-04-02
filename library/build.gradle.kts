@file:OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)

import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.Copy
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

    androidLibrary {
        namespace = group.toString()
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
        languageVersion.set(KotlinVersion.KOTLIN_2_3)
        freeCompilerArgs.addAll("-Xexpect-actual-classes")
    }

    js(IR) {
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

    iosX64()
    iosArm64()
    iosSimulatorArm64()
    macosArm64()
    macosX64()
    linuxArm64()
    linuxX64()

    sourceSets {
        commonMain.dependencies {
            api(libs.kotlinx.coroutines.core)
            api(libs.kotlinx.datetime)
            api(libs.kotlinx.serialization.json)
            api(libs.kermit)
            api(libs.ktor.client.core)
            api(libs.ktor.client.content.negotiation)
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

val sqlJsModuleDir = rootProject.layout.buildDirectory.dir("js/node_modules/sql.js/dist")
val wasmSqlJsResourceDir = layout.buildDirectory.dir("generated/sqljs/wasm")

val copySqlJsWasmForWasm by tasks.registering(Copy::class) {
    dependsOn(rootProject.tasks.named("kotlinNpmInstall"))
    from(sqlJsModuleDir.map { it.file("sql-wasm.wasm") })
    into(wasmSqlJsResourceDir)
}

tasks.named<ProcessResources>("wasmJsProcessResources") {
    dependsOn(copySqlJsWasmForWasm)
    from(wasmSqlJsResourceDir)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

val jvmTest by tasks.existing(Test::class)

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
    }

    outputs.upToDateWhen { false }
    shouldRunAfter(jvmTest)
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
    }

    environment("OVERSQLITE_REALSERVER_TESTS", "true")
    val baseUrlOverride = System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
    if (!baseUrlOverride.isNullOrBlank()) {
        environment("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL", baseUrlOverride)
    }

    inputs.property("oversqliteRealServerSmokeBaseUrl", baseUrlOverride ?: "")
    outputs.upToDateWhen { false }
    shouldRunAfter(jvmTest)
}

tasks.register<Test>("jvmRealServerSharedConnectionStress") {
    group = "verification"
    description = "Runs the local-only shared realserver stress test on the JVM target."

    dependsOn(tasks.named("jvmTestClasses"))

    testClassesDirs = jvmTest.get().testClassesDirs
    classpath = jvmTest.get().classpath
    useJUnit()

    filter {
        includeTestsMatching("dev.goquick.sqlitenow.oversqlite.RealServerSharedConnectionStressTest")
    }

    environment("OVERSQLITE_REALSERVER_TESTS", "true")
    environment("OVERSQLITE_REALSERVER_HEAVY", "true")
    val baseUrlOverride = System.getenv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
    if (!baseUrlOverride.isNullOrBlank()) {
        environment("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL", baseUrlOverride)
    }

    inputs.property("oversqliteRealServerSmokeBaseUrl", baseUrlOverride ?: "")
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

    coordinates(group.toString(), "core", version.toString())

    pom {
        name = "SQLiteNow Multiplatform Library"
        description = "SQLiteNow Multiplatform Library"
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
