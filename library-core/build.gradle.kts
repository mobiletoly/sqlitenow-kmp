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
        namespace = "dev.goquick.sqlitenow.core"
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
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
        }

        val nativeMain by getting {
            dependencies {
                implementation(libs.sqlite.bundled)
            }
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.bundled)
        }

        androidMain.dependencies {
            implementation(libs.sqlite.bundled)
        }

        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.rules)
            implementation(libs.androidx.junit)
            implementation(libs.kotlinx.coroutines.test)
        }

        val webMain by getting {
            dependencies {
                implementation(npm("sql.js", "1.13.0"))
            }
        }

        jsMain.dependencies {
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

mavenPublishing {
    publishToMavenCentral()

    if (!isPublishingToMavenLocal && hasSigningCredentials) {
        signAllPublications()
    }

    coordinates(group.toString(), "core", version.toString())

    pom {
        name = "SQLiteNow Core Multiplatform Library"
        description = "SQLiteNow core multiplatform runtime"
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
