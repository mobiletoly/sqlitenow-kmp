import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.Copy
import org.gradle.language.jvm.tasks.ProcessResources
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.BaseKotlinCompile

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id(libs.plugins.androidKotlinMultiplatformLibrary.get().pluginId)
    id(libs.plugins.jetbrainsCompose.get().pluginId)
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
    alias(libs.plugins.composeCompiler)
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    android {
        namespace = "dev.goquick.sqlitenow.oversqlite"
        compileSdk = libs.versions.android.compileSdk.get().toInt()
        minSdk = libs.versions.android.minSdk.get().toInt()

        androidResources {
            enable = true
        }

        withDeviceTestBuilder {
            sourceSetTreeName = "test"
        }

        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }

    jvm()

    js {
        nodejs()
        browser()
        binaries.library()
    }

    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
        browser()
        binaries.library()
        binaries.executable()
    }

    iosArm64()
    iosSimulatorArm64()
    macosArm64()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
    }
    sourceSets {
        commonMain.dependencies {
            implementation(libs.jetbrains.compose.runtime)
            implementation(libs.jetbrains.compose.foundation)
            implementation(libs.jetbrains.compose.ui)
            implementation(libs.jetbrains.compose.material)
            implementation(libs.jetbrains.compose.components.resources)
            implementation(libs.jetbrains.compose.ui.toolingPreview)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.cbor)
            implementation(libs.kermit)
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.content.negotiation)
            implementation(libs.ktor.client.auth)
            implementation(libs.ktor.serialization.kotlinx.json)

            implementation(project(":library-core"))
            implementation(project(":library-oversqlite"))
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotlinx.coroutines.test)
            implementation("io.ktor:ktor-client-mock:${libs.versions.ktor.get()}")
        }

        val webTest by getting {
            kotlin.srcDir(rootProject.file("library-core/src/legacySqlJsWebTest/kotlin"))
            dependencies {
                implementation(npm("sql.js", "1.13.0"))
            }
        }

        val jsTest by getting {
            kotlin.srcDir(rootProject.file("library-core/src/legacySqlJsJsTest/kotlin"))
        }

        val wasmJsTest by getting {
            kotlin.srcDir(rootProject.file("library-core/src/legacySqlJsWasmJsTest/kotlin"))
            resources.srcDir(rootProject.file("library-core/src/legacySqlJsWasmJsTest/resources"))
        }

        val nativeMain by getting {
            dependencies {
                implementation(libs.sqlite.bundled)
            }
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.bundled)
            implementation("io.ktor:ktor-client-cio:${libs.versions.ktor.get()}")
        }

        androidMain.dependencies {
            implementation(libs.androidx.activityCompose)
            implementation(libs.compose.ui.tooling)
            implementation(libs.compose.ui.toolingPreview)
            implementation(libs.sqlite.bundled)
            implementation(libs.ktor.client.okhttp)
        }

        iosMain.dependencies {
            implementation(libs.ktor.client.darwin)
        }

        macosMain.dependencies {
            implementation(libs.ktor.client.darwin)
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
        }

        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.rules)
            implementation(libs.androidx.junit)
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.okhttp.mockwebserver3)
        }
    }
}

val legacySqlJsFixtureModuleDir = rootProject.layout.buildDirectory.dir("js/node_modules/sql.js/dist")
val legacySqlJsFixtureWasmDir = layout.buildDirectory.dir("generated/test-fixtures/sqljs/wasm")

val copyLegacySqlJsFixtureWasm by tasks.registering(Copy::class) {
    dependsOn(rootProject.tasks.named("kotlinNpmInstall"))
    from(legacySqlJsFixtureModuleDir.map { it.file("sql-wasm.wasm") })
    into(legacySqlJsFixtureWasmDir)
}

tasks.named<ProcessResources>("wasmJsTestProcessResources") {
    dependsOn(copyLegacySqlJsFixtureWasm)
    from(legacySqlJsFixtureWasmDir)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

val coreProject = project(":library-core")
val oversqliteProject = project(":library-oversqlite")

listOf("Js", "WasmJs").forEach { targetSuffix ->
    val coreMainCompile =
        coreProject.tasks.named<BaseKotlinCompile>("compileKotlin$targetSuffix")
    val oversqliteMainCompile =
        oversqliteProject.tasks.named<BaseKotlinCompile>("compileKotlin$targetSuffix")

    tasks.named<BaseKotlinCompile>("compileTestKotlin$targetSuffix") {
        dependsOn(coreMainCompile, oversqliteMainCompile)
        friendPaths.from(
            coreMainCompile.flatMap { it.destinationDirectory },
            oversqliteMainCompile.flatMap { it.destinationDirectory },
        )
    }
}

sqliteNow {
    databases {
        create("RealServerGeneratedDatabase") {
            packageName = "dev.goquick.sqlitenow.oversqlite.platform.generated"
            oversqlite = true
        }
    }
}
