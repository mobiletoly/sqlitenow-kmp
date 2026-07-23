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
    id(libs.plugins.serialization.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    android {
        namespace = "dev.goquick.sqlitenow.core.test"
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
    }

    iosSimulatorArm64()
    macosArm64()
    linuxArm64()
    linuxX64()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.serialization.cbor)
            implementation(libs.kermit)

            implementation(project(":library-core"))
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

        val webTest by getting {
            kotlin.srcDir(rootProject.file("library-core/src/legacySqlJsWebTest/kotlin"))
            dependencies {
                implementation(npm("@sqlite.org/sqlite-wasm", "3.53.0-build1"))
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
        }

        getByName("androidDeviceTest").dependencies {
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.rules)
            implementation(libs.androidx.junit)
            implementation(libs.kotlinx.coroutines.test)
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

listOf("Js", "WasmJs").forEach { targetSuffix ->
    val coreMainCompile =
        coreProject.tasks.named<BaseKotlinCompile>("compileKotlin$targetSuffix")

    tasks.named<BaseKotlinCompile>("compileTestKotlin$targetSuffix") {
        dependsOn(coreMainCompile)
        friendPaths.from(coreMainCompile.flatMap { it.destinationDirectory })
    }
}

sqliteNow {
    databases {
        create("LibraryTestDatabase") {
            packageName = "dev.goquick.sqlitenow.core.test.db"
            debug = true
        }
        create("MigrationFixtureDatabase") {
            packageName = "dev.goquick.sqlitenow.core.test.migration.db"
            debug = true
        }
        create("Phase6WorkerValueDatabase") {
            packageName = "dev.goquick.sqlitenow.core.test.phase6.db"
            debug = true
        }
    }
}
