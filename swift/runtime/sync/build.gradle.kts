import org.jetbrains.kotlin.gradle.dsl.KotlinVersion
import org.jetbrains.kotlin.gradle.tasks.KotlinNativeCompile

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
}

// This opt-in builds an isolated XCTest framework. Its Swift probe is never part
// of the reusable or release runtime artifacts built without the property.
val a3SwiftTestBridge = providers.gradleProperty("a3SwiftTestBridge").orNull == "true"
if (a3SwiftTestBridge) {
    layout.buildDirectory.set(layout.projectDirectory.dir("build/a3-test"))
    tasks.matching { it.name.startsWith("linkReleaseFramework") || it.name.startsWith("packageReleaseRuntime") }
        .configureEach {
            doFirst { error("A3 Swift test bridge may only be linked into the isolated debug test framework.") }
        }
    val oversqliteProject = project(":library-oversqlite")
    tasks.withType<KotlinNativeCompile>().configureEach {
        val targetSuffix = name.removePrefix("compileKotlin")
        if (targetSuffix in setOf("IosArm64", "IosSimulatorArm64", "MacosArm64")) {
            val mainKlib = oversqliteProject.layout.buildDirectory.dir(
                "classes/kotlin/${targetSuffix.replaceFirstChar(Char::lowercase)}/main/klib/library-oversqlite"
            )
            dependsOn(oversqliteProject.tasks.named("compileKotlin$targetSuffix"))
            compilerOptions.freeCompilerArgs.add(mainKlib.map { "-friend-modules=${it.asFile.absolutePath}" })
        }
    }
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }

    listOf(
        macosArm64(),
        iosArm64(),
        iosSimulatorArm64(),
    ).forEach { target ->
        target.binaries.framework {
            baseName = "SQLiteNowSyncRuntime"
            isStatic = false
            export(project(":swift:runtime:core"))
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":library-oversqlite"))
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.auth)
            implementation(libs.ktor.client.content.negotiation)
            implementation(libs.ktor.serialization.kotlinx.json)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }

        val appleMain by getting {
            kotlin.srcDir(if (a3SwiftTestBridge) "src/a3TestBridge/kotlin" else "src/a3Noop/kotlin")
            dependencies {
                api(project(":swift:runtime:core"))
                implementation(libs.ktor.client.darwin)
            }
        }
    }
}

extra["swiftRuntimeMode"] = "sync"
extra["swiftRuntimeFrameworkModuleName"] = "SQLiteNowSyncRuntime"
apply(from = rootProject.file("gradle/swift-runtime-artifact-packaging.gradle.kts"))
