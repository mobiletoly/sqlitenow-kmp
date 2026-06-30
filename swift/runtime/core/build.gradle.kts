import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
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
            baseName = "SQLiteNowCoreRuntime"
            isStatic = false
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":library-core"))
        }
    }
}

extra["swiftRuntimeMode"] = "core"
extra["swiftRuntimeFrameworkModuleName"] = "SQLiteNowCoreRuntime"
apply(from = rootProject.file("gradle/swift-runtime-artifact-packaging.gradle.kts"))
