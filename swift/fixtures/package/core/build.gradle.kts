import org.gradle.api.tasks.Exec
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    id(libs.plugins.kotlinMultiplatform.get().pluginId)
    id("dev.goquick.sqlitenow")
}

kotlin {
    jvmToolchain(17)
    applyDefaultHierarchyTemplate()

    compilerOptions {
        languageVersion.set(KotlinVersion.KOTLIN_2_4)
        freeCompilerArgs.add("-Xexpect-actual-classes")
    }

    jvm()
    macosArm64()
    iosArm64()
    iosSimulatorArm64()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":library-core"))
        }
    }
}

val fixtureSwiftPackageName = "CoreFixtureDatabaseSQLiteNow"
val fixtureSwiftTargetName = "CoreFixtureDatabaseSQLiteNow"
val fixtureRuntimeModuleName = "SQLiteNowCoreRuntime"
val swiftPackageOutputDir = layout.projectDirectory.dir("../build/swift-package/$fixtureSwiftPackageName")
val swiftGeneratedSourceDir = layout.buildDirectory.dir("generated/sqlitenow/swift-product/$fixtureSwiftTargetName")
val swiftTestSourceDir = layout.projectDirectory.dir("Tests")
val runtimeXcframeworkDir =
    project(":swift:runtime:core").layout.buildDirectory.dir("runtime/$fixtureRuntimeModuleName.xcframework")

sqliteNow {
    databases {
        create("CoreFixtureDatabase") {
            packageName = "dev.goquick.sqlitenow.swiftfixtures.core.db"
            debug = false
            swiftPackage {
                packageName.set(fixtureSwiftPackageName)
                swiftTargetName.set(fixtureSwiftTargetName)
                outputDirectory.set(swiftPackageOutputDir)
                generatedSourceDirectory.set(swiftGeneratedSourceDir)
                runtimeXcframework.set(runtimeXcframeworkDir)
                runtimeTaskPath.set(":swift:runtime:core:packageDebugRuntimeXcframework")
                runtimeModuleName.set(fixtureRuntimeModuleName)
            }
        }
    }
}

fun findExecutableOnPath(name: String): File? =
    System.getenv("PATH")
        .orEmpty()
        .split(File.pathSeparator)
        .asSequence()
        .filter { it.isNotBlank() }
        .map { File(it, name) }
        .firstOrNull { it.isFile && it.canExecute() }

tasks.register<Exec>("swiftTest") {
    group = "verification"
    description = "Runs SwiftPM tests for the generated core fixture package."

    dependsOn(
        "packageDebugSwiftPackage",
        "validateDebugSwiftPackageManifest",
        "checkDebugSwiftPackageLeaks",
    )

    inputs.file(layout.projectDirectory.file("Package.swift"))
    inputs.dir(swiftPackageOutputDir)
    inputs.dir(swiftTestSourceDir)
    inputs.dir(runtimeXcframeworkDir)
    outputs.upToDateWhen { false }

    workingDir = projectDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args("test")
}
