import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsEnvSpec
import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsRootPlugin
import groovy.json.JsonSlurper
import java.io.File
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

apply(from = "gradle/swiftpm-plugin-release.gradle.kts")

val swiftPackageDefaultAppleTargets = listOf("macosArm64", "iosArm64", "iosSimulatorArm64")
val swiftPackageDefaultMinimumIos = "15"
val swiftPackageDefaultMinimumMacos = "14"

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

fun findExecutableOnPath(name: String): File? =
    System.getenv("PATH")
        .orEmpty()
        .split(File.pathSeparator)
        .asSequence()
        .filter { it.isNotBlank() }
        .map { File(it, name) }
        .firstOrNull { it.isFile && it.canExecute() }

fun manifestString(manifest: Map<*, *>, key: String): String =
    manifest[key] as? String ?: error("Expected manifest $key to be a string.")

fun manifestStringList(manifest: Map<*, *>, key: String): List<String> =
    (manifest[key] as? List<*>)?.map {
        it as? String ?: error("Expected manifest $key to contain only strings.")
    } ?: error("Expected manifest $key to be a string array.")

fun requireManifestString(manifest: Map<*, *>, key: String, expected: String) {
    val actual = manifestString(manifest, key)
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

fun requireManifestStringList(manifest: Map<*, *>, key: String, expected: List<String>) {
    val actual = manifestStringList(manifest, key)
    require(actual == expected) {
        "Expected manifest $key=$expected, got $actual."
    }
}

fun registerSwiftSampleManifestValidationTask(
    name: String,
    generateTaskName: String,
    generatedPackageDir: org.gradle.api.file.Directory,
    databaseName: String,
    swiftPackageName: String,
    runtimeMode: String,
    runtimeModuleName: String,
    expectedGeneratedSwiftFiles: List<String>,
    expectedSyncTables: List<Pair<String, String>> = emptyList(),
) {
    tasks.register(name) {
        group = "verification"
        description = "Validates $databaseName generated Swift package metadata."
        dependsOn(generateTaskName)

        val manifestFile = generatedPackageDir.file(".sqlitenow/package-manifest.json")
        inputs.file(manifestFile)
        inputs.file(generatedPackageDir.file("Package.swift"))
        inputs.dir(generatedPackageDir.dir("Sources"))

        doLast {
            @Suppress("UNCHECKED_CAST")
            val manifest = JsonSlurper().parse(manifestFile.asFile) as Map<*, *>
            val manifestVersion = (manifest["manifestVersion"] as? Number)?.toInt()
            require(manifestVersion == 3) {
                "Expected manifest manifestVersion=3, got $manifestVersion."
            }

            val sqliteNowVersion = providers.gradleProperty("sqlitenow.version").get()
            requireManifestString(manifest, "sqliteNowVersion", sqliteNowVersion)
            requireManifestString(manifest, "generatorVersion", sqliteNowVersion)
            requireManifestString(manifest, "databaseName", databaseName)
            requireManifestString(manifest, "packageName", swiftPackageName)
            requireManifestString(manifest, "swiftTargetName", swiftPackageName)
            requireManifestString(manifest, "runtimeMode", runtimeMode)
            requireManifestStringList(manifest, "runtimeBinaryTargets", listOf(runtimeModuleName))
            requireManifestString(manifest, "runtimeArtifactKind", "localXcframework")
            requireManifestStringList(manifest, "runtimeArtifactPaths", listOf("Binaries/$runtimeModuleName.xcframework"))
            requireManifestStringList(manifest, "requestedAppleTargets", swiftPackageDefaultAppleTargets)
            requireManifestString(manifest, "frameworkMode", "dynamic")
            requireManifestString(manifest, "generatedBy", "swift package plugin --allow-writing-to-package-directory sqlitenow-generate")

            val minimumPlatforms = manifest["minimumPlatforms"] as? Map<*, *>
                ?: error("Expected manifest minimumPlatforms object.")
            require(minimumPlatforms["iOS"] == swiftPackageDefaultMinimumIos) {
                "Expected manifest minimumPlatforms.iOS=$swiftPackageDefaultMinimumIos, got ${minimumPlatforms["iOS"]}."
            }
            require(minimumPlatforms["macOS"] == swiftPackageDefaultMinimumMacos) {
                "Expected manifest minimumPlatforms.macOS=$swiftPackageDefaultMinimumMacos, got ${minimumPlatforms["macOS"]}."
            }

            val generatedSwiftFiles = manifestStringList(manifest, "generatedSwiftFiles")
            expectedGeneratedSwiftFiles.forEach { expected ->
                require(expected in generatedSwiftFiles) {
                    "Expected generatedSwiftFiles to include $expected, got $generatedSwiftFiles."
                }
            }

            if (runtimeMode == "sync") {
                val actualSyncTables = (manifest["syncTables"] as? List<*>)
                    ?.map { table ->
                        val tableMap = table as? Map<*, *> ?: error("Expected sync table object, got $table.")
                        (tableMap["tableName"] as? String ?: error("Expected sync tableName.")) to
                            (tableMap["syncKeyColumnName"] as? String ?: error("Expected syncKeyColumnName."))
                    }
                    ?: error("Expected manifest syncTables array.")
                require(actualSyncTables == expectedSyncTables) {
                    "Expected manifest syncTables=$expectedSyncTables, got $actualSyncTables."
                }
            }
        }
    }
}

fun registerSwiftRuntimeArtifactManifestValidationTask(
    name: String,
    packageTaskPath: String,
    manifestDirectoryPath: String,
    runtimeMode: String,
    runtimeModuleName: String,
) {
    tasks.register(name) {
        group = "verification"
        description = "Validates $runtimeModuleName runtime artifact metadata defaults."
        dependsOn(packageTaskPath)

        val manifestFile = providers.gradleProperty("sqlitenow.version").map { version ->
            file("$manifestDirectoryPath/$runtimeModuleName-$version.artifact-manifest.json")
        }
        inputs.file(manifestFile)

        doLast {
            @Suppress("UNCHECKED_CAST")
            val manifest = JsonSlurper().parse(manifestFile.get()) as Map<*, *>
            val manifestVersion = (manifest["manifestVersion"] as? Number)?.toInt()
            require(manifestVersion == 1) {
                "Expected manifest manifestVersion=1, got $manifestVersion."
            }

            val sqliteNowVersion = providers.gradleProperty("sqlitenow.version").get()
            requireManifestString(manifest, "sqliteNowVersion", sqliteNowVersion)
            requireManifestString(manifest, "runtimeMode", runtimeMode)
            requireManifestString(manifest, "runtimeModuleName", runtimeModuleName)
            requireManifestString(manifest, "frameworkMode", "dynamic")
            requireManifestStringList(manifest, "requestedAppleTargets", swiftPackageDefaultAppleTargets)

            val minimumPlatforms = manifest["minimumPlatforms"] as? Map<*, *>
                ?: error("Expected manifest minimumPlatforms object.")
            require(minimumPlatforms["iOS"] == swiftPackageDefaultMinimumIos) {
                "Expected manifest minimumPlatforms.iOS=$swiftPackageDefaultMinimumIos, got ${minimumPlatforms["iOS"]}."
            }
            require(minimumPlatforms["macOS"] == swiftPackageDefaultMinimumMacos) {
                "Expected manifest minimumPlatforms.macOS=$swiftPackageDefaultMinimumMacos, got ${minimumPlatforms["macOS"]}."
            }
        }
    }
}

fun registerSwiftSampleLeakCheckTask(
    name: String,
    generateTaskName: String,
    generatedPackageDir: org.gradle.api.file.Directory,
    forbiddenTokenPatterns: List<String>,
) {
    tasks.register(name) {
        group = "verification"
        description = "Checks generated Swift package source for forbidden implementation leak tokens."
        dependsOn(generateTaskName)

        val sourceDir = generatedPackageDir.dir("Sources")
        inputs.dir(sourceDir)

        doLast {
            val forbiddenRegexes = forbiddenTokenPatterns.map { it.toRegex() }
            val leaks = sourceDir.asFile
                .walkTopDown()
                .filter { it.isFile && it.extension == "swift" }
                .flatMap { file ->
                    val text = file.readText()
                    forbiddenRegexes
                        .filter { regex -> regex.containsMatchIn(text) }
                        .map { regex -> "${file.relativeTo(sourceDir.asFile).invariantSeparatorsPath}: ${regex.pattern}" }
                }
                .toList()
            require(leaks.isEmpty()) {
                "Generated Swift package leaked forbidden support tokens:\n${leaks.joinToString("\n")}"
            }
        }
    }
}

val swiftOnlyCoreFixtureDir = layout.projectDirectory.dir("swift/fixtures/swiftpm/swift-only-core")
val swiftOnlyCoreGeneratedPackageDir = swiftOnlyCoreFixtureDir.dir("SQLiteNowGenerated/SwiftOnlyCoreDatabaseSQLiteNow")
val swiftOnlyCoreConsumerDir = swiftOnlyCoreFixtureDir.dir("Consumer")
val swiftOnlyCoreXcodeProject = swiftOnlyCoreFixtureDir.dir("XcodeConsumer/SwiftOnlyCoreXcodeApp.xcodeproj")
val swiftOnlyCoreXcodeDerivedDataDir = swiftOnlyCoreFixtureDir.dir(".build/xcode-derived-data")
val swiftOnlyCoreConsumerScratchDir = layout.buildDirectory.dir("swiftpm/swift-only-core-consumer")
val swiftOnlySyncFixtureDir = layout.projectDirectory.dir("swift/fixtures/swiftpm/swift-only-sync")
val swiftOnlySyncGeneratedPackageDir = swiftOnlySyncFixtureDir.dir("SQLiteNowGenerated/SwiftOnlySyncDatabaseSQLiteNow")
val swiftOnlySyncConsumerDir = swiftOnlySyncFixtureDir.dir("Consumer")
val swiftOnlySyncXcodeProject = swiftOnlySyncFixtureDir.dir("XcodeConsumer/SwiftOnlySyncXcodeApp.xcodeproj")
val swiftOnlySyncXcodeDerivedDataDir = swiftOnlySyncFixtureDir.dir(".build/xcode-derived-data")
val swiftOnlySyncConsumerScratchDir = layout.buildDirectory.dir("swiftpm/swift-only-sync-consumer")
val sampleSwiftDir = layout.projectDirectory.dir("swift/samples/core")
val sampleSwiftGeneratedPackageDir = sampleSwiftDir.dir("SQLiteNowGenerated/NowSampleDatabaseSQLiteNow")
val sampleSwiftXcodeProject = sampleSwiftDir.dir("iosApp/iosApp.xcodeproj")
val sampleSwiftXcodeDerivedDataDir = sampleSwiftDir.dir(".build/xcode-derived-data")
val sampleSyncSwiftDir = layout.projectDirectory.dir("swift/samples/sync")
val sampleSyncSwiftGeneratedPackageDir = sampleSyncSwiftDir.dir("SQLiteNowGenerated/NowSampleSyncDatabaseSQLiteNow")
val sampleSyncSwiftXcodeProject = sampleSyncSwiftDir.dir("iosApp/iosApp.xcodeproj")
val sampleSyncSwiftXcodeDerivedDataDir = sampleSyncSwiftDir.dir(".build/xcode-derived-data")
val swiftSampleForbiddenTokenPatterns = listOf(
    "Kotlin",
    "Ktor",
    "StateFlow",
    "Flow<",
    "Result<",
    "Throwable",
    "KotlinByteArray",
    "Coroutine",
    "\\bKt\\b",
)

registerSwiftRuntimeArtifactManifestValidationTask(
    name = "swiftCoreRuntimeDebugArtifactManifestDefaults",
    packageTaskPath = ":swift:runtime:core:packageDebugRuntimeArtifact",
    manifestDirectoryPath = "swift/runtime/core/build/runtime-artifacts",
    runtimeMode = "core",
    runtimeModuleName = "SQLiteNowCoreRuntime",
)

registerSwiftRuntimeArtifactManifestValidationTask(
    name = "swiftSyncRuntimeDebugArtifactManifestDefaults",
    packageTaskPath = ":swift:runtime:sync:packageDebugRuntimeArtifact",
    manifestDirectoryPath = "swift/runtime/sync/build/runtime-artifacts",
    runtimeMode = "sync",
    runtimeModuleName = "SQLiteNowSyncRuntime",
)

tasks.register("swiftRuntimeDebugArtifactManifestDefaults") {
    group = "verification"
    description = "Validates debug Swift runtime artifact metadata defaults."
    dependsOn(
        "swiftCoreRuntimeDebugArtifactManifestDefaults",
        "swiftSyncRuntimeDebugArtifactManifestDefaults",
    )
}

registerSwiftRuntimeArtifactManifestValidationTask(
    name = "swiftCoreRuntimeReleaseArtifactManifestDefaults",
    packageTaskPath = ":swift:runtime:core:packageReleaseRuntimeArtifact",
    manifestDirectoryPath = "swift/runtime/core/build/runtime-release-artifacts",
    runtimeMode = "core",
    runtimeModuleName = "SQLiteNowCoreRuntime",
)

registerSwiftRuntimeArtifactManifestValidationTask(
    name = "swiftSyncRuntimeReleaseArtifactManifestDefaults",
    packageTaskPath = ":swift:runtime:sync:packageReleaseRuntimeArtifact",
    manifestDirectoryPath = "swift/runtime/sync/build/runtime-release-artifacts",
    runtimeMode = "sync",
    runtimeModuleName = "SQLiteNowSyncRuntime",
)

tasks.register("swiftRuntimeReleaseArtifactManifestDefaults") {
    group = "verification"
    description = "Builds and validates release Swift runtime artifact metadata defaults."
    dependsOn(
        "swiftCoreRuntimeReleaseArtifactManifestDefaults",
        "swiftSyncRuntimeReleaseArtifactManifestDefaults",
    )
}

tasks.register("oversqliteComprehensive") {
    group = "verification"
    description = "Runs the host-side oversqlite comprehensive suite."
    dependsOn(":library-oversqlite:oversqliteComprehensiveJvm")
}

tasks.register<Exec>("swiftXcodeOnlyCoreGenerate") {
    group = "verification"
    description = "Runs the Gradle-free SwiftPM generator for the core Swift-only fixture."

    dependsOn(
        gradle.includedBuild("sqlitenow-compiler").task(":syncSwiftPmPluginCompilerJar"),
        "swiftCoreRuntimeDebugArtifactManifestDefaults",
    )

    inputs.file(swiftOnlyCoreFixtureDir.file("Package.swift"))
    inputs.file(swiftOnlyCoreFixtureDir.file("SQLiteNow.json"))
    inputs.dir(swiftOnlyCoreFixtureDir.dir("SQLiteNow"))
    outputs.dir(swiftOnlyCoreGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "package",
        "--package-path",
        swiftOnlyCoreFixtureDir.asFile.absolutePath,
        "plugin",
        "--allow-writing-to-package-directory",
        "sqlitenow-generate",
    )
    doFirst {
        environment.remove("SQLITENOW_COMPILER_JAR")
    }
}

tasks.register<Exec>("swiftXcodeOnlyCoreBuildGeneratedPackage") {
    group = "verification"
    description = "Builds the Gradle-free generated core Swift package with SwiftPM."

    dependsOn("swiftXcodeOnlyCoreGenerate")

    inputs.file(swiftOnlyCoreGeneratedPackageDir.file("Package.swift"))
    inputs.dir(swiftOnlyCoreGeneratedPackageDir.dir("Sources"))
    inputs.dir(swiftOnlyCoreGeneratedPackageDir.dir("Binaries"))
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "build",
        "--package-path",
        swiftOnlyCoreGeneratedPackageDir.asFile.absolutePath,
    )
}

tasks.register<Exec>("swiftXcodeOnlyCoreSwiftTest") {
    group = "verification"
    description = "Runs SwiftPM tests for the Gradle-free core Swift consumer fixture."

    dependsOn("swiftXcodeOnlyCoreGenerate")

    inputs.file(swiftOnlyCoreConsumerDir.file("Package.swift"))
    inputs.dir(swiftOnlyCoreConsumerDir.dir("Tests"))
    inputs.dir(swiftOnlyCoreGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "test",
        "--package-path",
        swiftOnlyCoreConsumerDir.asFile.absolutePath,
        "--scratch-path",
        swiftOnlyCoreConsumerScratchDir.get().asFile.absolutePath,
    )
    doFirst {
        delete(swiftOnlyCoreConsumerScratchDir.get().asFile)
    }
}

tasks.register<Exec>("swiftXcodeOnlyCoreXcodeBuild") {
    group = "verification"
    description = "Builds the Swift-only core Xcode consumer against the generated local package."

    dependsOn("swiftXcodeOnlyCoreGenerate")

    inputs.dir(swiftOnlyCoreXcodeProject)
    inputs.dir(swiftOnlyCoreFixtureDir.dir("XcodeConsumer/SwiftOnlyCoreXcodeApp"))
    inputs.dir(swiftOnlyCoreGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("xcodebuild")?.absolutePath ?: "xcodebuild"
    args(
        "-project",
        swiftOnlyCoreXcodeProject.asFile.absolutePath,
        "-scheme",
        "SwiftOnlyCoreXcodeApp",
        "-destination",
        "generic/platform=iOS Simulator",
        "-derivedDataPath",
        swiftOnlyCoreXcodeDerivedDataDir.asFile.absolutePath,
        "build",
    )
    doFirst {
        delete(swiftOnlyCoreXcodeDerivedDataDir.asFile)
    }
}

tasks.register("swiftXcodeOnlyCoreSupportGate") {
    group = "verification"
    description = "Validates the Gradle-free core SwiftPM and Xcode support workflow."
    dependsOn(
        "swiftXcodeOnlyCoreBuildGeneratedPackage",
        "swiftXcodeOnlyCoreSwiftTest",
        "swiftXcodeOnlyCoreXcodeBuild",
    )
}

tasks.register<Exec>("swiftXcodeOnlySyncGenerate") {
    group = "verification"
    description = "Runs the Gradle-free SwiftPM generator for the sync Swift-only fixture."

    dependsOn(
        gradle.includedBuild("sqlitenow-compiler").task(":syncSwiftPmPluginCompilerJar"),
        "swiftSyncRuntimeDebugArtifactManifestDefaults",
    )

    inputs.file(swiftOnlySyncFixtureDir.file("Package.swift"))
    inputs.file(swiftOnlySyncFixtureDir.file("SQLiteNow.json"))
    inputs.dir(swiftOnlySyncFixtureDir.dir("SQLiteNow"))
    outputs.dir(swiftOnlySyncGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "package",
        "--package-path",
        swiftOnlySyncFixtureDir.asFile.absolutePath,
        "plugin",
        "--allow-writing-to-package-directory",
        "sqlitenow-generate",
    )
    doFirst {
        environment.remove("SQLITENOW_COMPILER_JAR")
    }
}

tasks.register<Exec>("swiftXcodeOnlySyncBuildGeneratedPackage") {
    group = "verification"
    description = "Builds the Gradle-free generated sync Swift package with SwiftPM."

    dependsOn("swiftXcodeOnlySyncGenerate")

    inputs.file(swiftOnlySyncGeneratedPackageDir.file("Package.swift"))
    inputs.dir(swiftOnlySyncGeneratedPackageDir.dir("Sources"))
    inputs.dir(swiftOnlySyncGeneratedPackageDir.dir("Binaries"))
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "build",
        "--package-path",
        swiftOnlySyncGeneratedPackageDir.asFile.absolutePath,
    )
}

tasks.register<Exec>("swiftXcodeOnlySyncSwiftTest") {
    group = "verification"
    description = "Runs SwiftPM tests for the Gradle-free sync Swift consumer fixture."

    dependsOn("swiftXcodeOnlySyncGenerate")

    inputs.file(swiftOnlySyncConsumerDir.file("Package.swift"))
    inputs.dir(swiftOnlySyncConsumerDir.dir("Tests"))
    inputs.dir(swiftOnlySyncGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "test",
        "--package-path",
        swiftOnlySyncConsumerDir.asFile.absolutePath,
        "--scratch-path",
        swiftOnlySyncConsumerScratchDir.get().asFile.absolutePath,
    )
    doFirst {
        delete(swiftOnlySyncConsumerScratchDir.get().asFile)
    }
}

tasks.register<Exec>("swiftXcodeOnlySyncXcodeBuild") {
    group = "verification"
    description = "Builds the Swift-only sync Xcode consumer against the generated local package."

    dependsOn("swiftXcodeOnlySyncGenerate")

    inputs.dir(swiftOnlySyncXcodeProject)
    inputs.dir(swiftOnlySyncFixtureDir.dir("XcodeConsumer/SwiftOnlySyncXcodeApp"))
    inputs.dir(swiftOnlySyncGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("xcodebuild")?.absolutePath ?: "xcodebuild"
    args(
        "-project",
        swiftOnlySyncXcodeProject.asFile.absolutePath,
        "-scheme",
        "SwiftOnlySyncXcodeApp",
        "-destination",
        "generic/platform=iOS Simulator",
        "-derivedDataPath",
        swiftOnlySyncXcodeDerivedDataDir.asFile.absolutePath,
        "build",
    )
    doFirst {
        delete(swiftOnlySyncXcodeDerivedDataDir.asFile)
    }
}

tasks.register("swiftXcodeOnlySyncSupportGate") {
    group = "verification"
    description = "Validates the Gradle-free sync SwiftPM and Xcode support workflow."
    dependsOn(
        "swiftXcodeOnlySyncBuildGeneratedPackage",
        "swiftXcodeOnlySyncSwiftTest",
        "swiftXcodeOnlySyncXcodeBuild",
    )
}

tasks.register<Exec>("sampleSwiftGenerate") {
    group = "verification"
    description = "Runs the SwiftPM generator for the native Swift sample."

    dependsOn(
        gradle.includedBuild("sqlitenow-compiler").task(":syncSwiftPmPluginCompilerJar"),
        "swiftCoreRuntimeDebugArtifactManifestDefaults",
    )

    inputs.file(sampleSwiftDir.file("Package.swift"))
    inputs.file(sampleSwiftDir.file("SQLiteNow.json"))
    inputs.dir(sampleSwiftDir.dir("SQLiteNow"))
    outputs.dir(sampleSwiftGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "package",
        "--package-path",
        sampleSwiftDir.asFile.absolutePath,
        "plugin",
        "--allow-writing-to-package-directory",
        "sqlitenow-generate",
    )
    doFirst {
        environment.remove("SQLITENOW_COMPILER_JAR")
    }
}

tasks.register<Exec>("sampleSwiftBuildGeneratedPackage") {
    group = "verification"
    description = "Builds the generated native Swift sample package with SwiftPM."

    dependsOn("sampleSwiftGenerate")

    inputs.file(sampleSwiftGeneratedPackageDir.file("Package.swift"))
    inputs.dir(sampleSwiftGeneratedPackageDir.dir("Sources"))
    inputs.dir(sampleSwiftGeneratedPackageDir.dir("Binaries"))
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "build",
        "--package-path",
        sampleSwiftGeneratedPackageDir.asFile.absolutePath,
    )
}

registerSwiftSampleManifestValidationTask(
    name = "sampleSwiftValidateGeneratedPackageManifest",
    generateTaskName = "sampleSwiftGenerate",
    generatedPackageDir = sampleSwiftGeneratedPackageDir,
    databaseName = "NowSampleDatabase",
    swiftPackageName = "NowSampleDatabaseSQLiteNow",
    runtimeMode = "core",
    runtimeModuleName = "SQLiteNowCoreRuntime",
    expectedGeneratedSwiftFiles = listOf(
        "Sources/NowSampleDatabaseSQLiteNow/NowSampleDatabase.swift",
        "Sources/NowSampleDatabaseSQLiteNow/SQLiteNowSupport.swift",
    ),
)

registerSwiftSampleLeakCheckTask(
    name = "sampleSwiftCheckGeneratedPackageLeaks",
    generateTaskName = "sampleSwiftGenerate",
    generatedPackageDir = sampleSwiftGeneratedPackageDir,
    forbiddenTokenPatterns = listOf("NowSampleDatabaseBridge") + swiftSampleForbiddenTokenPatterns,
)

tasks.register<Exec>("sampleSwiftXcodeBuild") {
    group = "verification"
    description = "Builds the native Swift sample app against the generated local package."

    dependsOn("sampleSwiftBuildGeneratedPackage")

    inputs.dir(sampleSwiftXcodeProject)
    inputs.dir(sampleSwiftDir.dir("iosApp/iosApp"))
    inputs.dir(sampleSwiftGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("xcodebuild")?.absolutePath ?: "xcodebuild"
    args(
        "-project",
        sampleSwiftXcodeProject.asFile.absolutePath,
        "-scheme",
        "iosApp",
        "-destination",
        "generic/platform=iOS Simulator",
        "-derivedDataPath",
        sampleSwiftXcodeDerivedDataDir.asFile.absolutePath,
        "build",
    )
    doFirst {
        delete(sampleSwiftXcodeDerivedDataDir.asFile)
    }
}

tasks.register("sampleSwiftSupportGate") {
    group = "verification"
    description = "Validates the native Swift sample generation, package metadata, leak checks, package build, and Xcode build."
    dependsOn(
        "sampleSwiftBuildGeneratedPackage",
        "sampleSwiftValidateGeneratedPackageManifest",
        "sampleSwiftCheckGeneratedPackageLeaks",
        "sampleSwiftXcodeBuild",
    )
}

tasks.register<Exec>("sampleSyncSwiftGenerate") {
    group = "verification"
    description = "Runs the SwiftPM generator for the native Swift sync sample."

    dependsOn(
        gradle.includedBuild("sqlitenow-compiler").task(":syncSwiftPmPluginCompilerJar"),
        "swiftSyncRuntimeDebugArtifactManifestDefaults",
    )

    inputs.file(sampleSyncSwiftDir.file("Package.swift"))
    inputs.file(sampleSyncSwiftDir.file("SQLiteNow.json"))
    inputs.dir(sampleSyncSwiftDir.dir("SQLiteNow"))
    outputs.dir(sampleSyncSwiftGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "package",
        "--package-path",
        sampleSyncSwiftDir.asFile.absolutePath,
        "plugin",
        "--allow-writing-to-package-directory",
        "sqlitenow-generate",
    )
    doFirst {
        environment.remove("SQLITENOW_COMPILER_JAR")
    }
}

tasks.register<Exec>("sampleSyncSwiftBuildGeneratedPackage") {
    group = "verification"
    description = "Builds the generated native Swift sync sample package with SwiftPM."

    dependsOn("sampleSyncSwiftGenerate")

    inputs.file(sampleSyncSwiftGeneratedPackageDir.file("Package.swift"))
    inputs.dir(sampleSyncSwiftGeneratedPackageDir.dir("Sources"))
    inputs.dir(sampleSyncSwiftGeneratedPackageDir.dir("Binaries"))
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("swift")?.absolutePath ?: "swift"
    args(
        "build",
        "--package-path",
        sampleSyncSwiftGeneratedPackageDir.asFile.absolutePath,
    )
}

registerSwiftSampleManifestValidationTask(
    name = "sampleSyncSwiftValidateGeneratedPackageManifest",
    generateTaskName = "sampleSyncSwiftGenerate",
    generatedPackageDir = sampleSyncSwiftGeneratedPackageDir,
    databaseName = "NowSampleSyncDatabase",
    swiftPackageName = "NowSampleSyncDatabaseSQLiteNow",
    runtimeMode = "sync",
    runtimeModuleName = "SQLiteNowSyncRuntime",
    expectedGeneratedSwiftFiles = listOf(
        "Sources/NowSampleSyncDatabaseSQLiteNow/NowSampleSyncDatabase.swift",
        "Sources/NowSampleSyncDatabaseSQLiteNow/SQLiteNowSupport.swift",
        "Sources/NowSampleSyncDatabaseSQLiteNow/SQLiteNowSyncSupport.swift",
    ),
    expectedSyncTables = listOf(
        "comment" to "id",
        "person" to "id",
        "person_address" to "id",
    ),
)

registerSwiftSampleLeakCheckTask(
    name = "sampleSyncSwiftCheckGeneratedPackageLeaks",
    generateTaskName = "sampleSyncSwiftGenerate",
    generatedPackageDir = sampleSyncSwiftGeneratedPackageDir,
    forbiddenTokenPatterns = listOf(
        "NowSampleDatabaseBridge",
        "NowSampleSyncDatabaseBridge",
    ) + swiftSampleForbiddenTokenPatterns,
)

tasks.register<Exec>("sampleSyncSwiftXcodeBuild") {
    group = "verification"
    description = "Builds the native Swift sync sample app against the generated local package."

    dependsOn("sampleSyncSwiftBuildGeneratedPackage")

    inputs.dir(sampleSyncSwiftXcodeProject)
    inputs.dir(sampleSyncSwiftDir.dir("iosApp/iosApp"))
    inputs.dir(sampleSyncSwiftGeneratedPackageDir)
    outputs.upToDateWhen { false }

    workingDir = rootDir
    executable = findExecutableOnPath("xcodebuild")?.absolutePath ?: "xcodebuild"
    args(
        "-project",
        sampleSyncSwiftXcodeProject.asFile.absolutePath,
        "-scheme",
        "iosApp",
        "-destination",
        "generic/platform=iOS Simulator",
        "-derivedDataPath",
        sampleSyncSwiftXcodeDerivedDataDir.asFile.absolutePath,
        "build",
    )
    doFirst {
        delete(sampleSyncSwiftXcodeDerivedDataDir.asFile)
    }
}

tasks.register("sampleSyncSwiftSupportGate") {
    group = "verification"
    description = "Validates the native Swift sync sample generation, package metadata, leak checks, package build, and Xcode build."
    dependsOn(
        "sampleSyncSwiftBuildGeneratedPackage",
        "sampleSyncSwiftValidateGeneratedPackageManifest",
        "sampleSyncSwiftCheckGeneratedPackageLeaks",
        "sampleSyncSwiftXcodeBuild",
    )
}

tasks.register("swiftHybridSupportGate") {
    group = "verification"
    description = "Validates the supported local Swift package workflows for core and sync."
    dependsOn(
        "sampleSwiftSupportGate",
        "sampleSyncSwiftSupportGate",
        ":swift:fixtures:package:core:swiftTest",
        ":swift:fixtures:package:sync:swiftTest",
        ":library-core:jvmTest",
        "oversqliteComprehensive",
    )
}

tasks.register("swiftHybridSupportRealserverGate") {
    group = "verification"
    description = "Runs the supported Swift hybrid gate plus the generated sync package realserver smoke."
    dependsOn(
        "swiftHybridSupportGate",
        ":swift:fixtures:package:sync:swiftRealserverSmoke",
    )
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
        "corePlatformLinuxX64",
        "corePlatformLinuxArm64",
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
    name = "corePlatformLinuxX64",
    description = "Runs the Linux x64 SQLiteNow core platform harness on a compatible Linux host.",
    arguments = listOf(":platform-core-test:harness:linuxX64Test"),
)

registerCorePlatformExecTask(
    name = "corePlatformLinuxArm64",
    description = "Runs the Linux arm64 SQLiteNow core platform harness on a compatible Linux ARM host.",
    arguments = listOf(":platform-core-test:harness:linuxArm64Test"),
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
    dependsOn(":library-oversqlite:oversqliteRealserverJvm")
}

tasks.register("oversqliteRealserverJvmHeavy") {
    group = "verification"
    description = "Runs the JVM heavy oversqlite realserver scenario."
    dependsOn(":library-oversqlite:jvmRealServerSharedConnectionStress")
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
