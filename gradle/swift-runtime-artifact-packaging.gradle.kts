import java.io.File
import java.security.MessageDigest
import org.gradle.api.tasks.bundling.Zip

val swiftRuntimeMode = extra["swiftRuntimeMode"] as? String
    ?: error("swiftRuntimeMode extra is required.")
val swiftRuntimeFrameworkModuleName = extra["swiftRuntimeFrameworkModuleName"] as? String
    ?: error("swiftRuntimeFrameworkModuleName extra is required.")
val swiftPackageDefaultAppleTargets = listOf("macosArm64", "iosArm64", "iosSimulatorArm64")
val swiftPackageDefaultMinimumIos = "15"
val swiftPackageDefaultMinimumMacos = "14"
val swiftRuntimeAppleTargets = listOf(
    "macosArm64" to "MacosArm64",
    "iosArm64" to "IosArm64",
    "iosSimulatorArm64" to "IosSimulatorArm64",
)
require(swiftRuntimeAppleTargets.map { it.first } == swiftPackageDefaultAppleTargets) {
    "Swift runtime packaging targets must match Swift package defaults. " +
        "Expected $swiftPackageDefaultAppleTargets, got ${swiftRuntimeAppleTargets.map { it.first }}."
}
val runtimeXcframeworkDir = layout.buildDirectory.dir("runtime/$swiftRuntimeFrameworkModuleName.xcframework")
val runtimeArtifactsDir = layout.buildDirectory.dir("runtime-artifacts")
val releaseRuntimeXcframeworkDir =
    layout.buildDirectory.dir("runtime-release/$swiftRuntimeFrameworkModuleName.xcframework")
val releaseRuntimeArtifactsDir = layout.buildDirectory.dir("runtime-release-artifacts")
val sqliteNowVersion = providers.gradleProperty("sqlitenow.version").orElse(project.version.toString())
val swiftRuntimeArtifactBaseUrl = providers.gradleProperty("sqlitenow.swiftRuntimeArtifactBaseUrl").orElse("")

fun findExecutableOnPath(name: String): File? =
    System.getenv("PATH")
        .orEmpty()
        .split(File.pathSeparator)
        .asSequence()
        .filter { it.isNotBlank() }
        .map { File(it, name) }
        .firstOrNull { it.isFile && it.canExecute() }

fun sha256(file: File): String {
    val digest = MessageDigest.getInstance("SHA-256")
    file.inputStream().buffered().use { input ->
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
            val read = input.read(buffer)
            if (read < 0) {
                break
            }
            digest.update(buffer, 0, read)
        }
    }
    return digest.digest().joinToString("") { "%02x".format(it) }
}

fun jsonString(value: String): String =
    "\"" + value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r") + "\""

fun jsonStringArray(values: List<String>): String =
    values.joinToString(prefix = "[", postfix = "]") { jsonString(it) }

fun registerSwiftRuntimeXcframeworkTask(
    taskName: String,
    buildType: String,
    buildDirectoryName: String,
    outputDirectory: org.gradle.api.provider.Provider<org.gradle.api.file.Directory>,
) {
    tasks.register(taskName) {
        group = "build"
        description = if (buildType == "Debug") {
            "Builds the reusable SQLiteNow $swiftRuntimeMode runtime XCFramework for SwiftPM smoke tests."
        } else {
            "Builds the releasable SQLiteNow $swiftRuntimeMode runtime XCFramework for SwiftPM binary artifacts."
        }

        dependsOn(swiftRuntimeAppleTargets.map { (_, taskSuffix) -> "link${buildType}Framework$taskSuffix" })

        swiftRuntimeAppleTargets.forEach { (target, _) ->
            inputs.dir(layout.buildDirectory.dir("bin/$target/$buildDirectoryName/$swiftRuntimeFrameworkModuleName.framework"))
        }
        outputs.dir(outputDirectory)

        doLast {
            val xcodebuild = findExecutableOnPath("xcodebuild")
                ?: error("xcodebuild is required to create the SQLiteNow $swiftRuntimeMode runtime XCFramework.")

            val frameworkPaths = swiftRuntimeAppleTargets.map { (target, _) ->
                target to layout.buildDirectory
                    .file("bin/$target/$buildDirectoryName/$swiftRuntimeFrameworkModuleName.framework")
                    .get()
                    .asFile
            }
            frameworkPaths.forEach { (target, framework) ->
                require(framework.exists()) {
                    "Expected $target ${buildType.lowercase()} framework at ${framework.absolutePath}. " +
                        "Run the matching link${buildType}Framework task first."
                }
            }

            val outputDir = outputDirectory.get().asFile
            project.delete(outputDir)
            outputDir.parentFile.mkdirs()

            val xcodebuildArgs = buildList {
                add("-create-xcframework")
                frameworkPaths.forEach { (_, framework) ->
                    add("-framework")
                    add(framework.absolutePath)
                }
                add("-output")
                add(outputDir.absolutePath)
            }
            providers.exec {
                commandLine(listOf(xcodebuild.absolutePath) + xcodebuildArgs)
            }.result.get().assertNormalExitValue()

            val artifactKind = if (buildType == "Debug") "reusable" else "release"
            logger.lifecycle(
                "Generated $artifactKind SQLiteNow $swiftRuntimeMode runtime XCFramework at ${outputDir.absolutePath}"
            )
        }
    }
}

fun registerSwiftRuntimeArtifactTask(
    taskName: String,
    buildType: String,
    xcframeworkDirectory: org.gradle.api.provider.Provider<org.gradle.api.file.Directory>,
    artifactsDirectory: org.gradle.api.provider.Provider<org.gradle.api.file.Directory>,
) {
    tasks.register<Zip>(taskName) {
        group = "build"
        description = if (buildType == "Debug") {
            "Builds the versioned SQLiteNow $swiftRuntimeMode runtime XCFramework zip and artifact metadata."
        } else {
            "Builds the releasable SQLiteNow $swiftRuntimeMode runtime XCFramework zip and artifact metadata."
        }

        dependsOn("package${buildType}RuntimeXcframework")

        archiveFileName.set(sqliteNowVersion.map { "$swiftRuntimeFrameworkModuleName-$it.xcframework.zip" })
        destinationDirectory.set(artifactsDirectory)
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
        from(xcframeworkDirectory) {
            into("$swiftRuntimeFrameworkModuleName.xcframework")
            eachFile {
                if (name == swiftRuntimeFrameworkModuleName && path.contains(".framework/")) {
                    permissions {
                        unix("rwxr-xr-x")
                    }
                }
            }
        }

        inputs.dir(xcframeworkDirectory)
        inputs.property("sqliteNowVersion", sqliteNowVersion)
        if (buildType == "Release") {
            inputs.property("swiftRuntimeArtifactBaseUrl", swiftRuntimeArtifactBaseUrl)
        }
        outputs.dir(artifactsDirectory)

        doFirst {
            val version = sqliteNowVersion.get()
            val xcframework = xcframeworkDirectory.get().asFile
            require(xcframework.isDirectory) {
                "Expected SQLiteNow $swiftRuntimeMode runtime XCFramework at ${xcframework.absolutePath}."
            }

            val outputDir = artifactsDirectory.get().asFile
            val artifactZip = outputDir.resolve("$swiftRuntimeFrameworkModuleName-$version.xcframework.zip")
            val artifactManifest = outputDir.resolve("$swiftRuntimeFrameworkModuleName-$version.artifact-manifest.json")
            project.delete(artifactZip, artifactManifest)
            outputDir.mkdirs()
        }

        doLast {
            val version = sqliteNowVersion.get()
            val xcframework = xcframeworkDirectory.get().asFile
            val artifactZip = archiveFile.get().asFile
            val outputDir = artifactsDirectory.get().asFile
            val artifactManifest = outputDir.resolve("$swiftRuntimeFrameworkModuleName-$version.artifact-manifest.json")
            val checksum = sha256(artifactZip)
            val artifactRelativePath = artifactZip.relativeTo(project.rootDir).invariantSeparatorsPath
            val xcframeworkRelativePath = xcframework.relativeTo(project.rootDir).invariantSeparatorsPath
            val artifactUrl = if (buildType == "Release") {
                val baseUrl = swiftRuntimeArtifactBaseUrl.get().trim().trimEnd('/')
                baseUrl.takeIf { it.isNotEmpty() }?.let { "$it/${artifactZip.name}" }
            } else {
                null
            }
            artifactManifest.writeText(
                """
                {
                  "manifestVersion": 1,
                  "sqliteNowVersion": ${jsonString(version)},
                  "runtimeMode": ${jsonString(swiftRuntimeMode)},
                  "runtimeModuleName": ${jsonString(swiftRuntimeFrameworkModuleName)},
                  "artifactName": ${jsonString(artifactZip.name)},
                  "artifactPath": ${jsonString(artifactRelativePath)},
                  "artifactUrl": ${artifactUrl?.let(::jsonString) ?: "null"},
                  "checksum": ${jsonString(checksum)},
                  "checksumAlgorithm": "swiftpm-sha256",
                  "checksumCommand": ${jsonString("swift package compute-checksum $artifactRelativePath")},
                  "frameworkMode": "dynamic",
                  "requestedAppleTargets": ${jsonStringArray(swiftPackageDefaultAppleTargets)},
                  "minimumPlatforms": {
                    "iOS": ${jsonString(swiftPackageDefaultMinimumIos)},
                    "macOS": ${jsonString(swiftPackageDefaultMinimumMacos)}
                  },
                  "sourceXcframeworkPath": ${jsonString(xcframeworkRelativePath)}
                }
                """.trimIndent() + "\n"
            )

            val artifactKind = if (buildType == "Debug") "artifact" else "release artifact"
            logger.lifecycle("Generated SQLiteNow $swiftRuntimeMode runtime $artifactKind at ${artifactZip.absolutePath}")
            logger.lifecycle("SwiftPM checksum: $checksum")
        }
    }
}

registerSwiftRuntimeXcframeworkTask(
    taskName = "packageDebugRuntimeXcframework",
    buildType = "Debug",
    buildDirectoryName = "debugFramework",
    outputDirectory = runtimeXcframeworkDir,
)
registerSwiftRuntimeArtifactTask(
    taskName = "packageDebugRuntimeArtifact",
    buildType = "Debug",
    xcframeworkDirectory = runtimeXcframeworkDir,
    artifactsDirectory = runtimeArtifactsDir,
)
registerSwiftRuntimeXcframeworkTask(
    taskName = "packageReleaseRuntimeXcframework",
    buildType = "Release",
    buildDirectoryName = "releaseFramework",
    outputDirectory = releaseRuntimeXcframeworkDir,
)
registerSwiftRuntimeArtifactTask(
    taskName = "packageReleaseRuntimeArtifact",
    buildType = "Release",
    xcframeworkDirectory = releaseRuntimeXcframeworkDir,
    artifactsDirectory = releaseRuntimeArtifactsDir,
)
