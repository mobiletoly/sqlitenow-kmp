import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.security.MessageDigest
import java.util.Base64
import org.gradle.api.Project
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.bundling.Zip

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

fun fileUrl(file: File): String =
    "file://${file.absoluteFile.toURI().rawPath}"

val sqliteNowVersion = providers.gradleProperty("sqlitenow.version")
val swiftPmPluginDistributionDir = layout.buildDirectory.dir("swiftpm-plugin-distribution/sqlitenow-kmp")
val swiftPmPluginDistributionGitDir = layout.buildDirectory.dir("swiftpm-plugin-distribution-git/sqlitenow-kmp")
val swiftPmPluginReleaseTemplateDir = layout.projectDirectory.dir("gradle/swiftpm-plugin-release")
val swiftPmPluginReleasePackageTemplate = swiftPmPluginReleaseTemplateDir.file("Package.swift.template")
val swiftPmPluginReleaseMarkerPath = ".sqlitenow/release-distribution.json"
val swiftPmCompilerArtifactBundleDir = layout.buildDirectory.dir("swiftpm-compiler-artifactbundle/SQLiteNowCompiler.artifactbundle")
val swiftPmCompilerArtifactsDir = layout.buildDirectory.dir("swiftpm-compiler-artifacts")
val defaultSwiftPmPluginCleanConsumerDir = layout.buildDirectory.dir("swiftpm-plugin-clean-consumer")
val defaultSwiftPmPluginLocalTagCleanConsumerDir = layout.buildDirectory.dir("swiftpm-plugin-local-tag-clean-consumer")
val defaultSwiftPmPluginPublicTagCleanConsumerDir = layout.buildDirectory.dir("swiftpm-plugin-public-tag-clean-consumer")
val defaultSwiftPmPluginPublicTagXcodeConsumerDir = providers.provider {
    val tempDir = System.getenv("TMPDIR")
        ?.takeIf { it.isNotBlank() }
        ?: System.getProperty("java.io.tmpdir")
    File(tempDir, "sqlitenow-swiftpm-public-tag-xcode-clean-consumer").absoluteFile
}
val defaultSwiftExternalXcodeConsumerDir = providers.provider {
    val tempDir = System.getenv("TMPDIR")
        ?.takeIf { it.isNotBlank() }
        ?: System.getProperty("java.io.tmpdir")
    File(tempDir, "sqlitenow-swift-external-xcode-consumer").absoluteFile
}
val swiftPmPluginCleanConsumerRoot = providers.gradleProperty("sqlitenow.swiftPmCleanConsumerRoot")
val swiftPmPluginCleanConsumerDistributionUrl =
    providers.gradleProperty("sqlitenow.swiftPmCleanConsumerDistributionUrl")
val swiftPmPluginPublicTagXcodeConsumerRoot =
    providers.gradleProperty("sqlitenow.swiftPmPublicTagXcodeConsumerRoot")
val swiftExternalXcodeConsumerRoot = providers.gradleProperty("sqlitenow.swiftExternalXcodeConsumerRoot")
val emptySwiftPmCleanConsumerDistributionUrl = providers.provider { "" }
val swiftPmCleanConsumerTemplateDir = layout.projectDirectory.dir("swift/fixtures/swiftpm/clean-consumer-template")
val defaultSwiftReleaseArtifactBaseUrl =
    sqliteNowVersion.map { "https://github.com/mobiletoly/sqlitenow-kmp/releases/download/v$it" }
val swiftPmCompilerArtifactBaseUrl = providers.gradleProperty("sqlitenow.swiftPmCompilerArtifactBaseUrl")
    .orElse(defaultSwiftReleaseArtifactBaseUrl)
val swiftRuntimeArtifactBaseUrl = providers.gradleProperty("sqlitenow.swiftRuntimeArtifactBaseUrl")
    .orElse(defaultSwiftReleaseArtifactBaseUrl)
val swiftPmPluginDistributionTagName = providers.environmentVariable("SQLITENOW_SWIFTPM_TAG")
    .orElse(sqliteNowVersion)
val githubRepository = providers.environmentVariable("GITHUB_REPOSITORY")
val githubToken = providers.environmentVariable("GITHUB_TOKEN")
val swiftPmCompilerJar = layout.projectDirectory.file(
    "sqlitenow-compiler/build/libs/sqlitenow-compiler-${sqliteNowVersion.get()}-compiler.jar",
)

enum class SwiftCleanConsumerProofMode {
    LOCAL_ARTIFACTS_LOCAL_GIT,
    REMOTE_ARTIFACTS_LOCAL_GIT,
    REMOTE_ARTIFACTS_PUBLIC_GIT,
}

data class SwiftPmGitIdentity(
    val userName: String,
    val userEmail: String,
)

data class ProcessCommandResult(
    val exitCode: Int,
    val output: String,
)

data class SwiftPmLeak(
    val relativePath: String,
    val value: String,
)

fun findSwiftPmLeaks(
    files: Iterable<File>,
    rootDirectory: File,
    forbiddenLiteralValues: Iterable<String>,
): List<SwiftPmLeak> {
    val literals = forbiddenLiteralValues
        .map { it.trim() }
        .filter { it.isNotEmpty() }
        .distinct()
    if (literals.isEmpty()) {
        return emptyList()
    }

    val root = rootDirectory.canonicalFile
    return files
        .filter { it.isFile }
        .flatMap { file ->
            val text = file.readText()
            val relativePath = runCatching { file.canonicalFile.relativeTo(root).invariantSeparatorsPath }
                .getOrElse { file.absolutePath }
            literals
                .filter { literal -> text.contains(literal) }
                .map { literal -> SwiftPmLeak(relativePath = relativePath, value = literal) }
        }
        .distinct()
}

val swiftPmProofGitIdentity = SwiftPmGitIdentity(
    userName = "SQLiteNow SwiftPM Proof",
    userEmail = "swiftpm-proof@example.invalid",
)
val swiftPmGitHubActionsIdentity = SwiftPmGitIdentity(
    userName = "github-actions[bot]",
    userEmail = "41898282+github-actions[bot]@users.noreply.github.com",
)

fun swiftPmReleaseCompilerBinaryTarget(
    compilerArtifactUrl: String,
    compilerArtifactChecksum: String,
): String =
    listOf(
        "        .binaryTarget(",
        "            name: \"SQLiteNowCompiler\",",
        "            url: ${jsonString(compilerArtifactUrl)},",
        "            checksum: ${jsonString(compilerArtifactChecksum)}",
        "        ),",
    ).joinToString("\n")

fun swiftPmLocalCompilerBinaryTarget(
    compilerArtifactPath: String,
): String =
    listOf(
        "        .binaryTarget(",
        "            name: \"SQLiteNowCompiler\",",
        "            path: ${jsonString(compilerArtifactPath)}",
        "        ),",
    ).joinToString("\n")

fun swiftPmReleaseRuntimeBinaryTarget(
    runtimeModuleName: String,
    runtimeArtifactUrl: String,
    runtimeArtifactChecksum: String,
): String =
    listOf(
        "        .binaryTarget(",
        "            name: ${jsonString(runtimeModuleName)},",
        "            url: ${jsonString(runtimeArtifactUrl)},",
        "            checksum: ${jsonString(runtimeArtifactChecksum)}",
        "        ),",
    ).joinToString("\n")

fun swiftPmLocalRuntimeBinaryTarget(
    runtimeModuleName: String,
    runtimeArtifactPath: String,
): String =
    listOf(
        "        .binaryTarget(",
        "            name: ${jsonString(runtimeModuleName)},",
        "            path: ${jsonString(runtimeArtifactPath)}",
        "        ),",
    ).joinToString("\n")

fun writeSwiftPmPackageManifest(
    templateFile: File,
    packageSwift: File,
    compilerBinaryTarget: String,
    coreRuntimeBinaryTarget: String,
    syncRuntimeBinaryTarget: String,
) {
    renderTemplateFile(
        templateFile,
        packageSwift,
        mapOf(
            "SQLITENOW_COMPILER_BINARY_TARGET" to compilerBinaryTarget,
            "SQLITENOW_CORE_RUNTIME_BINARY_TARGET" to coreRuntimeBinaryTarget,
            "SQLITENOW_SYNC_RUNTIME_BINARY_TARGET" to syncRuntimeBinaryTarget,
        ),
    )
}

fun assertTextOnlySwiftPmDistribution(distributionDir: File) {
    val forbiddenFileSuffixes = listOf(
        ".a",
        ".artifactbundle.zip",
        ".dylib",
        ".jar",
        ".so",
        ".wasm",
        ".xcframework.zip",
        ".zip",
    )
    val forbiddenDirectories = listOf(
        ".artifactbundle",
        ".xcframework",
    )
    val forbidden = distributionDir.walkTopDown()
        .filter { file ->
            (file.isFile && forbiddenFileSuffixes.any { suffix -> file.name.endsWith(suffix) }) ||
                (file.isDirectory && forbiddenDirectories.any { suffix -> file.name.endsWith(suffix) })
        }
        .map { it.relativeTo(distributionDir).invariantSeparatorsPath }
        .toList()
    val nonTextFiles = distributionDir.walkTopDown()
        .filter { it.isFile }
        .filter { file ->
            val bytes = file.readBytes()
            bytes.any { it == 0.toByte() } || runCatching {
                Charsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
            }.isFailure
        }
        .map { it.relativeTo(distributionDir).invariantSeparatorsPath }
        .toList()
    val executableFiles = distributionDir.walkTopDown()
        .filter { it.isFile && it.canExecute() }
        .map { it.relativeTo(distributionDir).invariantSeparatorsPath }
        .toList()

    if (forbidden.isNotEmpty() || nonTextFiles.isNotEmpty() || executableFiles.isNotEmpty()) {
        throw GradleException(
            "SwiftPM distribution tree must be text-only. " +
                "Found binary artifact paths: ${forbidden.joinToString(", ")}; " +
                "non-UTF-8 files: ${nonTextFiles.joinToString(", ")}; " +
                "executable files: ${executableFiles.joinToString(", ")}"
        )
    }
}

fun assertInspectableSwiftPmDistribution(distributionDir: File) {
    assertTextOnlySwiftPmDistribution(distributionDir)
    if (distributionDir.resolve(".git").exists()) {
        throw GradleException(
            "Staged SwiftPM distribution must not contain Git metadata: " +
                distributionDir.resolve(".git").absolutePath
        )
    }
}

fun renderTemplateFile(templateFile: File, outputFile: File, values: Map<String, String>) {
    var rendered = templateFile.readText()
    for ((key, value) in values) {
        rendered = rendered.replace("{{$key}}", value)
    }
    if (rendered.contains("{{")) {
        throw GradleException("Unresolved template token in ${templateFile.absolutePath}.")
    }
    outputFile.parentFile.mkdirs()
    outputFile.writeText(rendered)
}

fun Project.runSwiftPmReleaseCommand(
    workingDir: File,
    command: List<String>,
    configure: org.gradle.process.ExecSpec.() -> Unit = {},
) {
    providers.exec {
        this.workingDir = workingDir
        commandLine(command)
        configure()
    }.result.get().assertNormalExitValue()
}

fun runProcessCommand(
    workingDir: File,
    command: List<String>,
    allowFailure: Boolean = false,
    environment: Map<String, String> = emptyMap(),
): ProcessCommandResult {
    val process = ProcessBuilder(command)
        .directory(workingDir)
        .redirectErrorStream(true)
        .also { builder -> builder.environment().putAll(environment) }
        .start()
    val output = process.inputStream.bufferedReader().use { it.readText() }
    val exitCode = process.waitFor()
    if (!allowFailure && exitCode != 0) {
        throw GradleException(
            "Command failed with exit code $exitCode: ${command.joinToString(" ")}\n$output"
        )
    }
    return ProcessCommandResult(exitCode = exitCode, output = output)
}

fun gitHubTokenGitEnvironment(token: String): Map<String, String> =
    mapOf(
        "GIT_CONFIG_COUNT" to "1",
        "GIT_CONFIG_KEY_0" to "http.https://github.com/.extraheader",
        "GIT_CONFIG_VALUE_0" to "AUTHORIZATION: basic ${
            Base64.getEncoder().encodeToString("x-access-token:$token".toByteArray(Charsets.UTF_8))
        }",
    )

fun Project.createSwiftPmDistributionGitCommit(
    distributionDir: File,
    git: File,
    version: String,
    identity: SwiftPmGitIdentity,
    resetGitDirectory: Boolean,
) {
    if (resetGitDirectory) {
        delete(distributionDir.resolve(".git"))
    }
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "init"))
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "config", "user.name", identity.userName))
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "config", "user.email", identity.userEmail))
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "add", "."))
    runSwiftPmReleaseCommand(
        distributionDir,
        listOf(git.absolutePath, "commit", "-m", "SQLiteNow SwiftPM distribution $version"),
    )
}

fun Project.copySwiftPmDistributionToGitWorkDir(
    distributionSource: File,
    gitWorkDir: File,
) {
    delete(gitWorkDir)
    copy {
        from(distributionSource) {
            exclude(".git/**")
        }
        into(gitWorkDir)
    }
}

fun Project.createSwiftPmDistributionGitTag(
    distributionDir: File,
    git: File,
    version: String,
    tagName: String,
) {
    runSwiftPmReleaseCommand(
        distributionDir,
        listOf(git.absolutePath, "tag", "-a", tagName, "-m", "SQLiteNow SwiftPM distribution $version"),
    )
}

fun Project.publishSwiftPmDistributionGitTag(
    distributionDir: File,
    git: File,
    version: String,
    tagName: String,
    repository: String,
    token: String,
) {
    val remoteUrl = "https://github.com/$repository.git"
    val authEnvironment = gitHubTokenGitEnvironment(token)
    createSwiftPmDistributionGitCommit(
        distributionDir = distributionDir,
        git = git,
        version = version,
        identity = swiftPmGitHubActionsIdentity,
        resetGitDirectory = true,
    )
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "remote", "add", "origin", remoteUrl))

    val remoteTag = runProcessCommand(
        distributionDir,
        listOf(git.absolutePath, "ls-remote", "--exit-code", "--tags", "origin", "refs/tags/$tagName"),
        allowFailure = true,
        environment = authEnvironment,
    )
    if (remoteTag.exitCode == 0) {
        runSwiftPmReleaseCommand(
            distributionDir,
            listOf(git.absolutePath, "fetch", "--depth=1", "origin", "refs/tags/$tagName:refs/tags/$tagName"),
        ) {
            environment(authEnvironment)
        }
        val stagedTree = runProcessCommand(distributionDir, listOf(git.absolutePath, "rev-parse", "HEAD^{tree}"))
            .output
            .trim()
        val publishedTree = runProcessCommand(
            distributionDir,
            listOf(git.absolutePath, "rev-parse", "refs/tags/$tagName^{tree}"),
        ).output.trim()
        if (stagedTree == publishedTree) {
            logger.lifecycle("SwiftPM distribution tag $tagName already exists with matching content; continuing.")
            return
        }
        throw GradleException("SwiftPM distribution tag $tagName already exists with different content.")
    }
    if (remoteTag.exitCode != 2) {
        throw GradleException(
            "Unable to inspect SwiftPM distribution tag $tagName in $repository.\n${remoteTag.output}"
        )
    }

    createSwiftPmDistributionGitTag(
        distributionDir = distributionDir,
        git = git,
        version = version,
        tagName = tagName,
    )
    runSwiftPmReleaseCommand(distributionDir, listOf(git.absolutePath, "push", "origin", "refs/tags/$tagName")) {
        environment(authEnvironment)
    }
}

fun remoteRuntimeArtifactJson(url: String, checksum: String, version: String): String =
    """
    {
        "kind": "remoteZip",
        "url": ${jsonString(url)},
        "checksum": ${jsonString(checksum)},
        "sqliteNowVersion": ${jsonString(version)}
      }
    """.trimIndent()

fun localRuntimeArtifactJson(path: String, checksum: String, version: String): String =
    """
    {
        "kind": "localZip",
        "path": ${jsonString(path)},
        "checksum": ${jsonString(checksum)},
        "sqliteNowVersion": ${jsonString(version)}
      }
    """.trimIndent()

tasks.register("swiftRuntimeReleaseArtifacts") {
    group = "build"
    description = "Builds release-mode SQLiteNow Swift runtime XCFramework zip artifacts for GitHub Releases."
    dependsOn(
        ":swift:runtime:core:packageReleaseRuntimeArtifact",
        ":swift:runtime:sync:packageReleaseRuntimeArtifact",
    )
}

tasks.register("prepareSwiftPmCompilerArtifactBundle") {
    group = "build"
    description = "Prepares the SwiftPM executable artifact bundle directory that wraps the SQLiteNow compiler jar."

    dependsOn(gradle.includedBuild("sqlitenow-compiler").task(":sqlitenowCompilerJar"))

    inputs.file(swiftPmCompilerJar)
    inputs.property("sqliteNowVersion", sqliteNowVersion)
    outputs.dir(swiftPmCompilerArtifactBundleDir)

    doLast {
        val version = sqliteNowVersion.get()
        val jarFile = swiftPmCompilerJar.asFile
        require(jarFile.isFile) {
            "Expected SQLiteNow compiler jar at ${jarFile.absolutePath}."
        }

        val bundleRoot = swiftPmCompilerArtifactBundleDir.get().asFile
        val toolRoot = bundleRoot.resolve("SQLiteNowCompiler")
        val binDir = toolRoot.resolve("bin")
        val libDir = toolRoot.resolve("lib")
        project.delete(bundleRoot)
        binDir.mkdirs()
        libDir.mkdirs()

        jarFile.copyTo(libDir.resolve("sqlitenow-compiler.jar"), overwrite = true)
        val wrapper = binDir.resolve("sqlitenow-compiler")
        wrapper.writeText(
            """
            #!/bin/sh
            set -eu
            DIR="${'$'}(cd "${'$'}(dirname "${'$'}0")" && pwd)"
            exec /usr/bin/env java -jar "${'$'}DIR/../lib/sqlitenow-compiler.jar" "${'$'}@"
            """.trimIndent() + "\n"
        )
        require(wrapper.setExecutable(true, false)) {
            "Unable to mark ${wrapper.absolutePath} executable."
        }

        bundleRoot.resolve("info.json").writeText(
            """
            {
              "schemaVersion": "1.0",
              "artifacts": {
                "SQLiteNowCompiler": {
                  "type": "executable",
                  "version": ${jsonString(version)},
                  "variants": [
                    {
                      "path": "SQLiteNowCompiler/bin/sqlitenow-compiler",
                      "supportedTriples": ${jsonStringArray(
                listOf(
                    "arm64-apple-macosx",
                    "aarch64-unknown-linux-gnu",
                    "x86_64-unknown-linux-gnu",
                )
            )}
                    }
                  ]
                }
              }
            }
            """.trimIndent() + "\n"
        )
    }
}

tasks.register<Zip>("packageSwiftPmCompilerArtifactBundle") {
    group = "build"
    description = "Builds the SwiftPM executable artifact bundle that wraps the SQLiteNow compiler jar."

    dependsOn("prepareSwiftPmCompilerArtifactBundle")

    archiveFileName.set(sqliteNowVersion.map { "SQLiteNowCompiler-$it.artifactbundle.zip" })
    destinationDirectory.set(swiftPmCompilerArtifactsDir)
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    from(swiftPmCompilerArtifactBundleDir) {
        into("SQLiteNowCompiler.artifactbundle")
        exclude("SQLiteNowCompiler/bin/sqlitenow-compiler")
    }
    from(swiftPmCompilerArtifactBundleDir.map { it.file("SQLiteNowCompiler/bin/sqlitenow-compiler") }) {
        into("SQLiteNowCompiler.artifactbundle/SQLiteNowCompiler/bin")
        filePermissions {
            unix("rwxr-xr-x")
        }
    }

    inputs.file(swiftPmCompilerJar)
    inputs.property("sqliteNowVersion", sqliteNowVersion)
    inputs.property("swiftPmCompilerArtifactBaseUrl", swiftPmCompilerArtifactBaseUrl)
    outputs.dir(swiftPmCompilerArtifactsDir)

    doFirst {
        val version = sqliteNowVersion.get()
        val outputDir = swiftPmCompilerArtifactsDir.get().asFile
        project.delete(
            outputDir.resolve("SQLiteNowCompiler-$version.artifactbundle.zip"),
            outputDir.resolve("SQLiteNowCompiler-$version.artifact-manifest.json"),
        )
        outputDir.mkdirs()
    }

    doLast {
        val version = sqliteNowVersion.get()
        val artifactZip = archiveFile.get().asFile
        val outputDir = swiftPmCompilerArtifactsDir.get().asFile
        val artifactManifest = outputDir.resolve("SQLiteNowCompiler-$version.artifact-manifest.json")

        val checksum = sha256(artifactZip)
        val baseUrl = swiftPmCompilerArtifactBaseUrl.get().trim().trimEnd('/')
        val artifactUrl = "$baseUrl/${artifactZip.name}"
        artifactManifest.writeText(
            """
            {
              "manifestVersion": 1,
              "sqliteNowVersion": ${jsonString(version)},
              "artifactName": ${jsonString(artifactZip.name)},
              "artifactPath": ${jsonString(artifactZip.relativeTo(project.rootDir).invariantSeparatorsPath)},
              "artifactUrl": ${jsonString(artifactUrl)},
              "checksum": ${jsonString(checksum)},
              "checksumAlgorithm": "swiftpm-sha256",
              "checksumCommand": ${jsonString("swift package compute-checksum ${artifactZip.relativeTo(project.rootDir).invariantSeparatorsPath}")},
              "artifactBundleName": "SQLiteNowCompiler.artifactbundle",
              "artifactType": "executable",
              "requiresJava": "17"
            }
            """.trimIndent() + "\n"
        )

        logger.lifecycle("Generated SQLiteNow compiler SwiftPM artifact bundle at ${artifactZip.absolutePath}")
        logger.lifecycle("SwiftPM checksum: $checksum")
    }
}

tasks.register("swiftPmPluginReleaseArtifacts") {
    group = "build"
    description = "Builds Swift release assets and validates the local SwiftPM plugin distribution path."
    dependsOn(
        "swiftRuntimeReleaseArtifactManifestDefaults",
        "packageSwiftPmCompilerArtifactBundle",
        "stageSwiftPmPluginDistribution",
        "inspectSwiftPmPluginDistribution",
        "swiftPmPluginCleanConsumerProof",
    )
}

tasks.register<Sync>("stageSwiftPmPluginDistribution") {
    group = "build"
    description = "Stages a text-only tag-ready SwiftPM package tree that references the compiler artifact bundle."

    dependsOn(
        "packageSwiftPmCompilerArtifactBundle",
        "swiftRuntimeReleaseArtifacts",
    )

    inputs.property("sqliteNowVersion", sqliteNowVersion)
    inputs.property("swiftPmCompilerArtifactBaseUrl", swiftPmCompilerArtifactBaseUrl)
    inputs.property("swiftRuntimeArtifactBaseUrl", swiftRuntimeArtifactBaseUrl)
    inputs.file(swiftPmPluginReleasePackageTemplate)
    inputs.dir(layout.projectDirectory.dir("swift/support/Sources"))
    inputs.file(
        swiftPmCompilerArtifactsDir.map {
            it.file("SQLiteNowCompiler-${sqliteNowVersion.get()}.artifactbundle.zip")
        },
    )
    inputs.file(
        swiftPmCompilerArtifactsDir.map {
            it.file("SQLiteNowCompiler-${sqliteNowVersion.get()}.artifact-manifest.json")
        },
    )
    inputs.file(sqliteNowVersion.map { version ->
        project.file("swift/runtime/core/build/runtime-release-artifacts/SQLiteNowCoreRuntime-$version.xcframework.zip")
    })
    inputs.file(sqliteNowVersion.map { version ->
        project.file("swift/runtime/core/build/runtime-release-artifacts/SQLiteNowCoreRuntime-$version.artifact-manifest.json")
    })
    inputs.file(sqliteNowVersion.map { version ->
        project.file("swift/runtime/sync/build/runtime-release-artifacts/SQLiteNowSyncRuntime-$version.xcframework.zip")
    })
    inputs.file(sqliteNowVersion.map { version ->
        project.file("swift/runtime/sync/build/runtime-release-artifacts/SQLiteNowSyncRuntime-$version.artifact-manifest.json")
    })

    into(swiftPmPluginDistributionDir)

    from(layout.projectDirectory.file("LICENSE"))
    from(layout.projectDirectory.file("README.md"))
    from(layout.projectDirectory.file("swift/plugin/sqlitenow-plugin/Package.swift")) {
        into("swift/plugin/sqlitenow-plugin")
    }
    from(layout.projectDirectory.dir("swift/plugin/sqlitenow-plugin/Sources")) {
        into("swift/plugin/sqlitenow-plugin/Sources")
        exclude("SQLiteNowGenerateCore/Resources/sqlitenow-compiler.jar")
    }
    from(layout.projectDirectory.dir("swift/plugin/sqlitenow-plugin/Plugins")) {
        into("swift/plugin/sqlitenow-plugin/Plugins")
    }
    from(layout.projectDirectory.dir("swift/plugin/sqlitenow-plugin/Tests")) {
        into("swift/plugin/sqlitenow-plugin/Tests")
    }
    from(layout.projectDirectory.dir("swift/support/Sources")) {
        into("swift/support/Sources")
    }

    doLast {
        val distributionDir = swiftPmPluginDistributionDir.get().asFile
        project.delete(distributionDir.resolve(".git"))
        val version = sqliteNowVersion.get()
        val artifactZip = swiftPmCompilerArtifactsDir.get().asFile
            .resolve("SQLiteNowCompiler-$version.artifactbundle.zip")
        if (!artifactZip.isFile) {
            throw GradleException("Expected SQLiteNow compiler artifact bundle at ${artifactZip.absolutePath}.")
        }
        val artifactManifest = swiftPmCompilerArtifactsDir.get().asFile
            .resolve("SQLiteNowCompiler-$version.artifact-manifest.json")
        if (!artifactManifest.isFile) {
            throw GradleException("Expected SQLiteNow compiler artifact manifest at ${artifactManifest.absolutePath}.")
        }
        @Suppress("UNCHECKED_CAST")
        val manifest = JsonSlurper().parse(artifactManifest) as Map<String, Any?>
        val artifactName = manifest["artifactName"] as? String
            ?: throw GradleException("Expected artifactName in ${artifactManifest.absolutePath}.")
        if (artifactName != artifactZip.name) {
            throw GradleException(
                "Compiler artifact manifest artifactName '$artifactName' does not match ${artifactZip.name}."
            )
        }
        val artifactChecksum = manifest["checksum"] as? String
            ?: throw GradleException("Expected checksum in ${artifactManifest.absolutePath}.")
        val baseUrl = swiftPmCompilerArtifactBaseUrl.get().trim().trimEnd('/')
        val artifactUrl = "$baseUrl/$artifactName"
        val runtimeBaseUrl = swiftRuntimeArtifactBaseUrl.get().trim().trimEnd('/')
        if (runtimeBaseUrl.isEmpty()) {
            throw GradleException("sqlitenow.swiftRuntimeArtifactBaseUrl must not be empty.")
        }
        fun releaseRuntimeArtifactMetadata(
            runtimeMode: String,
            runtimeModuleName: String,
            artifactZip: File,
            artifactManifest: File,
        ): Map<String, Any> {
            if (!artifactZip.isFile) {
                throw GradleException("Expected SQLiteNow $runtimeMode runtime artifact at ${artifactZip.absolutePath}.")
            }
            if (!artifactManifest.isFile) {
                throw GradleException("Expected SQLiteNow $runtimeMode runtime artifact manifest at ${artifactManifest.absolutePath}.")
            }
            @Suppress("UNCHECKED_CAST")
            val runtimeManifest = JsonSlurper().parse(artifactManifest) as Map<String, Any?>
            val manifestRuntimeMode = runtimeManifest["runtimeMode"] as? String
                ?: throw GradleException("Expected runtimeMode in ${artifactManifest.absolutePath}.")
            if (manifestRuntimeMode != runtimeMode) {
                throw GradleException("Expected runtimeMode '$runtimeMode' in ${artifactManifest.absolutePath}, got '$manifestRuntimeMode'.")
            }
            val manifestRuntimeModuleName = runtimeManifest["runtimeModuleName"] as? String
                ?: throw GradleException("Expected runtimeModuleName in ${artifactManifest.absolutePath}.")
            if (manifestRuntimeModuleName != runtimeModuleName) {
                throw GradleException("Expected runtimeModuleName '$runtimeModuleName' in ${artifactManifest.absolutePath}, got '$manifestRuntimeModuleName'.")
            }
            val manifestArtifactName = runtimeManifest["artifactName"] as? String
                ?: throw GradleException("Expected artifactName in ${artifactManifest.absolutePath}.")
            if (manifestArtifactName != artifactZip.name) {
                throw GradleException(
                    "Runtime artifact manifest artifactName '$manifestArtifactName' does not match ${artifactZip.name}."
                )
            }
            val manifestVersion = runtimeManifest["sqliteNowVersion"] as? String
                ?: throw GradleException("Expected sqliteNowVersion in ${artifactManifest.absolutePath}.")
            if (manifestVersion != version) {
                throw GradleException("Expected sqliteNowVersion '$version' in ${artifactManifest.absolutePath}, got '$manifestVersion'.")
            }
            val manifestChecksum = runtimeManifest["checksum"] as? String
                ?: throw GradleException("Expected checksum in ${artifactManifest.absolutePath}.")
            val actualChecksum = sha256(artifactZip)
            if (!manifestChecksum.equals(actualChecksum, ignoreCase = true)) {
                throw GradleException(
                    "Runtime artifact checksum mismatch for ${artifactZip.absolutePath}: expected $manifestChecksum, actual $actualChecksum."
                )
            }
            val artifactReleaseUrl = "$runtimeBaseUrl/${artifactZip.name}"
            return linkedMapOf(
                "kind" to "remoteZip",
                "url" to artifactReleaseUrl,
                "checksum" to manifestChecksum,
                "sqliteNowVersion" to manifestVersion,
                "runtimeModuleName" to runtimeModuleName,
            )
        }
        val coreRuntimeZip = project.file("swift/runtime/core/build/runtime-release-artifacts/SQLiteNowCoreRuntime-$version.xcframework.zip")
        val coreRuntimeManifest = project.file("swift/runtime/core/build/runtime-release-artifacts/SQLiteNowCoreRuntime-$version.artifact-manifest.json")
        val syncRuntimeZip = project.file("swift/runtime/sync/build/runtime-release-artifacts/SQLiteNowSyncRuntime-$version.xcframework.zip")
        val syncRuntimeManifest = project.file("swift/runtime/sync/build/runtime-release-artifacts/SQLiteNowSyncRuntime-$version.artifact-manifest.json")
        val coreRuntimeArtifact = releaseRuntimeArtifactMetadata(
            runtimeMode = "core",
            runtimeModuleName = "SQLiteNowCoreRuntime",
            artifactZip = coreRuntimeZip,
            artifactManifest = coreRuntimeManifest,
        )
        val syncRuntimeArtifact = releaseRuntimeArtifactMetadata(
            runtimeMode = "sync",
            runtimeModuleName = "SQLiteNowSyncRuntime",
            artifactZip = syncRuntimeZip,
            artifactManifest = syncRuntimeManifest,
        )
        val releaseDistributionJson = JsonOutput.prettyPrint(
            JsonOutput.toJson(
                linkedMapOf(
                    "manifestVersion" to 1,
                    "sqliteNowVersion" to version,
                    "compilerBinaryTargetName" to "SQLiteNowCompiler",
                    "compilerArtifactUrl" to artifactUrl,
                    "compilerArtifactChecksum" to artifactChecksum,
                    "runtimeArtifacts" to linkedMapOf(
                        "core" to coreRuntimeArtifact,
                        "sync" to syncRuntimeArtifact,
                    ),
                    "sqliteNowPackage" to linkedMapOf(
                        "kind" to "remoteExact",
                        "packageIdentity" to "sqlitenow-kmp",
                        "url" to "https://github.com/mobiletoly/sqlitenow-kmp.git",
                        "version" to version,
                        "coreRuntimeProduct" to "SQLiteNowCoreRuntime",
                        "syncRuntimeProduct" to "SQLiteNowSyncRuntime",
                        "coreSupportProduct" to "SQLiteNowCoreSupport",
                        "syncSupportProduct" to "SQLiteNowSyncSupport",
                    ),
                )
            )
        ) + "\n"
        writeSwiftPmPackageManifest(
            templateFile = swiftPmPluginReleasePackageTemplate.asFile,
            packageSwift = distributionDir.resolve("Package.swift"),
            compilerBinaryTarget = swiftPmReleaseCompilerBinaryTarget(
                compilerArtifactUrl = artifactUrl,
                compilerArtifactChecksum = artifactChecksum,
            ),
            coreRuntimeBinaryTarget = swiftPmReleaseRuntimeBinaryTarget(
                runtimeModuleName = "SQLiteNowCoreRuntime",
                runtimeArtifactUrl = coreRuntimeArtifact["url"] as String,
                runtimeArtifactChecksum = coreRuntimeArtifact["checksum"] as String,
            ),
            syncRuntimeBinaryTarget = swiftPmReleaseRuntimeBinaryTarget(
                runtimeModuleName = "SQLiteNowSyncRuntime",
                runtimeArtifactUrl = syncRuntimeArtifact["url"] as String,
                runtimeArtifactChecksum = syncRuntimeArtifact["checksum"] as String,
            ),
        )
        distributionDir.resolve(swiftPmPluginReleaseMarkerPath).also { marker ->
            marker.parentFile.mkdirs()
            marker.writeText(releaseDistributionJson)
        }
        distributionDir
            .resolve("swift/plugin/sqlitenow-plugin/Sources/SQLiteNowGenerateCore/Resources/release-distribution.json")
            .also { resource ->
                resource.parentFile.mkdirs()
                resource.writeText(releaseDistributionJson)
            }
        assertInspectableSwiftPmDistribution(distributionDir)
    }
}

tasks.register("swiftPmPluginDistributionPreflight") {
    group = "verification"
    description = "Runs the local SwiftPM plugin release-distribution preflight used by PR CI."
    dependsOn(
        "swiftPmPluginReleaseArtifacts",
        "validateSwiftPmPluginDistributionTag",
    )
}

tasks.register("inspectSwiftPmPluginDistribution") {
    group = "verification"
    description = "Inspects the staged text-only SwiftPM plugin distribution tree."

    dependsOn("stageSwiftPmPluginDistribution")

    inputs.dir(swiftPmPluginDistributionDir)
    outputs.upToDateWhen { false }

    doLast {
        val swift = findExecutableOnPath("swift")
            ?: error("swift is required to inspect the SwiftPM plugin distribution.")
        val distributionDir = swiftPmPluginDistributionDir.get().asFile
        assertInspectableSwiftPmDistribution(distributionDir)
        project.runSwiftPmReleaseCommand(
            distributionDir,
            listOf(swift.absolutePath, "package", "describe", "--package-path", distributionDir.absolutePath),
        )
        logger.lifecycle("SwiftPM plugin distribution staged at ${distributionDir.absolutePath}")
    }
}

tasks.register("validateSwiftPmPluginDistributionTag") {
    group = "verification"
    description = "Creates and validates a local Git tag for the staged SwiftPM plugin distribution."

    dependsOn("stageSwiftPmPluginDistribution")

    inputs.dir(swiftPmPluginDistributionDir)
    inputs.property("sqliteNowVersion", sqliteNowVersion)
    inputs.property("swiftPmPluginDistributionTagName", swiftPmPluginDistributionTagName)
    outputs.upToDateWhen { false }

    doLast {
        val git = findExecutableOnPath("git")
            ?: error("git is required to validate the SwiftPM plugin distribution tag.")
        val swift = findExecutableOnPath("swift")
            ?: error("swift is required to validate the SwiftPM plugin distribution tag.")
        val distributionSource = swiftPmPluginDistributionDir.get().asFile
        val distributionDir = swiftPmPluginDistributionGitDir.get().asFile
        val version = sqliteNowVersion.get()
        val tagName = swiftPmPluginDistributionTagName.get()
        assertInspectableSwiftPmDistribution(distributionSource)
        project.copySwiftPmDistributionToGitWorkDir(
            distributionSource = distributionSource,
            gitWorkDir = distributionDir,
        )
        assertTextOnlySwiftPmDistribution(distributionDir)
        project.createSwiftPmDistributionGitCommit(
            distributionDir = distributionDir,
            git = git,
            version = version,
            identity = swiftPmGitHubActionsIdentity,
            resetGitDirectory = false,
        )
        project.createSwiftPmDistributionGitTag(
            distributionDir = distributionDir,
            git = git,
            version = version,
            tagName = tagName,
        )
        project.runSwiftPmReleaseCommand(
            distributionDir,
            listOf(swift.absolutePath, "package", "describe", "--package-path", distributionDir.absolutePath),
        )
        logger.lifecycle("Validated local SwiftPM distribution tag $tagName.")
    }
}

tasks.register("publishSwiftPmPluginDistributionTag") {
    group = "publishing"
    description = "Publishes the staged SwiftPM plugin distribution as an idempotent Git tag."

    dependsOn("stageSwiftPmPluginDistribution")

    inputs.dir(swiftPmPluginDistributionDir)
    inputs.property("sqliteNowVersion", sqliteNowVersion)
    inputs.property("swiftPmPluginDistributionTagName", swiftPmPluginDistributionTagName)
    inputs.property("githubRepository", githubRepository.orElse(""))
    outputs.upToDateWhen { false }

    doLast {
        val git = findExecutableOnPath("git")
            ?: error("git is required to publish the SwiftPM plugin distribution tag.")
        val repository = githubRepository.orNull?.trim().orEmpty()
        val token = githubToken.orNull?.trim().orEmpty()
        if (repository.isEmpty()) {
            throw GradleException("GITHUB_REPOSITORY is required to publish the SwiftPM plugin distribution tag.")
        }
        if (token.isEmpty()) {
            throw GradleException("GITHUB_TOKEN is required to publish the SwiftPM plugin distribution tag.")
        }
        val distributionDir = swiftPmPluginDistributionDir.get().asFile
        val distributionGitDir = swiftPmPluginDistributionGitDir.get().asFile
        val version = sqliteNowVersion.get()
        val tagName = swiftPmPluginDistributionTagName.get()
        assertInspectableSwiftPmDistribution(distributionDir)
        project.copySwiftPmDistributionToGitWorkDir(
            distributionSource = distributionDir,
            gitWorkDir = distributionGitDir,
        )
        assertTextOnlySwiftPmDistribution(distributionGitDir)
        project.publishSwiftPmDistributionGitTag(
            distributionDir = distributionGitDir,
            git = git,
            version = version,
            tagName = tagName,
            repository = repository,
            token = token,
        )
    }
}

data class SwiftCleanConsumerProofRequest(
    val configuredProofRoot: String,
    val defaultProofRoot: File,
    val rootPropertyName: String,
    val configuredDistributionUrl: String,
    val proofMode: SwiftCleanConsumerProofMode,
    val requireProofRootOutsideRepo: Boolean,
    val includeXcodeBuild: Boolean,
)

fun registerSwiftCleanConsumerProofTask(
    taskName: String,
    description: String,
    defaultProofRoot: Provider<File>,
    rootPropertyName: String,
    configuredProofRoot: Provider<String>,
    configuredDistributionUrl: Provider<String>,
    proofMode: SwiftCleanConsumerProofMode,
    requireProofRootOutsideRepo: Boolean,
    includeXcodeBuild: Boolean,
) = tasks.register(taskName) {
    group = "verification"
    this.description = description

    dependsOn(
        "stageSwiftPmPluginDistribution",
        ":swift:runtime:core:packageReleaseRuntimeArtifact",
        ":swift:runtime:sync:packageReleaseRuntimeArtifact",
    )

    inputs.dir(swiftPmPluginDistributionDir)
    inputs.dir(swiftPmCleanConsumerTemplateDir)
    inputs.property("sqliteNowVersion", sqliteNowVersion)
    inputs.property("swiftPmCompilerArtifactBaseUrl", swiftPmCompilerArtifactBaseUrl)
    inputs.property("swiftRuntimeArtifactBaseUrl", swiftRuntimeArtifactBaseUrl)
    inputs.property(rootPropertyName, configuredProofRoot.orElse(""))
    inputs.property("swiftPmCleanConsumerDistributionUrl", configuredDistributionUrl.orElse(""))
    inputs.property("swiftCleanConsumerProofMode", proofMode.name)
    outputs.upToDateWhen { false }
    outputs.dir(
        providers.provider {
            val configuredRoot = configuredProofRoot.orNull?.trim().orEmpty()
            if (configuredRoot.isEmpty()) {
                defaultProofRoot.get()
            } else {
                File(configuredRoot)
            }
        },
    )

    doLast {
        runSwiftCleanConsumerProof(
            SwiftCleanConsumerProofRequest(
                configuredProofRoot = configuredProofRoot.orNull?.trim().orEmpty(),
                defaultProofRoot = defaultProofRoot.get(),
                rootPropertyName = rootPropertyName,
                configuredDistributionUrl = configuredDistributionUrl.orNull?.trim().orEmpty(),
                proofMode = proofMode,
                requireProofRootOutsideRepo = requireProofRootOutsideRepo,
                includeXcodeBuild = includeXcodeBuild,
            )
        )
    }
}

val runSwiftCleanConsumerProof = { request: SwiftCleanConsumerProofRequest ->
    val git = findExecutableOnPath("git") ?: error("git is required to validate the SwiftPM plugin distribution tag.")
    val swift = findExecutableOnPath("swift") ?: error("swift is required to validate the SwiftPM plugin distribution tag.")
    val xcodebuild = if (request.includeXcodeBuild) {
        findExecutableOnPath("xcodebuild") ?: error("xcodebuild is required to validate the Xcode consumer app.")
    } else {
        null
    }
    val version = sqliteNowVersion.get()
    val configuredProofRoot = request.configuredProofRoot
    val configuredDistributionUrl = request.configuredDistributionUrl
    val localArtifactProofRequested = request.proofMode == SwiftCleanConsumerProofMode.LOCAL_ARTIFACTS_LOCAL_GIT
    val remoteArtifactProofRequested = !localArtifactProofRequested
    val publicTagProofRequested = request.proofMode == SwiftCleanConsumerProofMode.REMOTE_ARTIFACTS_PUBLIC_GIT
    if (!publicTagProofRequested && configuredDistributionUrl.isNotEmpty()) {
        throw GradleException(
            "sqlitenow.swiftPmCleanConsumerDistributionUrl is only supported by " +
                "public tag clean-consumer proof tasks."
        )
    }
    if (publicTagProofRequested) {
        if (configuredDistributionUrl.isEmpty()) {
            throw GradleException(
                "Public tag clean-consumer proof tasks require " +
                    "-Psqlitenow.swiftPmCleanConsumerDistributionUrl=<https Git URL>."
            )
        }
        if (!configuredDistributionUrl.startsWith("https://")) {
            throw GradleException(
                "Public tag clean-consumer proof tasks require an https distribution URL: " +
                    configuredDistributionUrl
            )
        }
    }
    val compilerArtifactBaseUrl = swiftPmCompilerArtifactBaseUrl.get().trim().trimEnd('/')
    val runtimeBaseUrl = swiftRuntimeArtifactBaseUrl.get().trim().trimEnd('/')
    if (remoteArtifactProofRequested) {
        if (!compilerArtifactBaseUrl.startsWith("https://")) {
            throw GradleException(
                "Remote clean-consumer proof requires an https compiler artifact base URL: " +
                    compilerArtifactBaseUrl
            )
        }
        if (!runtimeBaseUrl.startsWith("https://")) {
            throw GradleException(
                "Remote clean-consumer proof requires an https runtime artifact base URL: " +
                    runtimeBaseUrl
            )
        }
    }
    val proofRoot = if (configuredProofRoot.isEmpty()) {
        request.defaultProofRoot
    } else {
        File(configuredProofRoot)
    }.absoluteFile.normalize()
    if (configuredProofRoot.isNotEmpty() && !File(configuredProofRoot).isAbsolute) {
        throw GradleException("${request.rootPropertyName} must be an absolute path: $configuredProofRoot")
    }
    if (request.requireProofRootOutsideRepo) {
        fun resolvedPath(file: File): java.nio.file.Path {
            var existingParent = file.absoluteFile
            val missingSegments = mutableListOf<String>()
            while (!existingParent.exists()) {
                missingSegments += existingParent.name
                existingParent = existingParent.parentFile ?: break
            }
            var resolved = existingParent.canonicalFile.toPath()
            for (segment in missingSegments.asReversed()) {
                resolved = resolved.resolve(segment)
            }
            return resolved.normalize()
        }

        val proofRootPath = resolvedPath(proofRoot)
        val repoRootPath = project.rootDir.canonicalFile.toPath().normalize()
        if (proofRootPath == repoRootPath || proofRootPath.startsWith(repoRootPath)) {
            throw GradleException(
                "${request.rootPropertyName} must resolve outside this repository: ${proofRoot.absolutePath}"
            )
        }
    }
    val enforceRepoRootLeakCheck = configuredProofRoot.isNotEmpty() || request.requireProofRootOutsideRepo
    val distributionSource = swiftPmPluginDistributionDir.get().asFile
    val distributionRepo = proofRoot.resolve("git/sqlitenow-kmp")
    val consumerDir = proofRoot.resolve("consumer")
    val templateDir = swiftPmCleanConsumerTemplateDir.asFile
    val generatedCorePackageDir = consumerDir.resolve("SQLiteNowGenerated/CleanConsumerCoreDatabaseSQLiteNow")
    val generatedSyncPackageDir = consumerDir.resolve("SQLiteNowGenerated/CleanConsumerSyncDatabaseSQLiteNow")
    val xcodeProject = consumerDir.resolve("XcodeConsumer/SQLiteNowCleanConsumerXcodeApp.xcodeproj")
    val coreRuntimeZip = project.file("swift/runtime/core/build/runtime-release-artifacts/SQLiteNowCoreRuntime-$version.xcframework.zip")
    val syncRuntimeZip = project.file("swift/runtime/sync/build/runtime-release-artifacts/SQLiteNowSyncRuntime-$version.xcframework.zip")
    val compilerArtifactZip = swiftPmCompilerArtifactsDir.get().asFile
        .resolve("SQLiteNowCompiler-$version.artifactbundle.zip")

    require(coreRuntimeZip.isFile) {
        "Expected SQLiteNow core runtime release artifact at ${coreRuntimeZip.absolutePath}."
    }
    require(syncRuntimeZip.isFile) {
        "Expected SQLiteNow sync runtime release artifact at ${syncRuntimeZip.absolutePath}."
    }
    require(compilerArtifactZip.isFile) {
        "Expected SQLiteNow compiler artifact bundle at ${compilerArtifactZip.absolutePath}."
    }
    if (configuredDistributionUrl.isEmpty()) {
        require(distributionSource.isDirectory) {
            "Expected staged SwiftPM distribution at ${distributionSource.absolutePath}."
        }
        assertInspectableSwiftPmDistribution(distributionSource)
    }
    require(templateDir.isDirectory) {
        "Expected clean consumer template at ${templateDir.absolutePath}."
    }

    fun assertNoCleanConsumerLeaks(stage: String) {
        val generatedPackageMetadataFiles = listOf(
            generatedCorePackageDir.resolve("Package.swift"),
            generatedCorePackageDir.resolve(".sqlitenow/package-manifest.json"),
            generatedSyncPackageDir.resolve("Package.swift"),
            generatedSyncPackageDir.resolve(".sqlitenow/package-manifest.json"),
        )
        val xcodeAuditFiles = if (request.includeXcodeBuild) {
            listOf(
                xcodeProject.resolve("project.pbxproj"),
                consumerDir.resolve("XcodeConsumer/SQLiteNowCleanConsumerXcodeApp/SQLiteNowCleanConsumerXcodeApp.swift"),
            )
        } else {
            emptyList()
        }
        val auditFiles = listOf(
            consumerDir.resolve("Package.swift"),
            consumerDir.resolve("SQLiteNow.json"),
        ) + generatedPackageMetadataFiles + xcodeAuditFiles
        val repoLeakValues = if (enforceRepoRootLeakCheck) {
            listOf(project.rootDir.absolutePath, project.rootDir.canonicalPath).distinct()
        } else {
            emptyList()
        }
        val forbiddenValues = (
            repoLeakValues + listOf(
                "SQLITENOW_COMPILER_JAR",
                "runtimeXcframeworkDirectory",
                coreRuntimeZip.absolutePath,
                syncRuntimeZip.absolutePath,
                coreRuntimeZip.canonicalPath,
                syncRuntimeZip.canonicalPath,
            )
        ).filter { it.isNotBlank() }.distinct()

        val leaks = findSwiftPmLeaks(
            files = auditFiles,
            rootDirectory = consumerDir,
            forbiddenLiteralValues = forbiddenValues,
        )
        if (leaks.isNotEmpty()) {
            throw GradleException(
                "Clean SwiftPM consumer proof leaked repo-local implementation details during $stage:\n" +
                    leaks.joinToString(separator = "\n") { leak ->
                        "${leak.relativePath} contains '${leak.value}'"
                    }
            )
        }
    }

    project.delete(
        proofRoot.resolve("git"),
        proofRoot.resolve("consumer"),
    )
    consumerDir.mkdirs()
    val distributionUrl = if (configuredDistributionUrl.isEmpty()) {
        distributionRepo.mkdirs()
        project.copy {
            from(distributionSource) {
                exclude(".git/**")
            }
            into(distributionRepo)
        }
        assertTextOnlySwiftPmDistribution(distributionRepo)
        if (localArtifactProofRequested) {
            val compilerArtifactRelativePath = "Artifacts/${compilerArtifactZip.name}"
            project.copy {
                from(compilerArtifactZip)
                from(coreRuntimeZip)
                from(syncRuntimeZip)
                into(distributionRepo.resolve("Artifacts"))
            }
            writeSwiftPmPackageManifest(
                templateFile = swiftPmPluginReleasePackageTemplate.asFile,
                packageSwift = distributionRepo.resolve("Package.swift"),
                compilerBinaryTarget = swiftPmLocalCompilerBinaryTarget(
                    compilerArtifactPath = compilerArtifactRelativePath,
                ),
                coreRuntimeBinaryTarget = swiftPmLocalRuntimeBinaryTarget(
                    runtimeModuleName = "SQLiteNowCoreRuntime",
                    runtimeArtifactPath = "Artifacts/${coreRuntimeZip.name}",
                ),
                syncRuntimeBinaryTarget = swiftPmLocalRuntimeBinaryTarget(
                    runtimeModuleName = "SQLiteNowSyncRuntime",
                    runtimeArtifactPath = "Artifacts/${syncRuntimeZip.name}",
                ),
            )
        }

        project.createSwiftPmDistributionGitCommit(
            distributionDir = distributionRepo,
            git = git,
            version = version,
            identity = swiftPmProofGitIdentity,
            resetGitDirectory = false,
        )
        project.createSwiftPmDistributionGitTag(
            distributionDir = distributionRepo,
            git = git,
            version = version,
            tagName = version,
        )

        fileUrl(distributionRepo)
    } else {
        configuredDistributionUrl
    }
    fun runtimeArtifactConfigJson(runtimeArtifactJson: String): String =
        ",\n      \"runtimeArtifact\": ${runtimeArtifactJson.replace("\n", "\n      ")}"

    val coreRuntimeArtifactConfigJson: String
    val syncRuntimeArtifactConfigJson: String
    if (localArtifactProofRequested) {
        val coreRuntimeChecksum = sha256(coreRuntimeZip)
        val syncRuntimeChecksum = sha256(syncRuntimeZip)
        val localRuntimeArtifactsDir = consumerDir.resolve("Artifacts")
        project.copy {
            from(coreRuntimeZip)
            from(syncRuntimeZip)
            into(localRuntimeArtifactsDir)
        }
        coreRuntimeArtifactConfigJson = runtimeArtifactConfigJson(
            localRuntimeArtifactJson(
                path = "Artifacts/${coreRuntimeZip.name}",
                checksum = coreRuntimeChecksum,
                version = version,
            )
        )
        syncRuntimeArtifactConfigJson = runtimeArtifactConfigJson(
            localRuntimeArtifactJson(
                path = "Artifacts/${syncRuntimeZip.name}",
                checksum = syncRuntimeChecksum,
                version = version,
            )
        )
    } else {
        coreRuntimeArtifactConfigJson = ""
        syncRuntimeArtifactConfigJson = ""
    }
    val templateValues = mapOf(
        "SQLITENOW_VERSION" to version,
        "SQLITENOW_DISTRIBUTION_URL" to distributionUrl,
        "SQLITENOW_CORE_RUNTIME_ARTIFACT_CONFIG" to coreRuntimeArtifactConfigJson,
        "SQLITENOW_SYNC_RUNTIME_ARTIFACT_CONFIG" to syncRuntimeArtifactConfigJson,
    )

    project.copy {
        from(templateDir) {
            exclude("Package.before-generation.swift.template")
            exclude("Package.after-generation.swift.template")
            exclude("SQLiteNow.json.template")
        }
        into(consumerDir)
    }
    renderTemplateFile(
        templateDir.resolve("Package.before-generation.swift.template"),
        consumerDir.resolve("Package.swift"),
        templateValues,
    )
    renderTemplateFile(
        templateDir.resolve("SQLiteNow.json.template"),
        consumerDir.resolve("SQLiteNow.json"),
        templateValues,
    )

    project.runSwiftPmReleaseCommand(
        consumerDir,
        listOf(
            swift.absolutePath,
            "package",
            "--package-path",
            consumerDir.absolutePath,
            "plugin",
            "--allow-writing-to-package-directory",
            "sqlitenow-generate",
        ),
    ) {
        environment.remove("SQLITENOW_COMPILER_JAR")
    }
    assertNoCleanConsumerLeaks("generation")

    project.runSwiftPmReleaseCommand(
        consumerDir,
        listOf(swift.absolutePath, "build", "--package-path", generatedCorePackageDir.absolutePath),
    )
    project.runSwiftPmReleaseCommand(
        consumerDir,
        listOf(swift.absolutePath, "build", "--package-path", generatedSyncPackageDir.absolutePath),
    )

    renderTemplateFile(
        templateDir.resolve("Package.after-generation.swift.template"),
        consumerDir.resolve("Package.swift"),
        templateValues,
    )
    assertNoCleanConsumerLeaks("consumer test setup")

    project.runSwiftPmReleaseCommand(
        consumerDir,
        listOf(swift.absolutePath, "build", "--package-path", consumerDir.absolutePath),
    )
    project.runSwiftPmReleaseCommand(
        consumerDir,
        listOf(swift.absolutePath, "test", "--package-path", consumerDir.absolutePath),
    )

    if (request.includeXcodeBuild) {
        require(xcodeProject.isDirectory) {
            "Expected Xcode consumer project at ${xcodeProject.absolutePath}."
        }
        project.runSwiftPmReleaseCommand(
            consumerDir,
            listOf(
                xcodebuild!!.absolutePath,
                "-project",
                xcodeProject.absolutePath,
                "-scheme",
                "SQLiteNowCleanConsumerXcodeApp",
                "-destination",
                "generic/platform=iOS Simulator",
                "-derivedDataPath",
                consumerDir.resolve(".build/xcode-derived-data").absolutePath,
                "build",
            ),
        )
        assertNoCleanConsumerLeaks("xcode build")
    }
}

registerSwiftCleanConsumerProofTask(
    taskName = "swiftPmPluginCleanConsumerProof",
    description = "Validates a clean SwiftPM consumer against local release artifacts.",
    defaultProofRoot = defaultSwiftPmPluginCleanConsumerDir.map { it.asFile },
    rootPropertyName = "sqlitenow.swiftPmCleanConsumerRoot",
    configuredProofRoot = swiftPmPluginCleanConsumerRoot,
    configuredDistributionUrl = emptySwiftPmCleanConsumerDistributionUrl,
    proofMode = SwiftCleanConsumerProofMode.LOCAL_ARTIFACTS_LOCAL_GIT,
    requireProofRootOutsideRepo = false,
    includeXcodeBuild = false,
)

registerSwiftCleanConsumerProofTask(
    taskName = "swiftPmPluginLocalTagCleanConsumerProof",
    description = "Validates a clean SwiftPM consumer against a local file-backed Git tag and remote artifacts.",
    defaultProofRoot = defaultSwiftPmPluginLocalTagCleanConsumerDir.map { it.asFile },
    rootPropertyName = "sqlitenow.swiftPmCleanConsumerRoot",
    configuredProofRoot = swiftPmPluginCleanConsumerRoot,
    configuredDistributionUrl = emptySwiftPmCleanConsumerDistributionUrl,
    proofMode = SwiftCleanConsumerProofMode.REMOTE_ARTIFACTS_LOCAL_GIT,
    requireProofRootOutsideRepo = false,
    includeXcodeBuild = false,
)

registerSwiftCleanConsumerProofTask(
    taskName = "swiftPmPluginPublicTagCleanConsumerProof",
    description = "Validates a clean SwiftPM consumer against a configured public HTTPS Git tag.",
    defaultProofRoot = defaultSwiftPmPluginPublicTagCleanConsumerDir.map { it.asFile },
    rootPropertyName = "sqlitenow.swiftPmCleanConsumerRoot",
    configuredProofRoot = swiftPmPluginCleanConsumerRoot,
    configuredDistributionUrl = swiftPmPluginCleanConsumerDistributionUrl,
    proofMode = SwiftCleanConsumerProofMode.REMOTE_ARTIFACTS_PUBLIC_GIT,
    requireProofRootOutsideRepo = false,
    includeXcodeBuild = false,
)

registerSwiftCleanConsumerProofTask(
    taskName = "swiftPmPluginPublicTagXcodeConsumerProof",
    description = "Validates a clean SwiftPM and Xcode consumer against a configured public HTTPS Git tag.",
    defaultProofRoot = defaultSwiftPmPluginPublicTagXcodeConsumerDir,
    rootPropertyName = "sqlitenow.swiftPmPublicTagXcodeConsumerRoot",
    configuredProofRoot = swiftPmPluginPublicTagXcodeConsumerRoot,
    configuredDistributionUrl = swiftPmPluginCleanConsumerDistributionUrl,
    proofMode = SwiftCleanConsumerProofMode.REMOTE_ARTIFACTS_PUBLIC_GIT,
    requireProofRootOutsideRepo = true,
    includeXcodeBuild = true,
)

registerSwiftCleanConsumerProofTask(
    taskName = "swiftExternalXcodeConsumerProof",
    description = "Validates an outside-repo SwiftPM and Xcode consumer against local release artifacts.",
    defaultProofRoot = defaultSwiftExternalXcodeConsumerDir,
    rootPropertyName = "sqlitenow.swiftExternalXcodeConsumerRoot",
    configuredProofRoot = swiftExternalXcodeConsumerRoot,
    configuredDistributionUrl = emptySwiftPmCleanConsumerDistributionUrl,
    proofMode = SwiftCleanConsumerProofMode.LOCAL_ARTIFACTS_LOCAL_GIT,
    requireProofRootOutsideRepo = true,
    includeXcodeBuild = true,
)
