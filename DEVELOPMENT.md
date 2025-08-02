# SQLiteNow Development Guide

## Quick Development Workflow

### First Time Setup:

```bash
# Download and setup SQLite 3.42.0 amalgamation (run once)
native-src/session/setup-sqlite-session.sh
```

### When you modify .c files:

```bash
# Quick rebuild of just the native session extension
./rebuild-native.sh

# Then publish the library with updated native code
./gradlew publishToMavenLocal
```

### When you modify Kotlin code or want a full rebuild:

```bash
# Complete rebuild and publish
./dev-rebuild.sh
```

### For releases:

```bash
# Release to Maven Central
./release.sh 1.0.0
```

## File Structure

```
native-src/
├── session/                         # Session extension C source
│   ├── session_extension.c          # Main implementation
│   └── session_extension.h          # Header file
├── android/
│   └── build-session-extension.sh   # Android build script
└── ios/
    └── build-session-extension.sh   # iOS build script

library/
├── src/androidMain/jniLibs/         # Android native libraries (auto-generated)
└── build.gradle.kts                 # Library build configuration
```

## Build Outputs

### Android
- **Location**: `library/src/androidMain/jniLibs/`
- **Files**: `libnowsession_ext.so` for each architecture
- **Usage**: Automatically included in published library

### iOS
- **Location**: `native-src/ios/build/SessionExtension.xcframework`
- **Usage**: Users drag-and-drop into their iOS project
- **Distribution**: Upload to GitHub releases or host separately

## Available Gradle Tasks

- `./gradlew buildSessionExtension` - Build for all platforms (Android + iOS)
- `./gradlew buildSessionExtensionAndroid` - Build only for Android
- `./gradlew buildSessionExtensionIOS` - Build only for iOS

## Development Tips

1. **Modify C code**: Use `./rebuild-native.sh` for quick iteration
2. **Test changes**: Use the sample app in `sample-kmp/`
3. **Publish locally**: `./gradlew publishToMavenLocal`
4. **Release**: Use `./release.sh VERSION` for production releases

## User Instructions (for library consumers)

### Android
```kotlin
// Just add the dependency - session extension is automatically included
implementation("dev.goquick.sqlitenow:core:VERSION")
```

### iOS
1. Add the dependency:
```kotlin
implementation("dev.goquick.sqlitenow:core:VERSION")
```

2. Drag and drop `SessionExtension.xcframework` into your iOS project

That's it! The session extension works automatically on both platforms.
