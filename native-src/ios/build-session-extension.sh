#!/bin/bash

# Build SQLite Session Extension for iOS
# This script builds the session extension for iOS device and simulator architectures
# and packages them into an XCFramework

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION_SRC_DIR="${SCRIPT_DIR}/../session"
BUILD_DIR="${SCRIPT_DIR}/build"
OUTPUT_DIR="${SCRIPT_DIR}/../../library/src/iosMain/resources"
SQLITE_SRC_DIR="${SESSION_SRC_DIR}/downloads/sqlite-src-3420000"
SQLITE_AMALGAMATION="${SQLITE_SRC_DIR}/sqlite3.c"

# Check if SQLite amalgamation is available
if [ ! -f "${SQLITE_AMALGAMATION}" ]; then
    echo "❌ SQLite amalgamation not found: ${SQLITE_AMALGAMATION}"
    echo "   Run: native-src/session/setup-sqlite-session.sh"
    exit 1
fi

# Clean previous builds
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
mkdir -p "${OUTPUT_DIR}"

# iOS Device (arm64)
echo "Building for iOS Device (arm64)..."
DEVICE_BUILD_DIR="${BUILD_DIR}/device"
mkdir -p "${DEVICE_BUILD_DIR}"

# Use the same successful approach as Android: full SQLite amalgamation + session extension
# REMOVED -DSQLITE_OMIT_LOAD_EXTENSION to allow sqlite3_create_function to work!
clang -arch arm64 \
    -isysroot $(xcrun --sdk iphoneos --show-sdk-path) \
    -mios-version-min=12.0 \
    -dynamiclib -fPIC -Os -DNDEBUG \
    -DSQLITE_ENABLE_SESSION=1 \
    -DSQLITE_ENABLE_PREUPDATE_HOOK=1 \
    -DHAVE_USLEEP=1 \
    -DSQLITE_DEFAULT_MEMSTATUS=0 \
    -DSQLITE_THREADSAFE=2 \
    -DSQLITE_OMIT_PROGRESS_CALLBACK \
    -DSQLITE_OMIT_FTS3 \
    -DSQLITE_OMIT_FTS4 \
    -DSQLITE_OMIT_FTS5 \
    -DSQLITE_OMIT_JSON \
    -DSQLITE_OMIT_RBU \
    -DSQLITE_OMIT_RTREE \
    -DSQLITE_OMIT_STAT4 \
    -DSQLITE_OMIT_COMPLETE \
    -DSQLITE_OMIT_TCL_VARIABLE \
    -DSQLITE_OMIT_TRACE \
    -DSQLITE_OMIT_EXPLAIN \
    -DSQLITE_OMIT_DEPRECATED \
    -DSQLITE_OMIT_BUILTIN_TEST \
    -I"${SQLITE_SRC_DIR}" \
    -I"${SQLITE_SRC_DIR}/src" \
    -I"${SQLITE_SRC_DIR}/ext/session" \
    -install_name @rpath/libnowsession_ext.dylib \
    "${SESSION_SRC_DIR}/nowsession_extension.c" \
    "${SQLITE_AMALGAMATION}" \
    -o "${DEVICE_BUILD_DIR}/libnowsession_ext.dylib"

# iOS Simulator (x86_64 and arm64)
echo "Building for iOS Simulator (x86_64 and arm64)..."
SIMULATOR_BUILD_DIR="${BUILD_DIR}/simulator"
mkdir -p "${SIMULATOR_BUILD_DIR}"

# Build universal simulator binary using the same successful approach as Android
# REMOVED -DSQLITE_OMIT_LOAD_EXTENSION to allow sqlite3_create_function to work!
clang -arch x86_64 -arch arm64 \
    -isysroot $(xcrun --sdk iphonesimulator --show-sdk-path) \
    -mios-simulator-version-min=12.0 \
    -dynamiclib -fPIC -Os -DNDEBUG \
    -DSQLITE_ENABLE_SESSION=1 \
    -DSQLITE_ENABLE_PREUPDATE_HOOK=1 \
    -DHAVE_USLEEP=1 \
    -DSQLITE_DEFAULT_MEMSTATUS=0 \
    -DSQLITE_THREADSAFE=2 \
    -DSQLITE_OMIT_PROGRESS_CALLBACK \
    -DSQLITE_OMIT_FTS3 \
    -DSQLITE_OMIT_FTS4 \
    -DSQLITE_OMIT_FTS5 \
    -DSQLITE_OMIT_JSON \
    -DSQLITE_OMIT_RBU \
    -DSQLITE_OMIT_RTREE \
    -DSQLITE_OMIT_STAT4 \
    -DSQLITE_OMIT_COMPLETE \
    -DSQLITE_OMIT_TCL_VARIABLE \
    -DSQLITE_OMIT_TRACE \
    -DSQLITE_OMIT_EXPLAIN \
    -DSQLITE_OMIT_DEPRECATED \
    -DSQLITE_OMIT_BUILTIN_TEST \
    -I"${SQLITE_SRC_DIR}" \
    -I"${SQLITE_SRC_DIR}/src" \
    -I"${SQLITE_SRC_DIR}/ext/session" \
    -install_name @rpath/libnowsession_ext.dylib \
    "${SESSION_SRC_DIR}/nowsession_extension.c" \
    "${SQLITE_AMALGAMATION}" \
    -o "${SIMULATOR_BUILD_DIR}/libnowsession_ext.dylib"

# Create XCFramework
echo "Creating XCFramework..."
XCFRAMEWORK_DIR="${BUILD_DIR}/SessionExtension.xcframework"

xcodebuild -create-xcframework \
    -library "${DEVICE_BUILD_DIR}/libnowsession_ext.dylib" \
    -library "${SIMULATOR_BUILD_DIR}/libnowsession_ext.dylib" \
    -output "${XCFRAMEWORK_DIR}"

# Copy XCFramework to resources
cp -R "${XCFRAMEWORK_DIR}" "${OUTPUT_DIR}/"

# Create a framework structure for embedding in the iOS app
FRAMEWORK_DIR="${BUILD_DIR}/SessionExtension.framework"
mkdir -p "${FRAMEWORK_DIR}"

# Create Info.plist for the framework
cat > "${FRAMEWORK_DIR}/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>SessionExtension</string>
    <key>CFBundleIdentifier</key>
    <string>dev.goquick.sqlitenow.SessionExtension</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>SessionExtension</string>
    <key>CFBundlePackageType</key>
    <string>FMWK</string>
    <key>CFBundleShortVersionString</key>
    <string>1.0</string>
    <key>CFBundleVersion</key>
    <string>1</string>
</dict>
</plist>
EOF

# Copy the universal simulator library as the framework binary
cp "${SIMULATOR_BUILD_DIR}/libnowsession_ext.dylib" "${FRAMEWORK_DIR}/SessionExtension"

# Copy framework to iOS app directory
IOS_APP_DIR="${SCRIPT_DIR}/../../sample-kmp/iosApp/iosApp"
mkdir -p "${IOS_APP_DIR}/Frameworks"
cp -R "${FRAMEWORK_DIR}" "${IOS_APP_DIR}/Frameworks/"

echo "iOS Session Extension build completed successfully!"
echo "XCFramework location: ${OUTPUT_DIR}/SessionExtension.xcframework"
echo "Framework copied to: ${IOS_APP_DIR}/Frameworks/SessionExtension.framework"
