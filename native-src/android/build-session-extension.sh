#!/bin/bash

# Build Simple SQLite Session Extension for Android using Android NDK
# This script builds a minimal session extension for Android ABIs using NDK

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION_SRC_DIR="${SCRIPT_DIR}/../session"
BUILD_DIR="${SCRIPT_DIR}/build"
OUTPUT_DIR="${SCRIPT_DIR}/../../library/src/androidMain/jniLibs"

# Clean previous builds
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
mkdir -p "${OUTPUT_DIR}"

# Find Android NDK
if [ -z "$ANDROID_NDK_ROOT" ]; then
    if [ -z "$ANDROID_HOME" ]; then
        echo "Error: ANDROID_HOME not set. Please set ANDROID_HOME environment variable."
        exit 1
    fi
    ANDROID_NDK_ROOT="$ANDROID_HOME/ndk-bundle"
    if [ ! -d "$ANDROID_NDK_ROOT" ]; then
        # Try to find NDK in the newer location
        NDK_VERSIONS=($(ls -1 "$ANDROID_HOME/ndk/" 2>/dev/null | sort -V -r))
        if [ ${#NDK_VERSIONS[@]} -gt 0 ]; then
            ANDROID_NDK_ROOT="$ANDROID_HOME/ndk/${NDK_VERSIONS[0]}"
        else
            echo "Error: Android NDK not found. Please install Android NDK."
            exit 1
        fi
    fi
fi

echo "Using Android NDK: $ANDROID_NDK_ROOT"
echo "Building Simple SQLite Session Extension for Android..."

# Android NDK toolchain setup
API_LEVEL=21
TOOLCHAIN="$ANDROID_NDK_ROOT/toolchains/llvm/prebuilt/darwin-x86_64"

# Build for different architectures using Android NDK
ABIS=("arm64-v8a" "armeabi-v7a" "x86_64" "x86")
TARGETS=("aarch64-linux-android" "armv7a-linux-androideabi" "x86_64-linux-android" "i686-linux-android")

for i in "${!ABIS[@]}"; do
    ABI="${ABIS[$i]}"
    TARGET="${TARGETS[$i]}"

    echo "Building for ${ABI} (${TARGET})..."

    ABI_BUILD_DIR="${BUILD_DIR}/${ABI}"
    ABI_OUTPUT_DIR="${OUTPUT_DIR}/${ABI}"

    mkdir -p "${ABI_BUILD_DIR}"
    mkdir -p "${ABI_OUTPUT_DIR}"

    # Set up NDK compiler
    CC="${TOOLCHAIN}/bin/${TARGET}${API_LEVEL}-clang"

    if [ ! -f "$CC" ]; then
        echo "Error: Compiler not found: $CC"
        exit 1
    fi

    # Check if SQLite source is available
    SQLITE_SRC_DIR="${SESSION_SRC_DIR}/downloads/sqlite-src-3420000"
    SQLITE_AMALGAMATION="${SQLITE_SRC_DIR}/sqlite3.c"
    if [ ! -f "${SQLITE_AMALGAMATION}" ]; then
        echo "❌ SQLite amalgamation not found: ${SQLITE_AMALGAMATION}"
        echo "   Run: native-src/session/setup-sqlite-session.sh"
        exit 1
    fi

    # Compile session extension using full SQLite amalgamation (working approach)
    # This includes the complete SQLite engine with session extension support
    "$CC" -fPIC -shared -Os -DNDEBUG \
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
        "${SESSION_SRC_DIR}/nowsession_extension.c" \
        "${SQLITE_AMALGAMATION}" \
        -llog \
        -o "${ABI_OUTPUT_DIR}/libnowsession_ext.so"

    echo "Built ${ABI_OUTPUT_DIR}/libnowsession_ext.so"

    # Verify the library format
    file "${ABI_OUTPUT_DIR}/libnowsession_ext.so"
done
echo "Android Session Extension build completed successfully!"
echo "Libraries placed in: ${OUTPUT_DIR}"
