#!/bin/bash
# SQLite Session Extension Setup Script
# Downloads SQLite 3.42.0 source and builds session extension using configure/make

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADS_DIR="${SCRIPT_DIR}/downloads"
SQLITE_VERSION="3420000"
SQLITE_SRC_ZIP="sqlite-src-${SQLITE_VERSION}.zip"
SQLITE_SRC_URL="https://distfiles.macports.org/sqlite3/${SQLITE_SRC_ZIP}"

echo "🔧 SQLite Session Extension Setup"
echo "=================================="
echo "SQLite Version: 3.42.0 (${SQLITE_VERSION})"
echo "Source URL: ${SQLITE_SRC_URL}"
echo ""

# Create downloads directory
mkdir -p "${DOWNLOADS_DIR}"
cd "${DOWNLOADS_DIR}"

# Download SQLite source if not already present
if [ ! -f "${SQLITE_SRC_ZIP}" ]; then
    echo "📥 Downloading SQLite ${SQLITE_VERSION} source..."
    curl -o "${SQLITE_SRC_ZIP}" "${SQLITE_SRC_URL}"
    echo "✅ Downloaded ${SQLITE_SRC_ZIP}"
else
    echo "ℹ️  ${SQLITE_SRC_ZIP} already exists, skipping download"
fi

# Extract if not already extracted
SQLITE_SRC_DIR="sqlite-src-${SQLITE_VERSION}"
if [ ! -d "${SQLITE_SRC_DIR}" ]; then
    echo "📦 Extracting ${SQLITE_SRC_ZIP}..."
    unzip -q "${SQLITE_SRC_ZIP}"
    echo "✅ Extracted to ${SQLITE_SRC_DIR}/"
else
    echo "ℹ️  ${SQLITE_SRC_DIR}/ already exists, skipping extraction"
fi

# Configure and prepare SQLite source for session extension build
cd "${SQLITE_SRC_DIR}"

echo "📋 Configuring SQLite with session extension..."

# Configure SQLite with session extension enabled
if [ ! -f "Makefile" ]; then
    echo "🔧 Running ./configure --enable-session..."
    ./configure --enable-session --enable-static --disable-shared
    echo "✅ Configuration complete"
else
    echo "ℹ️  Already configured, skipping ./configure"
fi

# Build SQLite (this generates parse.h and other required files)
echo "🔨 Building SQLite to generate required files..."
make sqlite3.c
echo "✅ SQLite build complete with session extension enabled"

cd ..

echo ""
echo "🎯 Setup complete!"
echo ""
echo "📁 SQLite source configured and built with session extension enabled"
echo "   - Source location: ${DOWNLOADS_DIR}/sqlite-src-${SQLITE_VERSION}/"
echo "   - Session extension: ext/session/sqlite3session.c"
echo "   - Headers: src/, parse.h, sqlite3ext.h"
echo "   - Extension entry point: nowsession_extension.c"
echo ""
echo "🎯 Build scripts will use -I flags to reference headers directly from source"
echo "   (no need to copy files, cleaner approach)"
echo ""
echo "🔨 To build the session extension:"
echo "   ./rebuild-native.sh"
echo ""
echo "📝 Downloaded files are in: ${DOWNLOADS_DIR}"
echo "   (These files are git-ignored and can be safely deleted)"
