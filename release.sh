#!/bin/bash
# Release script for SQLiteNow
# Builds everything and publishes to Maven Central

set -e

echo "🚀 SQLiteNow Release Process"
echo "============================"

# Check if version is provided
if [ -z "$1" ]; then
    echo "❌ Please provide a version number"
    echo "Usage: ./release.sh 1.0.0"
    exit 1
fi

VERSION=$1
echo "📦 Releasing version: $VERSION"

# Update version in gradle.properties
echo "📝 Updating version in gradle.properties..."
sed -i '' "s/version=.*/version=$VERSION/" gradle.properties

# Clean and build everything
echo "🧹 Cleaning previous builds..."
./gradlew clean

echo "🔨 Building session extensions..."
./gradlew buildSessionExtension

echo "📦 Building and publishing library..."
./gradlew publishToMavenCentral

echo ""
echo "✅ Release $VERSION completed!"
echo ""
echo "📋 Don't forget to:"
echo "   1. Commit and tag the version: git tag v$VERSION"
echo "   2. Upload SessionExtension.xcframework to GitHub releases"
echo "   3. Update documentation with new version"
echo ""
echo "📍 XCFramework location: native-src/ios/build/SessionExtension.xcframework"
echo ""
