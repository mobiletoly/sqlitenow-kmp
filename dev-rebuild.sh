#!/bin/bash
# Development rebuild script for SQLiteNow
# Use this when you modify .c files and want to rebuild everything

set -e

echo "🔄 SQLiteNow Development Rebuild"
echo "================================"

# Clean previous builds
echo "🧹 Cleaning previous builds..."
./gradlew clean
rm -rf native-src/android/build
rm -rf native-src/ios/build

# Build session extension for all platforms
echo "🔨 Building session extension for all platforms..."
./gradlew buildSessionExtension

# Build and publish the library
echo "📦 Building and publishing library..."
./gradlew :library:publishToMavenLocal :sqlitenow-gradle-plugin:publishToMavenLocal

echo ""
echo "✅ Development rebuild complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Android: Session extension is automatically included"
echo "   2. iOS: Use the XCFramework at: native-src/ios/build/SessionExtension.xcframework"
echo "   3. Drag and drop the XCFramework into your iOS project"
echo ""
echo "🎯 Published to Maven Local"
echo ""
