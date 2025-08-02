#!/bin/bash
# Quick rebuild script for native session extension only
# Use this when you only modify .c files and don't need to rebuild the entire library

set -e

echo "🔄 Rebuilding Native Session Extension"
echo "======================================"

# Clean native builds only
echo "🧹 Cleaning native builds..."
rm -rf native-src/android/build
rm -rf native-src/ios/build

# Build session extension for Android
echo "🤖 Building Android session extension..."
bash native-src/android/build-session-extension.sh

# Build session extension for iOS (creates XCFramework)
echo "📱 Building iOS session extension..."
bash native-src/ios/build-session-extension.sh

echo ""
echo "✅ Native rebuild complete!"
echo ""
echo "📋 Results:"
echo "   Android: native-src/android/build/ (automatically included in library)"
echo "   iOS XCFramework: native-src/ios/build/SessionExtension.xcframework"
echo ""
echo "💡 To publish the library with updated native code, run:"
echo "   ./gradlew publishToMavenLocal"
