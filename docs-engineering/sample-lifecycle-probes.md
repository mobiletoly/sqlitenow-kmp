# Sample lifecycle probes

These probes validate a fresh, run-owned database across an intentional process stop and reopen.
They do not upgrade or delete an existing user database. Supply the device identifier, authority
URL, token, and run role from the qualification run; never store credentials in the repository.

## Android

Build and install the sample, replacing `<device-id>` and the package/activity if the selected
variant reports a different application ID:

```sh
./gradlew :samplesync-kmp:androidApp:assembleDebug --no-daemon
adb -s <device-id> install -r samplesync-kmp/androidApp/build/outputs/apk/debug/androidApp-debug.apk
adb -s <device-id> shell am start -n dev.goquick.sqlitenow.samplesynckmp/.MainActivity \
  --ez lifecycle_probe true --es lifecycle_probe_role owner --es lifecycle_probe_token <run-token>
adb -s <device-id> logcat -d -s SQLiteNowLifecycle:I | grep SQLITENOW_ANDROID_DURABLE_CUT
adb -s <device-id> shell am force-stop dev.goquick.sqlitenow.samplesynckmp
adb -s <device-id> shell am start -n dev.goquick.sqlitenow.samplesynckmp/.MainActivity \
  --ez lifecycle_probe true --es lifecycle_probe_role reopener --es lifecycle_probe_token <run-token>
adb -s <device-id> logcat -d -s SQLiteNowLifecycle:I | grep SQLITENOW_ANDROID_DURABLE_CUT
```

Run the app once without extras as a separate ordinary-startup check.
Use a fresh emulator or a temporary ephemeral Android user when the installed application ID has
pre-format-2 sample data. Do not clear or uninstall an existing user's sample storage for this
check.

## iOS

Build the sample for the caller-selected simulator or device, then launch it with the final
environment names:

```sh
./gradlew swiftHybridSupportGate --no-daemon
xcrun simctl boot <device-id> 2>/dev/null || true
xcrun simctl bootstatus <device-id> -b
ios_probe_derived=$(mktemp -d /tmp/sqlitenow-ios-lifecycle.XXXXXX)
xcodebuild -project swift/samples/sync/iosApp/iosApp.xcodeproj -scheme iosApp \
  -configuration Debug -sdk iphonesimulator -destination 'id=<device-id>' \
  -derivedDataPath "$ios_probe_derived" CODE_SIGNING_ALLOWED=NO build
xcrun simctl install <device-id> \
  "$ios_probe_derived/Build/Products/Debug-iphonesimulator/SQLiteNow Swift Sync.app"
SIMCTL_CHILD_SQLITENOW_LIFECYCLE=1 SIMCTL_CHILD_SQLITENOW_LIFECYCLE_ROLE=owner \
  SIMCTL_CHILD_SQLITENOW_LIFECYCLE_TOKEN=<run-token> \
  SIMCTL_CHILD_SQLITENOW_LIFECYCLE_BASE_URL=<authority-url> \
  xcrun simctl launch --terminate-running-process --console-pty \
    <device-id> <bundle-id> >/tmp/sqlitenow-ios-lifecycle-owner.log 2>&1 &
owner_console_pid=$!
for attempt in {1..30}; do
  grep -q SQLITENOW_IOS_DURABLE_CUT /tmp/sqlitenow-ios-lifecycle-owner.log && break
  sleep 1
done
grep SQLITENOW_IOS_DURABLE_CUT /tmp/sqlitenow-ios-lifecycle-owner.log
xcrun simctl terminate <device-id> <bundle-id>
wait "$owner_console_pid"
SIMCTL_CHILD_SQLITENOW_LIFECYCLE=1 SIMCTL_CHILD_SQLITENOW_LIFECYCLE_ROLE=reopener \
  SIMCTL_CHILD_SQLITENOW_LIFECYCLE_TOKEN=<run-token> \
  SIMCTL_CHILD_SQLITENOW_LIFECYCLE_BASE_URL=<authority-url> \
  xcrun simctl launch --console-pty <device-id> <bundle-id> \
    >/tmp/sqlitenow-ios-lifecycle-reopener.log 2>&1 &
reopener_console_pid=$!
for attempt in {1..30}; do
  grep -q SQLITENOW_IOS_DURABLE_CUT /tmp/sqlitenow-ios-lifecycle-reopener.log && break
  sleep 1
done
grep SQLITENOW_IOS_DURABLE_CUT /tmp/sqlitenow-ios-lifecycle-reopener.log
xcrun simctl terminate <device-id> <bundle-id>
wait "$reopener_console_pid"
```

Run the app once without lifecycle variables as the ordinary-startup check. The owner role resets
only the probe's run-owned storage; the reopener role must preserve it.
