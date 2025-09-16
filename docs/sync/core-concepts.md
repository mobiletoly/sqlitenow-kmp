---
layout: doc
title: Core Concepts
permalink: /sync/core-concepts/
parent: Sync
---

# Core Concepts

Understanding the fundamental concepts of SQLiteNow's synchronization system is essential before implementing sync in your application. This guide explains the key concepts without diving into implementation details.

## What is Synchronization?

Synchronization is the process of keeping data consistent across multiple devices. When a user makes changes on one device, those changes are automatically propagated to all their other devices. SQLiteNow's sync system handles this complex process automatically while providing offline-first capabilities.

## The Sync Ecosystem

### Users
A **user** represents a person who owns multiple devices. In the sync system, each user has a unique identifier that remains constant across all their devices. Users can sign in on multiple devices and expect their data to be available everywhere.

**Key Characteristics:**
- Each user has a globally unique identifier
- The user ID never changes for a given person
- One user can have many devices
- Users can sign out and sign back in without losing sync capability

### Devices
A **device** represents a specific installation of your application. Each device has its own unique identifier that distinguishes it from other devices, even those belonging to the same user.

**Key Characteristics:**
- Each device has a globally unique identifier
- Device IDs persist across app launches and updates
- Device IDs are independent of the user (same device can be used by different users)
- Each app installation gets its own device ID

### The User-Device Relationship
- **One user, many devices**: A user can have your app installed on their phone, tablet, laptop, etc.
- **One device, multiple users**: A shared device (like a family tablet) can be used by different users
- **Device independence**: Each device tracks its own sync state independently

## Bootstrap: Preparing the Local Database

**Bootstrap** is the process of preparing the local database for sync operations. Unlike traditional "registration" processes, bootstrap is called every time your app launches and a user signs in.

### What Happens During Bootstrap?
- **Database Configuration**: Sets up SQLite pragmas (foreign keys, WAL mode, timeouts)
- **Metadata Tables**: Creates or ensures sync system tables exist (`_sync_client_info`, `_sync_row_meta`, `_sync_pending`)
- **Client Info Setup**: Creates or updates the client record for this user-device combination
- **Trigger Creation**: Creates or recreates SQLite triggers that track changes on sync-enabled tables
- **Apply Mode Reset**: Resets the database to normal operation mode (not applying remote changes)

### When Bootstrap Occurs
- **Every App Launch**: Bootstrap is called each time the user signs in
- **User Switching**: When switching between different user accounts on the same device
- **After Database Changes**: Ensures triggers and metadata are always current

### Why Bootstrap Every Time?
- **Idempotent Operation**: Bootstrap is safe to run multiple times - it only creates what's missing
- **Trigger Maintenance**: Ensures sync triggers are always present and up-to-date
- **Database Consistency**: Guarantees the database is properly configured for sync operations
- **User Switching**: Handles cases where different users sign in on the same device

### Bootstrap is Not...
- **Data Transfer**: Bootstrap doesn't move data between devices (that's hydration)
- **Server Communication**: Bootstrap only prepares the local database
- **Authentication**: Bootstrap assumes the user is already authenticated

## Hydration: Bulk Data Loading

**Hydration** is the process of downloading the complete dataset from the server to populate a device. This is typically used for new devices or recovery scenarios where you need to rebuild the local database.

### What Happens During Hydration?
1. **Snapshot Creation**: Server creates a consistent snapshot of all user data (windowed mode)
2. **Bulk Download**: Device downloads all data in paginated chunks
3. **Trigger Suppression**: Local change-tracking triggers are temporarily disabled
4. **Batch Insert**: Downloaded data is inserted directly into local tables
5. **Metadata Update**: Sync metadata is updated to reflect the current server state
6. **Trigger Restoration**: Change-tracking triggers are re-enabled

### Hydration Modes
- **Windowed Hydration** (Recommended): Uses a frozen snapshot for consistency across multiple pages
- **Non-Windowed Hydration**: Simpler paging without snapshot consistency (fine for small datasets)

### When to Use Hydration
- **New User Sign-In**: When a user signs in for the first time (not session restoration)
- **Recovery Scenarios**: After app reinstall or database corruption
- **Complete Refresh**: When you need to rebuild local state from server
- **Large Dataset Sync**: More efficient than incremental sync for bulk data

### When Hydration is NOT Used
- **Session Restoration**: When restoring a saved session, incremental sync is used instead
- **Regular App Launches**: Existing users get incremental sync, not full hydration
- **User Switching Back**: If a user has used the device before, incremental sync is sufficient

### Hydration vs Regular Sync
- **Hydration**: Bulk transfer of complete dataset, triggers disabled, uses snapshots
- **Regular Sync**: Incremental updates of only changed data, triggers enabled, real-time

## Change Tracking

SQLiteNow automatically tracks changes to your data when sync is enabled on a table.

### What Gets Tracked?
- **INSERT operations**: New records are marked for upload
- **UPDATE operations**: Modified records are marked for upload
- **DELETE operations**: Deleted records are marked for upload

### How Tracking Works
- **Automatic**: No manual intervention required once configured
- **Table-Level**: Only tables configured for sync are tracked
- **Transparent**: Your app code doesn't need to change
- **Efficient**: Only actual changes are tracked, not every database access

### Table Configuration Methods
- **SQLiteNow Annotations**: Tables marked with `enableSync=true` are automatically registered for sync
- **Manual Configuration**: Tables can be manually specified during oversqlite client configuration
- **Mixed Approach**: You can combine both annotation-based and manual table configuration

### Change Metadata
Each change includes:
- **What changed**: The actual data that was modified
- **When it changed**: Timestamp of the modification
- **Where it changed**: Which device made the change
- **Change type**: INSERT, UPDATE, or DELETE

## Sync Operations

### Upload
**Upload** is the process of sending local changes to the server.

- **Purpose**: Share your device's changes with other devices
- **Frequency**: Can be triggered manually or automatically
- **Batching**: Multiple changes are sent together for efficiency
- **Conflict Detection**: Server checks for conflicts with other devices' changes

### Download
**Download** is the process of receiving changes from other devices via the server.

- **Purpose**: Get changes made on other devices
- **Frequency**: Can be triggered manually or automatically
- **Filtering**: Only receives changes newer than what the device already has
- **Conflict Resolution**: Applies conflict resolution rules when needed

### Bidirectional Sync
Most sync operations involve both upload and download:
1. Upload local changes first
2. Download remote changes second
3. Handle any conflicts that arise

## Conflict Resolution

**Conflicts** occur when the same data is modified on multiple devices before they sync.

### When Conflicts Happen
- User edits a note on their phone
- User edits the same note on their laptop
- Both devices try to sync before seeing each other's changes

### Resolution Strategies
- **Server Wins**: Server's version is always kept
- **Client Wins**: Local device's version is always kept
- **Last Writer Wins**: Most recent change wins (based on timestamp)
- **Custom Logic**: Application-specific resolution rules

### Conflict Prevention
- **Frequent Sync**: Sync often to minimize conflict windows
- **User Awareness**: Show sync status to users
- **Optimistic UI**: Show changes immediately, handle conflicts in background

## Offline-First Architecture

SQLiteNow's sync system is designed to work offline-first.

### Core Principles
- **Local Database is Primary**: Your app always works with local data
- **Sync is Secondary**: Network operations happen in the background
- **Graceful Degradation**: App works fully even without network
- **Eventual Consistency**: Data becomes consistent across devices over time

### Benefits
- **Always Responsive**: No waiting for network requests
- **Works Anywhere**: Functions without internet connection
- **Better UX**: Users never see loading spinners for basic operations
- **Resilient**: Handles network interruptions gracefully

This architecture ensures your app provides a smooth user experience regardless of network conditions while maintaining data consistency across all devices.

## Security and Privacy

### Authentication
Sync operations require proper authentication to ensure data security:
- **JWT Tokens**: Industry-standard tokens for secure API access
- **Token Refresh**: Automatic renewal of expired tokens
- **User Authorization**: Only authenticated users can access their data

### Data Privacy
- **User Isolation**: Each user's data is completely separate
- **Device Privacy**: Device IDs don't contain personal information
- **Encryption**: Data is encrypted in transit between devices and server

### Access Control
- **User-Scoped Data**: Users can only sync their own data
- **Device Registration**: Only registered devices can participate in sync
- **Revocation**: Devices can be removed from sync if compromised

## Sync Lifecycle

Understanding the complete lifecycle helps you implement sync correctly:

### 1. App Installation
- Generate unique device ID
- Store device ID persistently
- App is ready for sync when user signs in

### 2. New User Sign-In (First Time)
- Authenticate user with your backend
- **Bootstrap device** (prepares database for sync)
- **Hydrate device** with user's complete dataset
- Begin regular sync operations

### 3. Session Restoration (Returning User)
- Authenticate user with your backend (restore saved session)
- **Bootstrap device** (prepares database for sync)
- **Incremental sync** to catch up on changes (no hydration)
- Resume regular sync operations

### 4. Regular Usage
- User makes changes to data
- Changes are automatically tracked by triggers (created during bootstrap)
- Periodic sync uploads changes and downloads updates
- Conflicts are resolved automatically

### 5. User Sign-Out
- Stop sync operations
- Optionally clear local data
- Device ID remains for future use

### 6. User Sign-In (Different User)
- Authenticate new user
- **Bootstrap device** for new user (same device, different user)
- **Hydrate with new user's data** (different user = different dataset)
- Resume sync operations

## Performance Considerations

### Sync Frequency
- **Too Frequent**: Wastes battery and bandwidth
- **Too Infrequent**: Users see stale data, more conflicts
- **Adaptive**: Sync more often when app is active, less when idle

### Data Volume
- **Large Datasets**: Use windowed hydration and pagination
- **Small Changes**: Incremental sync is very efficient
- **Bulk Operations**: Consider batching for better performance

### Network Conditions
- **Poor Connectivity**: Sync operations will retry automatically
- **Offline Mode**: App continues working, sync resumes when online
- **Background Sync**: Sync can continue when app is backgrounded

## Next Steps

Now that you understand the core concepts, you're ready to:

1. **[Getting Started]({{ site.baseurl }}/sync/getting-started/)** - Implement sync in your application
2. **[Bootstrap & Hydration]({{ site.baseurl }}/sync/bootstrap-hydration/)** - Deep dive into device setup
3. **[Sync Operations]({{ site.baseurl }}/sync/sync-operations/)** - Learn about upload/download mechanics
4. **[Authentication]({{ site.baseurl }}/sync/authentication/)** - Set up secure authentication

The concepts covered here form the foundation for all sync operations. Understanding them will help you implement sync correctly and troubleshoot issues when they arise.
