cmake_minimum_required(VERSION 3.31)
project(sqlite_ext_dev C)

set(CMAKE_C_STANDARD 11)

add_executable(sqlite_ext_dev main.c session/nowsession_extension.c
        session/downloads/sqlite-src-3420000/sqlite3.c)

# Modern CMake approach for include directories
target_include_directories(sqlite_ext_dev PRIVATE
        session/downloads/sqlite-src-3420000
        session/downloads/sqlite-src-3420000/src
        session/downloads/sqlite-src-3420000/ext/session
)

# Enable session extension using target-specific compile definitions
target_compile_definitions(sqlite_ext_dev PRIVATE
    SQLITE_ENABLE_SESSION=1
    SQLITE_ENABLE_PREUPDATE_HOOK=1
    SQLITE_ENABLE_LOAD_EXTENSION=1
)

# Build nowsession extension as a shared library
# Use the same approach as Android/iOS: compile extension with full SQLite amalgamation
add_library(nowsession_ext SHARED session/nowsession_extension.c
        session/downloads/sqlite-src-3420000/sqlite3.c)

# Configure shared library
target_include_directories(nowsession_ext PRIVATE
        session/downloads/sqlite-src-3420000
        session/downloads/sqlite-src-3420000/src
        session/downloads/sqlite-src-3420000/ext/session
)

target_compile_definitions(nowsession_ext PRIVATE
    SQLITE_ENABLE_SESSION=1
    SQLITE_ENABLE_PREUPDATE_HOOK=1
    SQLITE_ENABLE_LOAD_EXTENSION=1
)

# Test executable for nowsession extension (dynamically loads the extension)
add_executable(test_nowsession test_nowsession.c
        session/downloads/sqlite-src-3420000/sqlite3.c)

# Configure test executable
target_include_directories(test_nowsession PRIVATE
        session/downloads/sqlite-src-3420000
        session/downloads/sqlite-src-3420000/src
        session/downloads/sqlite-src-3420000/ext/session
)

target_compile_definitions(test_nowsession PRIVATE
    SQLITE_ENABLE_SESSION=1
    SQLITE_ENABLE_PREUPDATE_HOOK=1
    SQLITE_ENABLE_LOAD_EXTENSION=1
)

# Set working directory for CLion run configurations
set_target_properties(test_nowsession PROPERTIES
    VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    XCODE_SCHEME_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

add_dependencies(test_nowsession nowsession_ext)
