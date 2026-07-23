import 'package:shared_preferences/shared_preferences.dart';

abstract interface class SampleSessionPreferences {
  Future<String?> readUsername();

  Future<String?> readMode();

  Future<void> writeUsername(String username);

  Future<void> writeMode(String mode);

  Future<void> clearUsername();
}

final class SharedPreferencesSampleSessionPreferences
    implements SampleSessionPreferences {
  SharedPreferencesSampleSessionPreferences({
    SharedPreferencesAsync? preferences,
  }) : _preferences = preferences ?? SharedPreferencesAsync();

  static const _usernameKey = 'samplesync.username';
  static const _modeKey = 'samplesync.mode';

  final SharedPreferencesAsync _preferences;

  @override
  Future<String?> readUsername() => _preferences.getString(_usernameKey);

  @override
  Future<String?> readMode() => _preferences.getString(_modeKey);

  @override
  Future<void> writeUsername(String username) {
    return _preferences.setString(_usernameKey, username);
  }

  @override
  Future<void> writeMode(String mode) {
    return _preferences.setString(_modeKey, mode);
  }

  @override
  Future<void> clearUsername() => _preferences.remove(_usernameKey);
}

final class MemorySampleSessionPreferences implements SampleSessionPreferences {
  MemorySampleSessionPreferences({String? username, String? mode})
    : _username = username,
      _mode = mode;

  String? _username;
  String? _mode;

  @override
  Future<String?> readUsername() async => _username;

  @override
  Future<String?> readMode() async => _mode;

  @override
  Future<void> writeUsername(String username) async {
    _username = username;
  }

  @override
  Future<void> writeMode(String mode) async {
    _mode = mode;
  }

  @override
  Future<void> clearUsername() async {
    _username = null;
  }
}
