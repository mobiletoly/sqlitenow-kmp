CREATE TABLE users (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  updated_at TEXT
);

CREATE INDEX idx_users_name ON users(name);

CREATE VIEW active_users AS
SELECT id, name
FROM users
WHERE name <> '';
