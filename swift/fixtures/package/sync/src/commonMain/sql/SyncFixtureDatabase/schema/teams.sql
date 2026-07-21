-- @@{ enableSync=true }
CREATE TABLE teams (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    captain_member_id TEXT REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
);
