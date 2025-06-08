CREATE TABLE Comment
(
    id         INTEGER PRIMARY KEY NOT NULL,
    person_id  INTEGER             NOT NULL,
    comment    TEXT                NOT NULL,
    created_at TEXT                NOT NULL DEFAULT current_timestamp,
    FOREIGN KEY (person_id) REFERENCES Person (id) ON DELETE CASCADE
);
