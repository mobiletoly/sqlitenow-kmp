CREATE TABLE parity_value
(
    id             INTEGER PRIMARY KEY NOT NULL,
    text_value     TEXT                NOT NULL,
    nullable_value TEXT,
    real_value     REAL                NOT NULL,
    blob_value     BLOB                NOT NULL,
    exact_value    INTEGER             NOT NULL
);
