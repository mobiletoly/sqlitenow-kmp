CREATE TABLE value_registry (
    -- @@{ field=id, propertyType=kotlin.uuid.Uuid }
    id BLOB PRIMARY KEY NOT NULL DEFAULT (randomblob(16)),

    key TEXT NOT NULL UNIQUE,

    -- @@{ field=value_type, propertyType=com.pluralfusion.daytempo.domain.model.RegisteredValueType }
    value_type TEXT NOT NULL,

    -- @@{ field=updated_at, propertyType=kotlinx.datetime.LocalDateTime }
    updated_at INTEGER DEFAULT 0 NOT NULL,

    string_value TEXT,

    numeric_value REAL
) WITHOUT ROWID;

CREATE INDEX idx_valueRegistry_key ON value_registry(key);
CREATE INDEX idx_valueRegistry_valueType ON value_registry(value_type);
CREATE INDEX idx_valueRegistry_updatedAt ON value_registry(updated_at);
