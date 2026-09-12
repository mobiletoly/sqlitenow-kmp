ALTER TABLE drafts ADD COLUMN normalized TEXT;
CREATE INDEX drafts_normalized ON drafts(normalized);
