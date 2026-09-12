ALTER TABLE drafts ADD COLUMN normalized TEXT;
UPDATE drafts SET normalized = upper(label);
CREATE INDEX drafts_normalized ON drafts(normalized);
