-- expected_digest: 7:0c5971eebc3c44655ed2b5c7384387058a6ed7866cf0ed8d41f1b7c2ade3564e
CREATE TABLE keyless_values(label TEXT,payload BLOB,numeric_value);
CREATE INDEX keyless_values_label_idx ON keyless_values(label);
CREATE TRIGGER keyless_values_touch AFTER INSERT ON keyless_values BEGIN SELECT 1; END;
INSERT INTO keyless_values(label,payload,numeric_value) VALUES(NULL,X'',NULL);
INSERT INTO keyless_values(label,payload,numeric_value) VALUES('left'||char(0)||'right',X'000102FF',1);
INSERT INTO keyless_values(label,payload,numeric_value) VALUES('left'||char(0)||'right',X'000102FF',1);
INSERT INTO keyless_values(label,payload,numeric_value) VALUES(replace(hex(zeroblob(65536)),'00','x'),zeroblob(65536),1.5);
INSERT INTO keyless_values(label,payload,numeric_value) VALUES(replace(hex(zeroblob(65537)),'00','y'),zeroblob(65537),-7);
CREATE TABLE without_rowid_values(part_a TEXT,part_b INTEGER,payload BLOB,PRIMARY KEY(part_a,part_b)) WITHOUT ROWID;
INSERT INTO without_rowid_values VALUES('alpha',1,X'00FF');
INSERT INTO without_rowid_values VALUES('beta',2,NULL);
