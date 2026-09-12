-- expected_digest: 6:ba589ffd9f18f8e535189c2940362dc9157f9fd82f4c1fe4ff2f5a1bff33263c
CREATE TABLE real_witness(id INTEGER PRIMARY KEY, value REAL NOT NULL);
INSERT INTO real_witness(id,value) VALUES(1,0.0);
INSERT INTO real_witness(id,value) VALUES(2,-0.0);
INSERT INTO real_witness(id,value) VALUES(3,0.1);
INSERT INTO real_witness(id,value) VALUES(4,-123.5);
INSERT INTO real_witness(id,value) VALUES(5,4.9406564584124654e-324);
INSERT INTO real_witness(id,value) VALUES(6,1.7976931348623157e308);
