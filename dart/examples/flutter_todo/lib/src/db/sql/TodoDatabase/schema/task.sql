CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL,
  -- @@{ field=priority, adapter=custom, propertyType=String }
  priority TEXT NOT NULL,
  completed INTEGER NOT NULL
);
