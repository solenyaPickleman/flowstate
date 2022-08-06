CREATE TABLE IF NOT EXISTS emails ( 
  id serial PRIMARY KEY,
  "date" TIMESTAMP, 
  "from" TEXT NOT NULL,
  "to" TEXT[],
  "cc" TEXT[],
  bcc TEXT[],
  subject TEXT, 
  body TEXT,
  filepath TEXT,
  "mime-version"  TEXT ,
  "x-filename" TEXT
);
CREATE TABLE IF NOT EXISTS email_language ( 
  id integer references emails(id),
  language text
);
create index idx_emails_from on emails("from");
create index idx_emaillang_id on email_language(id);

