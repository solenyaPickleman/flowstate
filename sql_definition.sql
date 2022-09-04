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
CREATE TABLE IF NOT EXISTS language ( 
  id integer references emails(id),
  language text
);
CREATE TABLE IF NOT EXISTS is_spam ( 
  id integer references emails(id),
  is_spam boolean
);
CREATE TABLE IF NOT EXISTS entities (
  id integer references emails(id),
  LOC text[] ,
  ORG text[] ,
  PER text[] 
);


create index idx_emails_from on emails("from");
create index idx_emaillang_id on email_language(id);
create index idx_isspam_id on email_isspam(id);

