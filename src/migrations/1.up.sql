CREATE TABLE repos (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
); 

CREATE TABLE blobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    oid BLOB NOT NULL,
    content_lz4 BLOB NOT NULL
);

CREATE UNIQUE INDEX blob_oid_idx ON blobs (oid);

CREATE TABLE commits (
    oid BLOB PRIMARY KEY NOT NULL,
    tree_oid BLOB NOT NULL,
    message TEXT NOT NULL,
    parents BLOB NOT NULL,
    author_name TEXT NOT NULL,
    author_email TEXT NOT NULL,
    author_date DATETIME NOT NULL,
    committer_name TEXT NOT NULL,
    committer_email TEXT NOT NULL,
    committer_date DATETIME NOT NULL
);

CREATE TABLE tree_entries (
    tree_oid BLOB NOT NULL,
    name TEXT NOT NULL,
    kind TINYINT NOT NULL,
    oid BLOB NOT NULL,
    PRIMARY KEY (tree_oid, name)
);

CREATE TABLE tags (
    oid BLOB NOT NULL,
    name TEXT NOT NULL,
    message TEXT NOT NULL,
    tagger_name TEXT NOT NULL,
    tagger_email TEXT NOT NULL,
    tagger_date DATETIME NOT NULL,
    target_oid BLOB NOT NULL
);

CREATE TABLE direct_refs (
    repo_id INTEGER NOT NULL,
    name STRING NOT NULL,
    target_oid BLOB NOT NULL,
    PRIMARY KEY (repo_id, name)
);

CREATE INDEX IF NOT EXISTS direct_refs_target_oid_idx ON direct_refs (target_oid);

CREATE TABLE symbolic_refs (
    repo_id INTEGER NOT NULL,
    name STRING NOT NULL,
    target_ref BLOB NOT NULL,
    PRIMARY KEY (repo_id, name)
);

CREATE INDEX IF NOT EXISTS symbolic_refs_target_oid_idx ON symbolic_refs (target_ref);
