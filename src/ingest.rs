use anyhow::Result;
use git2::{ObjectType, Oid, Repository, Tree};
use lz4::EncoderBuilder;
use rusqlite::{Connection, Row, Statement, Transaction};
use std::io::Write;

pub struct Ingestor<'tx, 'repo> {
    repo: &'repo Repository,
    tree_entry_stmt: Statement<'tx>,
    commit_stmt: Statement<'tx>,
    blob_exists_stmt: Statement<'tx>,
    blob_insert_stmt: Statement<'tx>,
    buf: Vec<u8>,
}

impl<'tx, 'repo> Ingestor<'tx, 'repo> {
    pub fn new(repo: &'repo Repository, tx: &'tx Transaction<'tx>) -> Result<Self> {
        let tree_entry_stmt = tx.prepare(
            "INSERT OR IGNORE INTO tree_entries (
                tree_oid, 
                name, 
                kind, 
                oid
            ) VALUES (?, ?, ?, ?)
            RETURNING *",
        )?;
        let commit_stmt = tx.prepare(
            "INSERT OR IGNORE INTO commits (
                oid, 
                tree_oid, 
                message, 
                parents,
                author_name,
                author_email,
                author_date,
                committer_name,
                committer_email,
                committer_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        let blob_exists_stmt = tx.prepare("SELECT EXISTS (SELECT * FROM blobs WHERE oid = ?);")?;
        let blob_insert_stmt = tx.prepare(
            "INSERT OR IGNORE INTO blobs (oid, content_lz4) 
            VALUES (?, ?)",
        )?;

        Ok(Self {
            repo,
            tree_entry_stmt,
            commit_stmt,
            blob_exists_stmt,
            blob_insert_stmt,
            buf: Vec::new(),
        })
    }

    pub fn add_commit(&mut self, commit_oid: Oid) -> Result<()> {
        println!("adding commit {}", commit_oid);
        let commit = self.repo.find_commit(commit_oid)?;

        self.add_tree(&commit.tree()?)?;
        let mut parents: Vec<u8> = Vec::new();
        for parent in commit.parent_ids() {
            parents.extend(parent.as_bytes())
        }

        self.commit_stmt.execute((
            commit.id().as_bytes(),
            commit.tree_id().as_bytes(),
            commit.message_bytes(),
            parents,
            commit.author().name_bytes(),
            commit.author().email_bytes(),
            commit.author().when().seconds(),
            commit.committer().name_bytes(),
            commit.committer().email_bytes(),
            commit.committer().when().seconds(),
        ))?;

        Ok(())
    }

    fn add_tree(&mut self, tree: &Tree) -> Result<()> {
        for tree_obj in tree.into_iter() {
            let new_entries = self.tree_entry_stmt.query_map(
                (
                    tree.id().as_bytes(),
                    tree_obj.name_bytes(),
                    object_type_to_int(tree_obj.kind()),
                    tree_obj.id().as_bytes(),
                ),
                |row| TreeEntry::try_from(row),
            )?;

            let mut new: Option<TreeEntry> = None;
            for new_entry in new_entries {
                new = Some(new_entry?);
            }

            if let Some(new_entry) = new {
                if new_entry.kind == Some(ObjectType::Tree) {
                    self.add_tree(&self.repo.find_tree(new_entry.oid)?)?;
                } else if new_entry.kind == Some(ObjectType::Blob) {
                    if !self.blob_exists(new_entry.oid)? {
                        self.insert_blob_content(new_entry.oid)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn blob_exists(&mut self, oid: Oid) -> Result<bool> {
        Ok(self
            .blob_exists_stmt
            .query_row((oid.as_bytes(),), |row| row.get::<_, bool>(0))?)
    }

    fn insert_blob_content(&mut self, oid: Oid) -> Result<()> {
        let mut dst = std::mem::take(&mut self.buf);
        dst.clear();
        let mut enc = EncoderBuilder::new()
            // TODO: tune this. 16 seemed to not improve compression much.
            .level(10)
            .favor_dec_speed(true)
            .build(dst)?;
        let blob = self.repo.find_blob(oid)?;
        enc.write_all(blob.content())?;
        let (dst, r) = enc.finish();
        r?;
        self.blob_insert_stmt.execute((oid.as_bytes(), &dst))?;
        self.buf = dst;
        Ok(())
    }
}

fn object_type_to_int(kind: Option<ObjectType>) -> u8 {
    match kind {
        None => 0,
        Some(ObjectType::Any) => 1,
        Some(ObjectType::Commit) => 2,
        Some(ObjectType::Tree) => 3,
        Some(ObjectType::Blob) => 4,
        Some(ObjectType::Tag) => 5,
    }
}

fn object_type_from_int(val: u8) -> Option<ObjectType> {
    match val {
        0 => None,
        1 => Some(ObjectType::Any),
        2 => Some(ObjectType::Commit),
        3 => Some(ObjectType::Tree),
        4 => Some(ObjectType::Blob),
        5 => Some(ObjectType::Tag),
        _ => panic!("unknown"),
    }
}

#[derive(Debug)]
struct TreeEntry {
    tree_oid: Oid,
    name: Vec<u8>,
    kind: Option<ObjectType>,
    oid: Oid,
}

impl<'a> TryFrom<&Row<'a>> for TreeEntry {
    type Error = rusqlite::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let tree_oid: Oid = row
            .get::<_, [u8; 20]>(0)
            .map(|arr| Oid::from_bytes(arr.as_slice()).unwrap())?;
        let name: Vec<u8> = row.get(1)?;
        let kind = object_type_from_int(row.get(2)?);
        let oid: Oid = row
            .get::<_, [u8; 20]>(3)
            .map(|arr| Oid::from_bytes(arr.as_slice()).unwrap())?;
        Ok(TreeEntry {
            tree_oid,
            name,
            kind,
            oid,
        })
    }
}
