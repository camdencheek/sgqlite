use std::path::PathBuf;

use anyhow::{Context, Error, Result};
use clap::{Parser, Subcommand};
use git2::{ObjectType, Oid, ReferenceType, Repository, Sort, Tree};
use lz4::EncoderBuilder;
use rusqlite::{
    types::{FromSql, FromSqlResult, ToSql, ValueRef},
    Connection, Row,
};
use rusqlite_migration::{Migrations, M};
use std::io::Write;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Ingest(IngestArgs),
}

#[derive(Parser, Debug)]
pub struct IngestArgs {
    #[clap(long = "db")]
    db: PathBuf,
    #[clap(long = "repo-id")]
    repo_id: u32,
    #[clap(long = "repo-name")]
    repo_name: String,
    #[clap(long = "repo-path")]
    repo_path: PathBuf,
}

fn main() -> Result<()> {
    let args = Cli::try_parse()?;
    let args = match args.cmd {
        Command::Ingest(a) => a,
    };

    let mut conn = Connection::open(args.db)?;
    conn.pragma_update_and_check(None, "journal_mode", "WAL", |_| Ok(()))?;

    let migrations = Migrations::new(vec![
        M::up(include_str!("migrations/1.up.sql")).down(include_str!("migrations/1.down.sql"))
    ]);

    migrations.to_latest(&mut conn).context("migrating")?;

    let repo = Repository::open(args.repo_path)?;

    let changed_refs = compare_refs(&mut conn, args.repo_id, &repo, "refs/heads/*")?;
    println!("{:?}", changed_refs);

    let mut walker = repo.revwalk()?;
    walker.set_sorting(Sort::TOPOLOGICAL | Sort::REVERSE)?;
    for diff in changed_refs {
        if let Some(new) = diff.new_target {
            walker.push(new)?;
        }
        if let Some(old) = diff.old_target {
            walker.hide(old)?;
        }
    }

    let tx = conn.transaction()?;
    {
        let mut tree_entry_stmt = tx.prepare(
            "INSERT OR IGNORE INTO tree_entries (
                tree_oid, 
                name, 
                kind, 
                oid
            ) VALUES (?, ?, ?, ?)
            RETURNING *",
        )?;
        let mut commit_stmt = tx.prepare(
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
        let mut blob_exists_stmt =
            tx.prepare("SELECT EXISTS (SELECT * FROM blobs WHERE oid = ?);")?;
        let mut blob_insert_stmt = tx.prepare(
            "INSERT OR IGNORE INTO blobs (oid, content_lz4) 
            VALUES (?, ?)",
        )?;
        for (i, commit_oid) in walker.enumerate() {
            let commit_oid = commit_oid?;
            println!("{}, {}", i, commit_oid);
            let commit = repo.find_commit(commit_oid)?;

            insert_tree(
                &mut tree_entry_stmt,
                &mut blob_exists_stmt,
                &mut blob_insert_stmt,
                &repo,
                &commit.tree()?,
            )?;

            let mut parents: Vec<u8> = Vec::new();
            for parent in commit.parent_ids() {
                parents.extend(parent.as_bytes())
            }

            commit_stmt.execute((
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
        }
    }

    tx.commit()?;
    Ok(())
}

fn insert_tree(
    tree_entry_stmt: &mut rusqlite::Statement,
    blob_exists_stmt: &mut rusqlite::Statement,
    blob_insert_stmt: &mut rusqlite::Statement,
    repo: &Repository,
    tree: &Tree,
) -> Result<()> {
    for tree_obj in tree.into_iter() {
        let new_entries = tree_entry_stmt.query_map(
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
                insert_tree(
                    tree_entry_stmt,
                    blob_exists_stmt,
                    blob_insert_stmt,
                    repo,
                    &repo.find_tree(new_entry.oid)?,
                )?;
            } else if new_entry.kind == Some(ObjectType::Blob) {
                let exists = blob_exists_stmt
                    .query_row((new_entry.oid.as_bytes(),), |row| row.get::<_, bool>(0))?;
                if !exists {
                    let dst = Vec::new();
                    let mut enc = EncoderBuilder::new()
                        // TODO: tune this. 16 seemed to not improve compression much.
                        .level(10)
                        .favor_dec_speed(true)
                        .build(dst)?;
                    let blob = repo.find_blob(new_entry.oid)?;
                    enc.write_all(blob.content())?;
                    let (dst, r) = enc.finish();
                    r?;
                    blob_insert_stmt.execute((new_entry.oid.as_bytes(), &dst))?;
                }
            }
        }
    }
    Ok(())
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

#[derive(Debug)]
struct RefDiff {
    name: String,
    old_target: Option<Oid>,
    new_target: Option<Oid>,
}

impl<'a> TryFrom<&Row<'a>> for RefDiff {
    type Error = rusqlite::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let name: String = row.get(0)?;
        let raw_old: Option<Vec<u8>> = row.get(1)?;
        let raw_new: Option<Vec<u8>> = row.get(2)?;
        let old_target = raw_old.map(|v| Oid::from_bytes(&v)).transpose().unwrap();
        let new_target = raw_new.map(|v| Oid::from_bytes(&v)).transpose().unwrap();
        Ok(RefDiff {
            name,
            old_target,
            new_target,
        })
    }
}

fn compare_refs(
    conn: &mut Connection,
    repo_id: u32,
    repo: &Repository,
    glob: &str,
) -> Result<Vec<RefDiff>> {
    conn.execute(
        "CREATE TEMPORARY TABLE new_refs (name TEXT NOT NULL, oid BLOB NOT NULL);",
        [],
    )?;

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare("INSERT INTO new_refs VALUES (?, ?);")?;
    for reference in repo.references_glob(glob)? {
        let reference = reference?;
        match reference.kind() {
            Some(ReferenceType::Direct) => {
                stmt.execute((
                    reference.name().unwrap(),
                    reference.target().unwrap().as_bytes(),
                ))
                .context("insert")?;
            }
            _ => {}
        }
    }

    // Update direct refs
    let mut stmt = tx.prepare(
        "WITH old_refs AS (
            SELECT name, target_oid as oid
            FROM direct_refs
            WHERE repo_id = ?
        )
        SELECT
            COALESCE(old_refs.name, new_refs.name),
            old_refs.oid as old_oid,
            new_refs.oid as new_oid
        FROM old_refs
        FULL JOIN new_refs
            ON old_refs.name = new_refs.name
        WHERE new_oid IS NOT NULL;",
    )?;
    let mut rows = stmt.query((repo_id,))?;
    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(RefDiff::try_from(row)?);
    }

    Ok(res)
}
