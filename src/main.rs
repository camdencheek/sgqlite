use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use git2::{Oid, ReferenceType, Repository, Sort};
use rusqlite::{Connection, Row, Transaction};
use rusqlite_migration::{Migrations, M};
mod ingest;
use ingest::Ingestor;

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

    let tx = conn.transaction()?;
    let changed_refs = compare_refs(&tx, args.repo_id, &repo, "refs/heads/*")?;
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

    // Ingest each new commit
    let mut ingestor = Ingestor::new(&repo, &tx)?;
    for (i, commit_oid) in walker.enumerate() {
        let commit_oid = commit_oid?;
        println!("{}, {}", i, commit_oid);
        ingestor.add_commit(commit_oid)?;
    }
    std::mem::drop(ingestor);

    tx.commit()?;

    Ok(())
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
    tx: &Transaction,
    repo_id: u32,
    repo: &Repository,
    glob: &str,
) -> Result<Vec<RefDiff>> {
    tx.execute(
        "CREATE TEMPORARY TABLE new_refs (name TEXT NOT NULL, oid BLOB NOT NULL);",
        [],
    )?;

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
        WHERE new_oid IS NOT NULL AND new_oid != old_oid;",
    )?;
    let mut rows = stmt.query((repo_id,))?;
    let mut res = Vec::new();
    while let Some(row) = rows.next()? {
        res.push(RefDiff::try_from(row)?);
    }

    Ok(res)
}
