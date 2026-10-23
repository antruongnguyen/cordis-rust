use cordis_core::{BoxError, Context};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

pub const MAX_SNAPSHOT_BYTES: u64 = 4 * 1024 * 1024;
pub const ITEM_LIMIT: usize = 100;

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub fn validate_repository(repository: &str) -> Result<(), io::Error> {
    let parts: Vec<_> = repository.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || !part.starts_with(|c: char| c.is_ascii_alphanumeric())
                || !part
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                || *part == "."
                || *part == ".."
        })
    {
        return Err(invalid("repository must be an ASCII OWNER/REPO"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum State {
    Open,
    Closed,
    Merged,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Item {
    pub number: u64,
    pub title: String,
    pub state: State,
    pub url: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub repository: String,
    pub captured_at: u64,
    pub issues: Vec<Item>,
    pub pull_requests: Vec<Item>,
}

fn valid_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
        || bytes
            .iter()
            .enumerate()
            .any(|(i, c)| ![4, 7, 10, 13, 16, 19].contains(&i) && !c.is_ascii_digit())
    {
        return false;
    }
    let number = |start: usize, end: usize| {
        bytes[start..end]
            .iter()
            .fold(0_u32, |n, c| n * 10 + u32::from(c - b'0'))
    };
    let year = number(0, 4);
    let month = number(5, 7);
    let day = number(8, 10);
    let days = match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return false,
    };
    year > 0
        && (1..=days).contains(&day)
        && number(11, 13) < 24
        && number(14, 16) < 60
        && number(17, 19) < 60
}

impl Snapshot {
    pub fn validate(&self) -> Result<(), io::Error> {
        validate_repository(&self.repository)?;
        let mut numbers = HashSet::new();
        for (items, path) in [(&self.issues, "issues"), (&self.pull_requests, "pull")] {
            if items.len() > ITEM_LIMIT {
                return Err(invalid("snapshot exceeds the 100-item limit per kind"));
            }
            for item in items {
                if item.number == 0
                    || !numbers.insert(item.number)
                    || item.title.trim().is_empty()
                    || !valid_timestamp(&item.updated_at)
                    || (path == "issues" && item.state == State::Merged)
                    || item.url
                        != format!(
                            "https://github.com/{}/{path}/{}",
                            self.repository, item.number
                        )
                {
                    return Err(invalid(format!("invalid or duplicate {path} item")));
                }
            }
        }
        Ok(())
    }
}

pub async fn read_limited(reader: impl AsyncRead + Unpin, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > limit {
        return Err(invalid(format!("input exceeds {limit} bytes")));
    }
    Ok(bytes)
}

pub async fn validate_output_parent(output: &Path) -> io::Result<()> {
    if output.file_name().is_none() {
        return Err(invalid("output needs a filename"));
    }
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if !tokio::fs::metadata(parent).await?.is_dir() {
        return Err(invalid("output parent must be an existing directory"));
    }
    Ok(())
}

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct Temporary(PathBuf);

impl Drop for Temporary {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.0) {
            if error.kind() != io::ErrorKind::NotFound {
                eprintln!("removing temporary output {}: {error}", self.0.display());
            }
        }
    }
}

pub async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    let name = path
        .file_name()
        .ok_or_else(|| invalid("output needs a filename"))?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = None;
    for _ in 0..16 {
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let mut filename = name.to_os_string();
        filename.push(format!(".{}.{}.tmp", std::process::id(), id));
        let candidate = parent.join(filename);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(file) => {
                temporary = Some((Temporary(candidate), file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let (temporary, mut file) =
        temporary.ok_or_else(|| invalid("temporary output names exhausted"))?;
    file.write_all(bytes).await?;
    file.flush().await?;
    drop(file);
    tokio::fs::rename(&temporary.0, path).await?;
    Ok(())
}

pub async fn drain(root: &Context) -> Result<(), BoxError> {
    let report = root.drain()?.await?;
    match report.failures().first() {
        Some(error) => Err(error.clone().into()),
        None => Ok(()),
    }
}
