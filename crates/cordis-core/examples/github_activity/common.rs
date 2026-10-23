use cordis_core::{BoxError, Context};
use std::io;

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

pub async fn drain(root: &Context) -> Result<(), BoxError> {
    let report = root.drain()?.await?;
    match report.failures().first() {
        Some(error) => Err(error.clone().into()),
        None => Ok(()),
    }
}
