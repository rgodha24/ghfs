//! Shared types for ghfs

use std::fmt;
use std::str::FromStr;
use thiserror::Error;

/// Error type for parsing failures
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("value cannot be empty")]
    Empty,
    #[error("invalid character in value: {0}")]
    InvalidCharacter(char),
    #[error("value cannot start with '{0}'")]
    InvalidStart(char),
    #[error("value cannot end with '{0}'")]
    InvalidEnd(char),
    #[error("missing separator '/' in repo key")]
    MissingSeparator,
    #[error("invalid owner: {0}")]
    InvalidOwner(#[source] Box<ParseError>),
    #[error("invalid repo: {0}")]
    InvalidRepo(#[source] Box<ParseError>),
}

/// A GitHub owner (user or organization)
///
/// Validation rules:
/// - Non-empty
/// - Alphanumeric characters and hyphens only
/// - Cannot start or end with a hyphen
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Owner(String);

impl Owner {
    /// Returns the owner name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Owner {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParseError::Empty);
        }

        if s.starts_with('-') {
            return Err(ParseError::InvalidStart('-'));
        }

        if s.ends_with('-') {
            return Err(ParseError::InvalidEnd('-'));
        }

        for c in s.chars() {
            if !c.is_ascii_alphanumeric() && c != '-' {
                return Err(ParseError::InvalidCharacter(c));
            }
        }

        Ok(Owner(s.to_string()))
    }
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A GitHub repository name
///
/// Validation rules:
/// - Non-empty
/// - Alphanumeric characters, hyphens, underscores, and dots only
/// - Cannot start with a dot
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Repo(String);

impl Repo {
    /// Returns the repository name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Repo {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParseError::Empty);
        }

        if s.starts_with('.') {
            return Err(ParseError::InvalidStart('.'));
        }

        for c in s.chars() {
            if !c.is_ascii_alphanumeric() && c != '-' && c != '_' && c != '.' {
                return Err(ParseError::InvalidCharacter(c));
            }
        }

        Ok(Repo(s.to_string()))
    }
}

impl fmt::Display for Repo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a specific GitHub repository (owner + repo)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoKey {
    /// Repository owner (user or organization).
    pub owner: Owner,
    /// Repository name.
    pub repo: Repo,
}

impl RepoKey {
    /// Creates a new RepoKey from owner and repo
    pub fn new(owner: Owner, repo: Repo) -> Self {
        Self { owner, repo }
    }
}

impl FromStr for RepoKey {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (owner_str, repo_str) = s.split_once('/').ok_or(ParseError::MissingSeparator)?;

        let owner = owner_str
            .parse::<Owner>()
            .map_err(|e| ParseError::InvalidOwner(Box::new(e)))?;
        let repo = repo_str
            .parse::<Repo>()
            .map_err(|e| ParseError::InvalidRepo(Box::new(e)))?;

        Ok(RepoKey { owner, repo })
    }
}

impl fmt::Display for RepoKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.owner, self.repo)
    }
}

/// Identifies a cache generation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenerationId(pub u64);

impl GenerationId {
    /// Creates a new GenerationId
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the inner u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for GenerationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod owner_tests {
        use super::*;

        #[test]
        fn valid_owner_with_hyphen() {
            let owner: Owner = "my-org".parse().unwrap();
            assert_eq!(owner.as_str(), "my-org");
        }

        #[test]
        fn valid_owner_with_numbers() {
            let owner: Owner = "user123".parse().unwrap();
            assert_eq!(owner.as_str(), "user123");
        }

        #[test]
        fn invalid_owner_empty() {
            let result = "".parse::<Owner>();
            assert_eq!(result, Err(ParseError::Empty));
        }

        #[test]
        fn invalid_owner_leading_hyphen() {
            let result = "-user".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidStart('-')));
        }

        #[test]
        fn invalid_owner_trailing_hyphen() {
            let result = "user-".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidEnd('-')));
        }

        #[test]
        fn invalid_owner_underscore() {
            let result = "my_org".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidCharacter('_')));
        }

        #[test]
        fn invalid_owner_dot() {
            let result = "my.org".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidCharacter('.')));
        }

        #[test]
        fn invalid_owner_space() {
            let result = "my org".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidCharacter(' ')));
        }

        #[test]
        fn invalid_owner_slash() {
            let result = "my/org".parse::<Owner>();
            assert_eq!(result, Err(ParseError::InvalidCharacter('/')));
        }
    }

    mod repo_tests {
        use super::*;

        #[test]
        fn valid_repo_complex() {
            let repo: Repo = "my-repo_v2.0".parse().unwrap();
            assert_eq!(repo.as_str(), "my-repo_v2.0");
        }

        #[test]
        fn invalid_repo_empty() {
            let result = "".parse::<Repo>();
            assert_eq!(result, Err(ParseError::Empty));
        }

        #[test]
        fn invalid_repo_leading_dot() {
            let result = ".hidden".parse::<Repo>();
            assert_eq!(result, Err(ParseError::InvalidStart('.')));
        }

        #[test]
        fn invalid_repo_space() {
            let result = "my repo".parse::<Repo>();
            assert_eq!(result, Err(ParseError::InvalidCharacter(' ')));
        }

        #[test]
        fn invalid_repo_slash() {
            let result = "my/repo".parse::<Repo>();
            assert_eq!(result, Err(ParseError::InvalidCharacter('/')));
        }

        #[test]
        fn invalid_repo_at_symbol() {
            let result = "repo@v1".parse::<Repo>();
            assert_eq!(result, Err(ParseError::InvalidCharacter('@')));
        }
    }

    mod repo_key_tests {
        use super::*;

        #[test]
        fn valid_repo_key() {
            let key: RepoKey = "octocat/hello-world".parse().unwrap();
            assert_eq!(key.owner.as_str(), "octocat");
            assert_eq!(key.repo.as_str(), "hello-world");
        }

        #[test]
        fn invalid_repo_key_no_slash() {
            let result = "octocat".parse::<RepoKey>();
            assert_eq!(result, Err(ParseError::MissingSeparator));
        }

        #[test]
        fn invalid_repo_key_empty_owner() {
            let result = "/repo".parse::<RepoKey>();
            assert!(matches!(result, Err(ParseError::InvalidOwner(_))));
        }

        #[test]
        fn invalid_repo_key_empty_repo() {
            let result = "owner/".parse::<RepoKey>();
            assert!(matches!(result, Err(ParseError::InvalidRepo(_))));
        }

        #[test]
        fn invalid_repo_key_invalid_owner() {
            let result = "-owner/repo".parse::<RepoKey>();
            assert!(matches!(result, Err(ParseError::InvalidOwner(_))));
        }

        #[test]
        fn invalid_repo_key_invalid_repo() {
            let result = "owner/.repo".parse::<RepoKey>();
            assert!(matches!(result, Err(ParseError::InvalidRepo(_))));
        }
    }
}
