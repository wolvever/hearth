//! Sixth kernel name: [`Place`] — where a session is attached, not an Environment.

use std::path::Path;

/// Cloud or host provider of a place. LocalDir is never Aws.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlaceProvider {
    LocalDir,
    Aws,
    Azure,
    Gcp,
    CursorVm,
    GrokBox,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlaceOs {
    Linux,
    Windows,
    Macos,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlaceAttach {
    MustExist,
    RecreateFromGit,
    CopyThenMount,
}

/// One attached location. Session may have zero or one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Place {
    pub provider: PlaceProvider,
    pub instance: String,
    pub os: PlaceOs,
    pub attach: PlaceAttach,
}

impl Place {
    pub fn local_dir(path: impl Into<String>, attach: PlaceAttach) -> Self {
        Self {
            provider: PlaceProvider::LocalDir,
            instance: path.into(),
            os: current_os(),
            attach,
        }
    }

    /// Fail-closed checks that do not touch session state.
    pub fn validate(&self) -> crate::Result<()> {
        use crate::Error;
        match self.provider {
            PlaceProvider::LocalDir => {
                if self.attach == PlaceAttach::MustExist && !Path::new(&self.instance).exists() {
                    return Err(Error::PlaceMissing(self.instance.clone()));
                }
            }
            _ => {
                // A real local directory must not be claimed as a cloud provider.
                let p = Path::new(&self.instance);
                if p.is_absolute() && p.exists() {
                    return Err(Error::PlaceProviderMismatch);
                }
            }
        }
        Ok(())
    }
}

fn current_os() -> PlaceOs {
    if cfg!(target_os = "linux") {
        PlaceOs::Linux
    } else if cfg!(target_os = "windows") {
        PlaceOs::Windows
    } else if cfg!(target_os = "macos") {
        PlaceOs::Macos
    } else {
        PlaceOs::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Error, InMemory};

    #[test]
    fn attach_must_exist_fails_if_missing() {
        let store = InMemory::new();
        let session = store.create_session();
        let err = session
            .attach_place(Place::local_dir(
                "/no/such/hearth/place/path",
                PlaceAttach::MustExist,
            ))
            .unwrap_err();
        assert!(matches!(err, Error::PlaceMissing(_)));
        assert!(session.place().unwrap().is_none());
    }

    #[test]
    fn place_survives_unbind() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let place = session
            .attach_place(Place::local_dir(dir.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let b = session.bind("cli", None, None).unwrap();
        session.unbind(b.id).unwrap();
        assert!(session.bindings().unwrap().is_empty());
        assert_eq!(session.place().unwrap().as_ref(), Some(&place));
    }

    #[test]
    fn refuse_provider_swap() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        session
            .attach_place(Place::local_dir(dir.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let err = session
            .attach_place(Place {
                provider: PlaceProvider::Aws,
                instance: "i-not-a-local-dir".into(),
                os: PlaceOs::Linux,
                attach: PlaceAttach::MustExist,
            })
            .unwrap_err();
        assert!(matches!(err, Error::PlaceProviderSwap));
        assert!(matches!(
            session.place().unwrap().unwrap().provider,
            PlaceProvider::LocalDir
        ));
    }

    #[test]
    fn local_dir_cannot_pretend_aws() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let err = session
            .attach_place(Place {
                provider: PlaceProvider::Aws,
                instance: dir.to_string_lossy().into_owned(),
                os: PlaceOs::Linux,
                attach: PlaceAttach::MustExist,
            })
            .unwrap_err();
        assert!(matches!(err, Error::PlaceProviderMismatch));
    }
}
