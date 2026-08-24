//! Sixth kernel name: [`Place`] — where a session is attached, not an Environment.

use std::path::Path;

use crate::PlaceId;

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

/// One attached location. A Session may have many, keyed by [`PlaceId`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Place {
    pub id: PlaceId,
    pub provider: PlaceProvider,
    pub instance: String,
    pub os: PlaceOs,
    pub attach: PlaceAttach,
}

impl Place {
    pub fn local_dir(path: impl Into<String>, attach: PlaceAttach) -> Self {
        Self {
            id: PlaceId::new(),
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
    use crate::{Error, InMemory, PlaceId};

    fn aws(instance: &str) -> Place {
        Place {
            id: PlaceId::new(),
            provider: PlaceProvider::Aws,
            instance: instance.into(),
            os: PlaceOs::Linux,
            attach: PlaceAttach::MustExist,
        }
    }

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
        assert!(session.places().unwrap().is_empty());
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
        assert_eq!(session.places().unwrap(), vec![place.clone()]);
        assert_eq!(session.place(place.id).unwrap().as_ref(), Some(&place));
    }

    #[test]
    fn many_local_dir_attaches_succeed() {
        let store = InMemory::new();
        let session = store.create_session();
        let a = std::env::temp_dir();
        let b = std::env::current_dir().unwrap();
        let pa = session
            .attach_place(Place::local_dir(a.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let pb = session
            .attach_place(Place::local_dir(b.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        assert_ne!(pa.id, pb.id);
        let mut ids: Vec<_> = session.places().unwrap().into_iter().map(|p| p.id).collect();
        ids.sort_by_key(|id| id.0);
        let mut expected = vec![pa.id, pb.id];
        expected.sort_by_key(|id| id.0);
        assert_eq!(ids, expected);
    }

    #[test]
    fn same_path_second_attach_does_not_duplicate() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let first = session
            .attach_place(Place::local_dir(dir.clone(), PlaceAttach::MustExist))
            .unwrap();
        let second = session
            .attach_place(Place::local_dir(dir, PlaceAttach::MustExist))
            .unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(session.places().unwrap().len(), 1);
    }

    #[test]
    fn aws_and_local_dir_both_stay() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let local = session
            .attach_place(Place::local_dir(dir.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let cloud = session.attach_place(aws("i-not-a-local-dir")).unwrap();
        assert_eq!(session.places().unwrap().len(), 2);
        assert!(session.place(local.id).unwrap().is_some());
        assert!(session.place(cloud.id).unwrap().is_some());
    }

    #[test]
    fn refuse_provider_swap_on_same_place_id() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let local = session
            .attach_place(Place::local_dir(dir.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let err = session
            .attach_place(Place {
                id: local.id,
                provider: PlaceProvider::Aws,
                instance: "i-not-a-local-dir".into(),
                os: PlaceOs::Linux,
                attach: PlaceAttach::MustExist,
            })
            .unwrap_err();
        assert!(matches!(err, Error::PlaceProviderSwap));
        assert_eq!(session.places().unwrap().len(), 1);
        assert!(matches!(
            session.place(local.id).unwrap().unwrap().provider,
            PlaceProvider::LocalDir
        ));
    }

    #[test]
    fn detach_one_leaves_the_other() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let local = session
            .attach_place(Place::local_dir(dir.to_string_lossy().into_owned(), PlaceAttach::MustExist))
            .unwrap();
        let cloud = session.attach_place(aws("i-keep")).unwrap();
        let gone = session.detach_place(local.id).unwrap();
        assert_eq!(gone.id, local.id);
        assert_eq!(session.places().unwrap().len(), 1);
        assert_eq!(session.place(cloud.id).unwrap().as_ref(), Some(&cloud));
        assert!(session.place(local.id).unwrap().is_none());
        let err = session.detach_place(local.id).unwrap_err();
        assert!(matches!(err, Error::UnknownPlace(id) if id == local.id));
    }

    #[test]
    fn local_dir_cannot_pretend_aws() {
        let store = InMemory::new();
        let session = store.create_session();
        let dir = std::env::temp_dir();
        let err = session
            .attach_place(Place {
                id: PlaceId::new(),
                provider: PlaceProvider::Aws,
                instance: dir.to_string_lossy().into_owned(),
                os: PlaceOs::Linux,
                attach: PlaceAttach::MustExist,
            })
            .unwrap_err();
        assert!(matches!(err, Error::PlaceProviderMismatch));
        assert!(session.places().unwrap().is_empty());
    }
}
