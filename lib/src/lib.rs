use std::{
    fmt, io,
    path::{Path, PathBuf},
    pin::Pin,
    task,
};

use async_fs::File;
pub use async_trait::async_trait;
use derive_more::with_trait::{Display, Error};
pub use futures;
use futures::{AsyncRead, AsyncWriteExt as _, Stream, StreamExt as _, pin_mut};
use tracerr::Traced;
use uuid::Uuid;

/// Execution of a filesystem operation.
#[allow( // false positive on newer Clippy versions
    clippy::double_must_use,
    reason = "`#[must_use]` is added by `#[async_trait]` macro"
)]
#[async_trait]
pub trait Exec<Operation> {
    type Ok;
    type Err;

    /// Executes the provided `operation`.
    async fn exec(&self, operation: Operation) -> Result<Self::Ok, Self::Err>;
}

/// Storage backed by files and directories.
#[derive(Clone, Debug)]
pub struct Storage {
    /// Absolute [`Path`] to the directory to persist data in.
    data_dir: PathBuf,

    /// Absolute [`Path`] to the directory to use for storing temporary data.
    ///
    /// [System's temporary directory][0] is not used, because it may be located
    /// on another device or has another filesystem, thus doesn't guarantee
    /// atomicity of [copy](async_fs::copy) and [rename](async_fs::rename)
    /// operations, which is vital for how this directory is used: files and
    /// symlinks are fully created here first, and only then are
    /// [renamed](async_fs::rename) to their destination in the
    /// [`Storage::data_dir`], so readers never observe partially written
    /// data.
    ///
    /// The contents of this directory are removed on every [`Storage::new()`],
    /// cleaning up leftovers of interrupted operations.
    ///
    /// [0]: https://en.wikipedia.org/wiki/Temporary_folder
    tmp_dir: PathBuf,
}

impl Storage {
    /// Creates a new [`Storage`].
    ///
    /// If the provided `root` directory doesn't exist yet, tries to create it
    /// recursively.
    ///
    /// # Errors
    ///
    /// - If the provided `root` is an invalid path.
    /// - If the provided `root` is not a directory.
    pub async fn new(
        root: impl Into<PathBuf>,
    ) -> Result<Self, Traced<io::Error>> {
        let root = root.into();

        let data = root.join("data");
        async_fs::create_dir_all(&data).await.map_err(tracerr::wrap!())?;

        let tmp = root.join("tmp");
        remove_existing_dir(&tmp).await.map_err(tracerr::wrap!())?;
        async_fs::create_dir_all(&tmp).await.map_err(tracerr::wrap!())?;

        Ok(Self {
            data_dir: async_fs::canonicalize(data)
                .await
                .map_err(tracerr::wrap!())?,
            tmp_dir: async_fs::canonicalize(tmp)
                .await
                .map_err(tracerr::wrap!())?,
        })
    }
}

/// Removes the existing `dir`ectory.
///
/// # Idempotent
///
/// Succeeds if the `dir`ectory doesn't exist already.
///
/// # Errors
///
/// If [`async_fs::remove_dir_all()`] errors with anything other than
/// [`io::ErrorKind::NotFound`].
async fn remove_existing_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    async_fs::remove_dir_all(dir).await.or_else(|e| {
        (e.kind() == io::ErrorKind::NotFound).then_some(()).ok_or(e)
    })
}

/// Operation of a new file creation.
#[derive(Clone, Debug)]
pub struct CreateFile<Bytes> {
    /// [`RelativePath`] of the file to be created.
    pub path: RelativePath,

    /// [`Stream`] of file bytes.
    pub bytes: Bytes,
}

#[async_trait]
impl<S, Bytes> Exec<CreateFile<S>> for Storage
where
    S: Stream<Item = Result<Bytes, io::Error>> + fmt::Debug + Send + 'static,
    Bytes: AsRef<[u8]> + Send + Sync,
{
    type Ok = ();
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: CreateFile<S>) -> Result<Self::Ok, Self::Err> {
        // We want files to be written atomically, so a failed or interrupted
        // upload never leaves a partial file. So, it's required to do this in
        // 2 steps:
        // 1. write the whole file into a temporary one, and sync it to disk;
        // 2. replace the original file with the temporary one.
        let tmp = self.tmp_dir.join(Uuid::new_v4().to_string());
        let dest = self.data_dir.join(op.path);

        let mut res = write_file(&tmp, op.bytes).await;
        if res.is_ok() {
            res = move_file(&tmp, &dest).await;
        }
        if res.is_err() {
            // Best-effort cleanup, the original error is more important.
            drop(async_fs::remove_file(&tmp).await);
        }
        res
    }
}

/// Writes all the provided `bytes` into a new file at the provided `path`, and
/// syncs it to disk.
///
/// # Errors
///
/// - If the file cannot be created.
/// - If the provided `bytes` [`Stream`] yields an error.
/// - If writing, flushing or syncing the file fails.
async fn write_file<S, Bytes>(
    path: &Path,
    bytes: S,
) -> Result<(), Traced<io::Error>>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
    Bytes: AsRef<[u8]>,
{
    let mut f = File::create(path).await.map_err(tracerr::wrap!())?;

    pin_mut!(bytes);
    while let Some(res) = bytes.next().await {
        f.write_all(res.map_err(tracerr::wrap!())?.as_ref())
            .await
            .map_err(tracerr::wrap!())?;
    }
    f.flush().await.map_err(tracerr::wrap!())?;
    f.sync_all().await.map_err(tracerr::wrap!())
}

/// Atomically moves the file from the `src` path to the `dest` path, creating
/// all the missing parent directories of the `dest` path.
///
/// Replaces the existing file at the `dest` path (if any). If it's a symlink,
/// then the symlink itself is replaced, not the file it points to.
///
/// # Errors
///
/// - If the parent directories of the `dest` path cannot be created.
/// - If [`async_fs::rename()`] fails.
async fn move_file(src: &Path, dest: &Path) -> Result<(), Traced<io::Error>> {
    in_parent_dir(dest, || async_fs::rename(src, dest))
        .await
        .map_err(tracerr::wrap!())
}

/// Maximum number of attempts [`in_parent_dir()`] makes.
const MAX_PARENT_DIR_ATTEMPTS: usize = 3;

/// Creates all the missing parent directories of the provided `path`, and then
/// executes the provided `op`eration, which is expected to create something at
/// this `path`.
///
/// [`DeleteFile`] prunes empty directories concurrently, so the parent
/// directories may be removed right after being created. That's why the whole
/// thing is retried (up to [`MAX_PARENT_DIR_ATTEMPTS`] times) if either step
/// fails with [`io::ErrorKind::NotFound`].
///
/// # Errors
///
/// - If the parent directories of the `path` cannot be created.
/// - If the provided `op`eration fails.
async fn in_parent_dir<F, Fut>(path: &Path, mut op: F) -> io::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<()>>,
{
    let mut attempt = 1;
    loop {
        let res = async {
            if let Some(dir) = path.parent() {
                async_fs::create_dir_all(dir).await?;
            }
            op().await
        }
        .await;

        match res {
            Err(e)
                if e.kind() == io::ErrorKind::NotFound
                    && attempt < MAX_PARENT_DIR_ATTEMPTS =>
            {
                tracing::debug!("Parent directory was pruned, retrying: {e}");
                attempt += 1;
            }
            res => return res,
        }
    }
}

/// Operation of a symlink creation.
#[derive(Debug, Clone)]
pub struct CreateSymlink {
    /// [`RelativePath`] of the original source file.
    pub src: RelativePath,

    /// [`RelativePath`] of the symlink itself.
    pub dest: RelativePath,
}

#[async_trait]
impl Exec<CreateSymlink> for Storage {
    type Ok = ();
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: CreateSymlink) -> Result<Self::Ok, Self::Err> {
        let dest = self.data_dir.join(op.dest);
        let src = self.data_dir.join(op.src);

        match in_parent_dir(&dest, || async_fs::unix::symlink(&src, &dest))
            .await
        {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(tracerr::new!(e)),
        }

        // We want symlinks to be overwritten atomically, so it's required to
        // do this in 2 steps:
        // 1. create temporary symlink file;
        // 2. replace the original file with the temporary one.
        let tmp = self.tmp_dir.join(Uuid::new_v4().to_string());
        async_fs::unix::symlink(src, &tmp).await.map_err(tracerr::wrap!())?;
        let res = move_file(&tmp, &dest).await;
        if res.is_err() {
            // Best-effort cleanup, the original error is more important.
            drop(async_fs::remove_file(&tmp).await);
        }
        res
    }
}

/// Operation of an existing file deletion.
///
/// # Idempotent
///
/// Succeeds if there is no file at the [`DeleteFile::path`] already.
///
/// # Symlinks
///
/// If the [`DeleteFile::path`] is a symlink, then only the symlink itself is
/// removed, while the file it points to stays intact.
///
/// Deleting a file doesn't touch symlinks pointing to it, so they become
/// dangling, and [`GetFile`] returns [`None`] for them.
///
/// # Empty directories
///
/// Once the file is removed, its parent directories that became empty are
/// removed too, up to (but not including) the data directory of the
/// [`Storage`]. This is done on a best-effort basis: the first non-empty
/// directory stops it, and errors caused by concurrent operations are ignored.
#[derive(Clone, Debug)]
pub struct DeleteFile {
    /// [`RelativePath`] of the file to be deleted.
    pub path: RelativePath,
}

#[async_trait]
impl Exec<DeleteFile> for Storage {
    type Ok = ();
    type Err = Traced<DeleteFileError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: DeleteFile) -> Result<Self::Ok, Self::Err> {
        let path = self.data_dir.join(op.path);

        match async_fs::remove_file(&path).await {
            Ok(()) => {}
            // `NotADirectory` means that some parent of the `path` is a file,
            // so there is no file at the `path` either.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory,
                ) =>
            {
                return Ok(());
            }
            Err(e) => {
                // Depending on the platform, removing a directory fails with
                // different errors, so check it explicitly.
                let is_dir = async_fs::symlink_metadata(&path)
                    .await
                    .is_ok_and(|m| m.is_dir());
                return Err(tracerr::new!(if is_dir {
                    DeleteFileError::IsDirectory
                } else {
                    DeleteFileError::Io(e)
                }));
            }
        }

        self.prune_empty_dirs(&path).await;
        Ok(())
    }
}

impl Storage {
    /// Removes empty parent directories of the provided `path`, up to (but not
    /// including) the [`Storage::data_dir`].
    ///
    /// Stops at the first non-empty directory. Never fails, because the file
    /// at the `path` is removed already, and concurrent operations may create
    /// or remove the same directories at any moment.
    async fn prune_empty_dirs(&self, path: &Path) {
        let mut dir = path.parent();
        while let Some(d) = dir.filter(|d| {
            d.starts_with(&self.data_dir) && *d != self.data_dir.as_path()
        }) {
            match async_fs::remove_dir(d).await {
                Ok(()) => {}
                // Already removed by a concurrent operation.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => {
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to prune `{}` directory: {e}",
                        d.display(),
                    );
                    break;
                }
            }
            dir = d.parent();
        }
    }
}

/// Error of executing a [`DeleteFile`] operation.
#[derive(Debug, Display, Error)]
pub enum DeleteFileError {
    /// [`DeleteFile::path`] is a directory, not a file.
    ///
    /// In S3 terms it's a prefix of other objects, rather than an object
    /// itself, so the directory is left intact.
    #[display("Path is a directory")]
    IsDirectory,

    /// Failed to remove the file.
    #[display("Failed to remove file: {_0}")]
    Io(io::Error),
}

/// Operation for getting an existing file.
#[derive(Debug, Clone)]
pub struct GetFile {
    /// [`RelativePath`] of the file.
    pub path: RelativePath,
}

#[async_trait]
impl Exec<GetFile> for Storage {
    type Ok = Option<ReadOnlyFile>;
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: GetFile) -> Result<Self::Ok, Self::Err> {
        let path = self.data_dir.join(op.path);

        match File::open(&path).await {
            Ok(f) => Ok(Some(ReadOnlyFile(f))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(tracerr::new!(e)),
        }
    }
}

/// Read-only [`File`].
pub struct ReadOnlyFile(File);

impl AsyncRead for ReadOnlyFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> task::Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

/// Filesystem path relative to the configured root folder.
///
/// # Format
///
/// The following [`PathBuf`] components are forbidden:
/// - root (leading `/`)
/// - current directory (`.`)
/// - parent directory (`..`)
/// - empty component (`//`)
#[derive(Clone, Debug)]
pub struct RelativePath(PathBuf);

impl RelativePath {
    /// Joins another [`RelativePath`]s to this one.
    #[must_use]
    pub fn join(mut self, other: RelativePath) -> Self {
        self.0.push(other.0);
        self
    }
}

impl AsRef<Path> for RelativePath {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}

impl TryFrom<String> for RelativePath {
    type Error = InvalidRelativePathError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.starts_with('/')
            || s.split('/').any(|c| matches!(c, "" | "." | ".."))
        {
            tracing::warn!("Invalid RelativePath");
            Err(InvalidRelativePathError)
        } else {
            Ok(RelativePath(s.into()))
        }
    }
}

/// Error of parsing [`RelativePath`] from a [`String`].
#[derive(Debug, Display, Error)]
#[display("Invalid `RelativePath` format")]
pub struct InvalidRelativePathError;

#[cfg(test)]
mod spec_util {
    use std::{env, path::PathBuf};

    use uuid::Uuid;

    use super::RelativePath;

    /// Temporary [`Storage`] root, removed on [`Drop`].
    ///
    /// [`Storage`]: super::Storage
    pub(super) struct Root(pub(super) PathBuf);

    impl Root {
        pub(super) fn new() -> Self {
            Self(env::temp_dir().join(format!("baza-{}", Uuid::new_v4())))
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.0));
        }
    }

    pub(super) fn path(s: &str) -> RelativePath {
        RelativePath::try_from(s.to_owned()).unwrap()
    }

    pub(super) fn tmp_is_empty(root: &Root) -> bool {
        std::fs::read_dir(root.0.join("tmp")).unwrap().next().is_none()
    }
}

#[cfg(test)]
mod create_file_spec {
    use std::io;

    use futures::{executor::block_on, stream};

    use super::{
        CreateFile, Exec as _, Storage,
        spec_util::{Root, path, tmp_is_empty},
    };

    #[test]
    fn writes_file() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let bytes =
                stream::iter([Ok(b"hello ".to_vec()), Ok(b"world".into())]);

            storage
                .exec(CreateFile { path: path("bucket/dir/key"), bytes })
                .await
                .unwrap();
        });

        let stored = std::fs::read(root.0.join("data/bucket/dir/key")).unwrap();
        assert_eq!(stored, b"hello world");
        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }

    #[test]
    fn leaves_nothing_on_stream_error() {
        let root = Root::new();
        let res = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let bytes = stream::iter([
                Ok(b"partial".to_vec()),
                Err(io::Error::other("connection reset")),
            ]);

            storage
                .exec(CreateFile { path: path("bucket/dir/key"), bytes })
                .await
        });

        assert!(res.is_err(), "`CreateFile` should fail");
        assert!(
            !root.0.join("data/bucket/dir/key").exists(),
            "partial file is left at the destination",
        );
        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }

    #[test]
    fn keeps_original_on_stream_error() {
        let root = Root::new();
        let res = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let bytes = stream::iter([Ok(b"original".to_vec())]);
            storage
                .exec(CreateFile { path: path("bucket/key"), bytes })
                .await
                .unwrap();

            let bytes = stream::iter([
                Ok(b"partial".to_vec()),
                Err(io::Error::other("connection reset")),
            ]);
            storage.exec(CreateFile { path: path("bucket/key"), bytes }).await
        });

        assert!(res.is_err(), "`CreateFile` should fail");
        let stored = std::fs::read(root.0.join("data/bucket/key")).unwrap();
        assert_eq!(stored, b"original");
        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }
}

#[cfg(test)]
mod delete_file_spec {
    use std::fs;

    use futures::{executor::block_on, stream};

    use super::{
        CreateFile, CreateSymlink, DeleteFile, DeleteFileError, Exec as _,
        GetFile, Storage,
        spec_util::{Root, path},
    };

    async fn create(storage: &Storage, p: &str) {
        let bytes = stream::iter([Ok(b"bytes".to_vec())]);
        storage.exec(CreateFile { path: path(p), bytes }).await.unwrap();
    }

    #[test]
    fn deletes_file_and_prunes_empty_dirs() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/a/b/key").await;

            storage.exec(DeleteFile { path: path("bucket/a/b/key") }).await
        })
        .unwrap();

        assert!(!root.0.join("data/bucket").exists(), "empty dirs are left");
        assert!(root.0.join("data").is_dir(), "`data/` is removed");
    }

    #[test]
    fn stops_pruning_at_non_empty_dir() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/a/b/key").await;
            create(&storage, "bucket/a/other").await;

            storage.exec(DeleteFile { path: path("bucket/a/b/key") }).await
        })
        .unwrap();

        assert!(!root.0.join("data/bucket/a/b").exists(), "empty dir is left");
        assert!(root.0.join("data/bucket/a/other").is_file());
    }

    #[test]
    fn succeeds_on_missing_file() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/file").await;

            for p in ["bucket/missing", "missing/key", "bucket/file/key"] {
                storage.exec(DeleteFile { path: path(p) }).await.unwrap();
            }
        });

        assert!(root.0.join("data/bucket/file").is_file());
    }

    #[test]
    fn leaves_directory_intact() {
        let root = Root::new();
        let res = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/dir/key").await;

            storage.exec(DeleteFile { path: path("bucket/dir") }).await
        });

        assert!(
            matches!(
                res.map_err(tracerr::Traced::into_inner),
                Err(DeleteFileError::IsDirectory),
            ),
            "expected `DeleteFileError::IsDirectory`",
        );
        assert!(root.0.join("data/bucket/dir/key").is_file());
    }

    #[test]
    fn deletes_only_symlink_itself() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/target").await;
            storage
                .exec(CreateSymlink {
                    src: path("bucket/target"),
                    dest: path("links/link"),
                })
                .await
                .unwrap();

            storage.exec(DeleteFile { path: path("links/link") }).await
        })
        .unwrap();

        assert!(fs::symlink_metadata(root.0.join("data/links/link")).is_err());
        assert_eq!(
            fs::read(root.0.join("data/bucket/target")).unwrap(),
            b"bytes"
        );
    }

    #[test]
    fn leaves_dangling_symlink_on_target_deletion() {
        let root = Root::new();
        let file = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/target").await;
            storage
                .exec(CreateSymlink {
                    src: path("bucket/target"),
                    dest: path("links/link"),
                })
                .await
                .unwrap();

            storage
                .exec(DeleteFile { path: path("bucket/target") })
                .await
                .unwrap();
            storage.exec(GetFile { path: path("links/link") }).await.unwrap()
        });

        assert!(file.is_none(), "dangling symlink is readable");
        assert!(
            fs::symlink_metadata(root.0.join("data/links/link"))
                .unwrap()
                .is_symlink(),
            "symlink is removed",
        );
    }

    #[test]
    fn recreates_pruned_parent_dirs() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/a/key").await;
            storage
                .exec(DeleteFile { path: path("bucket/a/key") })
                .await
                .unwrap();

            create(&storage, "bucket/a/key").await;
            storage
                .exec(CreateSymlink {
                    src: path("bucket/a/key"),
                    dest: path("links/a/link"),
                })
                .await
                .unwrap();
        });

        assert!(root.0.join("data/bucket/a/key").is_file());
        assert!(root.0.join("data/links/a/link").is_file());
    }
}

#[cfg(test)]
mod relative_path_spec {
    use super::{InvalidRelativePathError, RelativePath};

    fn case(s: &str) -> Result<RelativePath, InvalidRelativePathError> {
        RelativePath::try_from(s.to_string())
    }

    #[test]
    fn cannot_start_with_root_component() {
        assert!(case("/my/path").is_err());
    }

    #[test]
    fn cannot_contain_empty_component() {
        assert!(case("//my/path").is_err());
        assert!(case("my//path").is_err());
        assert!(case("my/path//").is_err());
    }

    #[test]
    fn cannot_contain_cur_dir_component() {
        assert!(case("/./my/path").is_err());
        assert!(case("./my/path").is_err());
        assert!(case("my/./path").is_err());
        assert!(case("my/path/.").is_err());
        assert!(case("my/path/./").is_err());
    }

    #[test]
    fn cannot_contain_parent_dir_component() {
        assert!(case("/../my/path").is_err());
        assert!(case("../my/path").is_err());
        assert!(case("my/../path").is_err());
        assert!(case("my/path/..").is_err());
        assert!(case("my/path/../").is_err());
    }
}
