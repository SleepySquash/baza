use std::{
    fmt,
    fs::{self, File as StdFile},
    io::{self, Read as _, Seek as _, SeekFrom},
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    task,
    time::{Duration, SystemTime},
};

use async_fs::File;
pub use async_trait::async_trait;
use derive_more::with_trait::{Display, Error};
pub use futures;
use futures::{
    AsyncRead, AsyncSeek, AsyncWriteExt as _, Stream, StreamExt as _, pin_mut,
};
use md5::{Digest as _, Md5};
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

    /// Absolute [`Path`] to the directory to store in-progress multipart
    /// uploads in.
    ///
    /// Every upload is a `<upload-id>/` directory, holding its manifest (see
    /// [`MANIFEST_FILE`]) and its uploaded parts (see [`part_file_name()`]).
    ///
    /// It's not the [`Storage::tmp_dir`], because in-progress uploads must
    /// survive restarts, while it's located on the same filesystem as the
    /// [`Storage::data_dir`] for the very same reasons as the
    /// [`Storage::tmp_dir`] is. Abandoned uploads are removed with the
    /// [`AbortExpiredUploads`] operation instead.
    multipart_dir: PathBuf,
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

        let multipart = root.join("multipart");
        async_fs::create_dir_all(&multipart).await.map_err(tracerr::wrap!())?;

        Ok(Self {
            data_dir: async_fs::canonicalize(data)
                .await
                .map_err(tracerr::wrap!())?,
            tmp_dir: async_fs::canonicalize(tmp)
                .await
                .map_err(tracerr::wrap!())?,
            multipart_dir: async_fs::canonicalize(multipart)
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
    write_stream(&mut f, bytes, |_| {}).await?;
    sync_file(&mut f).await
}

/// Writes all the provided `bytes` into the provided [`File`], passing every
/// written chunk to the provided `inspect` function.
///
/// # Errors
///
/// - If the provided `bytes` [`Stream`] yields an error.
/// - If writing into the [`File`] fails.
async fn write_stream<S, Bytes>(
    f: &mut File,
    bytes: S,
    mut inspect: impl FnMut(&[u8]),
) -> Result<(), Traced<io::Error>>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
    Bytes: AsRef<[u8]>,
{
    pin_mut!(bytes);
    while let Some(res) = bytes.next().await {
        let chunk = res.map_err(tracerr::wrap!())?;
        inspect(chunk.as_ref());
        f.write_all(chunk.as_ref()).await.map_err(tracerr::wrap!())?;
    }
    Ok(())
}

/// Flushes the provided [`File`] and syncs it to disk.
///
/// # Errors
///
/// If flushing or syncing the [`File`] fails.
async fn sync_file(f: &mut File) -> Result<(), Traced<io::Error>> {
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
            Err(e) if is_missing(&e) => return Ok(()),
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
///
/// Returns [`None`] if there is no file at the [`GetFile::path`], or it's a
/// directory (a prefix of other objects in S3 terms, rather than an object
/// itself).
///
/// # Symlinks
///
/// Symlinks are followed, so the returned [`ReadOnlyFile`] (and its
/// [`FileMeta`]) is the one the symlink points to. Dangling symlinks are
/// reported as [`None`].
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

        let file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if is_missing(&e) => return Ok(None),
            Err(e) => return Err(tracerr::new!(e)),
        };

        // Metadata is taken from the opened file rather than from the `path`,
        // so it always describes exactly the bytes being read, even if the
        // file is replaced concurrently.
        let meta = file.metadata().await.map_err(tracerr::wrap!())?;
        Ok(FileMeta::from_metadata(&meta)
            .map_err(tracerr::wrap!())?
            .map(|meta| ReadOnlyFile { file, meta }))
    }
}

/// Operation for getting [`FileMeta`] of an existing file.
///
/// Returns [`None`] if there is no file at the [`GetFileMeta::path`], or it's
/// a directory (a prefix of other objects in S3 terms, rather than an object
/// itself).
///
/// # Symlinks
///
/// Symlinks are followed, so the [`FileMeta`] of the file the symlink points to
/// is returned. Dangling symlinks are reported as [`None`].
#[derive(Clone, Debug)]
pub struct GetFileMeta {
    /// [`RelativePath`] of the file.
    pub path: RelativePath,
}

#[async_trait]
impl Exec<GetFileMeta> for Storage {
    type Ok = Option<FileMeta>;
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: GetFileMeta) -> Result<Self::Ok, Self::Err> {
        let path = self.data_dir.join(op.path);

        match async_fs::metadata(&path).await {
            Ok(meta) => {
                FileMeta::from_metadata(&meta).map_err(tracerr::wrap!())
            }
            Err(e) if is_missing(&e) => Ok(None),
            Err(e) => Err(tracerr::new!(e)),
        }
    }
}

/// Operation of listing files of a bucket, in S3 `ListObjects` terms.
///
/// Files are listed by their keys relative to the [`ListFiles::bucket`]
/// (`/`-joined path components), in lexicographic byte order, as S3 does.
///
/// Returns an empty [`FileList`] if the [`ListFiles::bucket`] doesn't exist,
/// because buckets are just directories, which are removed once their last file
/// is [deleted](DeleteFile), so a missing bucket is indistinguishable from an
/// empty one.
///
/// # Symlinks
///
/// Symlinks are listed as files, with the [`FileMeta`] of the file they point
/// to. Dangling symlinks and symlinks to directories are skipped, the same way
/// [`GetFile`] and [`GetFileMeta`] return [`None`] for them.
///
/// Anything else that is neither a regular file nor a directory (as well as
/// entries whose names are not valid UTF-8) is skipped too.
///
/// # Performance
///
/// There is no index: the whole bucket directory tree (except the directories
/// not matching the [`ListFiles::prefix`]) is walked and sorted on every
/// execution, which is fine for small buckets only.
#[derive(Clone, Debug)]
pub struct ListFiles {
    /// [`RelativePath`] of the bucket to list files of.
    pub bucket: RelativePath,

    /// Prefix the listed keys must start with.
    pub prefix: String,

    /// Delimiter grouping the listed keys into [`FileList::common_prefixes`].
    ///
    /// Keys containing this delimiter after the [`ListFiles::prefix`] are not
    /// listed as files. Instead, their parts up to (and including) the first
    /// occurrence of this delimiter after the [`ListFiles::prefix`] are listed
    /// as common prefixes, each one only once.
    ///
    /// [`None`] or an empty delimiter means no grouping.
    pub delimiter: Option<String>,

    /// Key (or common prefix) to list the entries after (exclusively).
    pub start_after: Option<String>,

    /// Maximum number of entries (both files and common prefixes) to list.
    pub max_keys: usize,
}

#[async_trait]
impl Exec<ListFiles> for Storage {
    type Ok = FileList;
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: ListFiles) -> Result<Self::Ok, Self::Err> {
        let dir = self.data_dir.join(&op.bucket);

        // `std::fs` is used in a single blocking task, rather than `async_fs`,
        // because the latter spawns a blocking task for every single call.
        blocking::unblock(move || list_files(&dir, &op)).await
    }
}

/// Lists files of the provided bucket `dir`ectory, as described by the
/// provided [`ListFiles`] operation.
///
/// # Errors
///
/// If reading any directory or file metadata fails with anything other than
/// [`is_missing()`] (which happens on concurrent modifications).
fn list_files(
    dir: &Path,
    op: &ListFiles,
) -> Result<FileList, Traced<io::Error>> {
    let delimiter = op.delimiter.as_deref().filter(|d| !d.is_empty());

    let mut candidates = walk_dir(dir, &op.prefix).map_err(tracerr::wrap!())?;
    candidates.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut list = FileList::default();
    let mut last = None::<String>;
    for (key, path) in candidates {
        let common_prefix = delimiter.and_then(|d| {
            key[op.prefix.len()..]
                .find(d)
                .map(|i| key[..op.prefix.len() + i + d.len()].to_owned())
        });
        let entry = common_prefix.as_ref().unwrap_or(&key);

        if op.start_after.as_ref().is_some_and(|after| entry <= after)
            || last.as_ref() == Some(entry)
        {
            continue;
        }

        // Metadata is taken lazily, only for the entries being listed.
        let meta = match fs::metadata(&path) {
            Ok(meta) => {
                FileMeta::from_metadata(&meta).map_err(tracerr::wrap!())?
            }
            Err(e) if is_missing(&e) => None,
            Err(e) => return Err(tracerr::new!(e)),
        };
        let Some(meta) = meta else {
            // A dangling symlink, a symlink to a directory, or a file removed
            // concurrently.
            continue;
        };

        if list.files.len() + list.common_prefixes.len() >= op.max_keys {
            list.is_truncated = true;
            break;
        }
        if let Some(prefix) = common_prefix {
            list.common_prefixes.push(prefix.clone());
            last = Some(prefix);
        } else {
            last = Some(key.clone());
            list.files.push(ListedFile { key, meta });
        }
    }
    if list.is_truncated {
        list.next_start_after = last;
    }

    Ok(list)
}

/// Recursively walks the provided bucket `dir`ectory, returning keys (along
/// with their filesystem paths) of all the regular files and symlinks in it,
/// starting with the provided `prefix`, in no particular order.
///
/// Directories whose keys cannot start with the `prefix` are not walked.
///
/// # Errors
///
/// If reading any directory fails with anything other than [`is_missing()`]
/// (which happens on concurrent modifications).
fn walk_dir(dir: &Path, prefix: &str) -> io::Result<Vec<(String, PathBuf)>> {
    let mut found = vec![];

    let mut dirs = vec![(dir.to_owned(), String::new())];
    while let Some((dir, dir_key)) = dirs.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if is_missing(&e) => continue,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned)
            else {
                tracing::warn!(
                    "Skipping non-UTF-8 `{}` path",
                    entry.path().display(),
                );
                continue;
            };
            let key = format!("{dir_key}{name}");

            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(e) if is_missing(&e) => continue,
                Err(e) => return Err(e),
            };
            if file_type.is_dir() {
                let dir_key = format!("{key}/");
                if dir_key.starts_with(prefix) || prefix.starts_with(&dir_key) {
                    dirs.push((entry.path(), dir_key));
                }
            } else if (file_type.is_file() || file_type.is_symlink())
                && key.starts_with(prefix)
            {
                found.push((key, entry.path()));
            }
        }
    }

    Ok(found)
}

/// Result of executing a [`ListFiles`] operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileList {
    /// Listed files, in lexicographic byte order of their keys.
    pub files: Vec<ListedFile>,

    /// Listed common prefixes, in lexicographic byte order.
    ///
    /// See [`ListFiles::delimiter`] for details.
    pub common_prefixes: Vec<String>,

    /// Indicator whether there are more entries to list after the listed ones,
    /// which haven't been listed due to [`ListFiles::max_keys`].
    pub is_truncated: bool,

    /// [`ListFiles::start_after`] to list the next entries with, if this
    /// [`FileList`] [is truncated](FileList::is_truncated).
    ///
    /// It's the last listed entry (either a file key or a common prefix).
    pub next_start_after: Option<String>,
}

/// File listed by a [`ListFiles`] operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListedFile {
    /// Key of the file, relative to the [`ListFiles::bucket`].
    pub key: String,

    /// [`FileMeta`] of the file.
    pub meta: FileMeta,
}

/// Name of the file in a multipart upload directory, holding the
/// [`RelativePath`] of the file being uploaded.
///
/// It's written once, when the upload is [created](CreateMultipartUpload), so
/// its last modification time is the creation time of the upload.
const MANIFEST_FILE: &str = "manifest";

/// Minimum size (in bytes) of every part of a multipart upload, except the
/// last one, as S3 requires (5 MiB).
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Size (in bytes) of a [`Md5Digest`].
const MD5_LEN: usize = 16;

/// MD5 digest of some bytes.
pub type Md5Digest = [u8; MD5_LEN];

/// Returns the name of the file holding the part with the provided `number` in
/// a multipart upload directory.
///
/// The file contains the bytes of the part, followed by their [`Md5Digest`],
/// so both are always replaced together and atomically, and the digest
/// doesn't need to be recomputed for every [`ListParts`] or
/// [`CompleteMultipartUpload`].
fn part_file_name(number: u16) -> String {
    format!("part-{number}")
}

/// ID of a multipart upload.
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub struct UploadId(Uuid);

impl FromStr for UploadId {
    type Err = InvalidUploadIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Only the canonical form is accepted, so every upload has exactly one
        // ID, and it's never interpreted as a path.
        Uuid::try_parse(s)
            .ok()
            .filter(|id| id.to_string() == s)
            .map(Self)
            .ok_or(InvalidUploadIdError)
    }
}

/// Error of parsing an [`UploadId`] from a [`str`].
#[derive(Debug, Display, Error)]
#[display("Invalid `UploadId` format")]
pub struct InvalidUploadIdError;

/// Error of executing a multipart upload operation.
#[derive(Debug, Display, Error)]
pub enum MultipartUploadError {
    /// Provided [`Md5Digest`] of an uploaded part doesn't match its bytes.
    #[display("Part MD5 digest mismatch")]
    BadDigest,

    /// Part other than the last one is smaller than the [`MIN_PART_SIZE`].
    #[display("Part {_0} is smaller than the minimum allowed size")]
    EntityTooSmall(#[error(not(source))] u16),

    /// Part doesn't exist, or its [`Md5Digest`] doesn't match the provided
    /// one.
    #[display("Part {_0} is missing or has another ETag")]
    InvalidPart(#[error(not(source))] u16),

    /// Parts are not listed in ascending order of their numbers.
    #[display("Parts are not in ascending order")]
    InvalidPartOrder,

    /// Filesystem operation failed.
    #[display("Filesystem operation failed: {_0}")]
    Io(io::Error),

    /// There is no multipart upload with the provided [`UploadId`] for the
    /// provided [`RelativePath`] (it may be completed or aborted already).
    #[display("No such upload")]
    NoSuchUpload,
}

impl From<io::Error> for MultipartUploadError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl Storage {
    /// Returns the absolute [`Path`] to the directory of the multipart upload
    /// with the provided [`UploadId`].
    fn upload_dir(&self, id: UploadId) -> PathBuf {
        self.multipart_dir.join(id.to_string())
    }

    /// Checks that the multipart upload with the provided [`UploadId`] exists
    /// and uploads the file at the provided [`RelativePath`], returning the
    /// absolute [`Path`] to its directory.
    ///
    /// # Errors
    ///
    /// - [`MultipartUploadError::NoSuchUpload`] if there is no such upload.
    /// - [`MultipartUploadError::Io`] if reading its manifest fails.
    async fn existing_upload(
        &self,
        id: UploadId,
        path: &RelativePath,
    ) -> Result<PathBuf, Traced<MultipartUploadError>> {
        let dir = self.upload_dir(id);
        match async_fs::read(dir.join(MANIFEST_FILE)).await {
            Ok(m) if m == path.as_ref().as_os_str().as_encoded_bytes() => {
                Ok(dir)
            }
            Ok(_) => Err(tracerr::new!(MultipartUploadError::NoSuchUpload)),
            Err(e) if is_missing(&e) => {
                Err(tracerr::new!(MultipartUploadError::NoSuchUpload))
            }
            Err(e) => Err(tracerr::new!(MultipartUploadError::Io(e))),
        }
    }

    /// Claims the multipart upload at the provided `dir`ectory exclusively, by
    /// moving it into the [`Storage::tmp_dir`], and returns the [`Path`] it's
    /// moved to.
    ///
    /// Once claimed, the upload doesn't exist for any other operation, so it
    /// cannot be completed, aborted or have parts uploaded concurrently.
    ///
    /// # Errors
    ///
    /// - [`MultipartUploadError::NoSuchUpload`] if the upload is claimed by
    ///   another operation already.
    /// - [`MultipartUploadError::Io`] if moving the upload fails.
    async fn claim_upload(
        &self,
        dir: &Path,
    ) -> Result<PathBuf, Traced<MultipartUploadError>> {
        let claimed = self.tmp_dir.join(Uuid::new_v4().to_string());
        match async_fs::rename(dir, &claimed).await {
            Ok(()) => Ok(claimed),
            Err(e) if is_missing(&e) => {
                Err(tracerr::new!(MultipartUploadError::NoSuchUpload))
            }
            Err(e) => Err(tracerr::new!(MultipartUploadError::Io(e))),
        }
    }
}

/// Operation of a multipart upload creation, returning its [`UploadId`].
///
/// The file at the [`CreateMultipartUpload::path`] is not touched until the
/// upload is [completed](CompleteMultipartUpload).
#[derive(Clone, Debug)]
pub struct CreateMultipartUpload {
    /// [`RelativePath`] of the file to be uploaded.
    pub path: RelativePath,
}

#[async_trait]
impl Exec<CreateMultipartUpload> for Storage {
    type Ok = UploadId;
    type Err = Traced<MultipartUploadError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(
        &self,
        op: CreateMultipartUpload,
    ) -> Result<Self::Ok, Self::Err> {
        let id = UploadId(Uuid::new_v4());

        // The upload directory is fully prepared in the `tmp_dir` first, so
        // other operations never observe an upload without its manifest.
        let tmp = self.tmp_dir.join(Uuid::new_v4().to_string());
        let manifest = op.path.as_ref().as_os_str().as_encoded_bytes().to_vec();
        let res = async {
            async_fs::create_dir(&tmp).await.map_err(tracerr::wrap!())?;
            write_file(
                &tmp.join(MANIFEST_FILE),
                futures::stream::iter([Ok::<_, io::Error>(manifest)]),
            )
            .await?;
            async_fs::rename(&tmp, self.upload_dir(id))
                .await
                .map_err(tracerr::wrap!())
        }
        .await;
        if res.is_err() {
            // Best-effort cleanup, the original error is more important.
            drop(remove_existing_dir(&tmp).await);
        }

        res.map(|()| id).map_err(tracerr::map_from_and_wrap!())
    }
}

/// Operation of uploading a part of a multipart upload, returning the
/// [`Md5Digest`] of the uploaded part.
///
/// The part is written atomically, so a failed or interrupted upload never
/// leaves a partial part. If a part with the same
/// [`UploadPart::part_number`] has been uploaded already, it's replaced.
#[derive(Clone, Debug)]
pub struct UploadPart<Bytes> {
    /// [`UploadId`] of the multipart upload.
    pub upload_id: UploadId,

    /// [`RelativePath`] of the file being uploaded.
    pub path: RelativePath,

    /// Number of the part.
    pub part_number: u16,

    /// [`Stream`] of the part bytes.
    pub bytes: Bytes,

    /// Expected [`Md5Digest`] of the part bytes, if any.
    ///
    /// The part is not stored if its bytes don't match it.
    pub content_md5: Option<Md5Digest>,
}

#[async_trait]
impl<S, Bytes> Exec<UploadPart<S>> for Storage
where
    S: Stream<Item = Result<Bytes, io::Error>> + fmt::Debug + Send + 'static,
    Bytes: AsRef<[u8]> + Send + Sync,
{
    type Ok = Md5Digest;
    type Err = Traced<MultipartUploadError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: UploadPart<S>) -> Result<Self::Ok, Self::Err> {
        let dir = self.existing_upload(op.upload_id, &op.path).await?;

        let tmp = self.tmp_dir.join(Uuid::new_v4().to_string());
        let res = async {
            let md5 = write_part(&tmp, op.bytes)
                .await
                .map_err(tracerr::map_from_and_wrap!())?;
            if op.content_md5.is_some_and(|expected| expected != md5) {
                return Err(tracerr::new!(MultipartUploadError::BadDigest));
            }

            // The upload directory is not created if it's missing, because
            // it means that the upload is completed or aborted concurrently.
            let dest = dir.join(part_file_name(op.part_number));
            match async_fs::rename(&tmp, dest).await {
                Ok(()) => Ok(md5),
                Err(e) if is_missing(&e) => {
                    Err(tracerr::new!(MultipartUploadError::NoSuchUpload))
                }
                Err(e) => Err(tracerr::new!(MultipartUploadError::Io(e))),
            }
        }
        .await;
        if res.is_err() {
            // Best-effort cleanup, the original error is more important.
            drop(async_fs::remove_file(&tmp).await);
        }
        res
    }
}

/// Writes all the provided `bytes` into a new part file (see
/// [`part_file_name()`]) at the provided `path`, syncs it to disk, and returns
/// the [`Md5Digest`] of the `bytes`.
///
/// # Errors
///
/// See [`write_file()`].
async fn write_part<S, Bytes>(
    path: &Path,
    bytes: S,
) -> Result<Md5Digest, Traced<io::Error>>
where
    S: Stream<Item = Result<Bytes, io::Error>>,
    Bytes: AsRef<[u8]>,
{
    let mut f = File::create(path).await.map_err(tracerr::wrap!())?;

    let mut md5 = Md5::new();
    write_stream(&mut f, bytes, |chunk| md5.update(chunk)).await?;
    let md5: Md5Digest = md5.finalize().into();
    f.write_all(&md5).await.map_err(tracerr::wrap!())?;

    sync_file(&mut f).await?;
    Ok(md5)
}

/// Opened part file (see [`part_file_name()`]) of a multipart upload.
#[derive(Debug)]
struct PartFile {
    /// Opened file itself.
    file: StdFile,

    /// Size of the part in bytes (not including its [`Md5Digest`]).
    size: u64,

    /// [`Md5Digest`] of the part bytes.
    md5: Md5Digest,

    /// Last modification time of the part.
    modified: SystemTime,
}

impl PartFile {
    /// Opens the part file at the provided `path`.
    ///
    /// Returns [`None`] if there is no part file at the `path`.
    ///
    /// # Errors
    ///
    /// - If the part file cannot be opened or read.
    /// - If the part file is too short to contain its [`Md5Digest`].
    fn open(path: &Path) -> io::Result<Option<Self>> {
        let mut file = match StdFile::open(path) {
            Ok(f) => f,
            Err(e) if is_missing(&e) => return Ok(None),
            Err(e) => return Err(e),
        };

        let meta = file.metadata()?;
        let size = meta.len().checked_sub(MD5_LEN as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("`{}` part file is corrupted", path.display()),
            )
        })?;
        let mut md5 = Md5Digest::default();
        file.seek(SeekFrom::Start(size))?;
        file.read_exact(&mut md5)?;
        file.seek(SeekFrom::Start(0))?;

        Ok(Some(Self { file, size, md5, modified: meta.modified()? }))
    }
}

/// Part of a multipart upload to be [completed](CompleteMultipartUpload) with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedPart {
    /// Number of the part.
    pub number: u16,

    /// Expected [`Md5Digest`] of the part, as returned by [`UploadPart`].
    pub md5: Md5Digest,
}

/// Operation of a multipart upload completion, returning the [`FileMeta`] of
/// the uploaded file.
///
/// The [`CompleteMultipartUpload::parts`] are joined into a single file, which
/// atomically replaces the existing file at the
/// [`CompleteMultipartUpload::path`] (if any), the same way [`CreateFile`]
/// does. Once completed, the upload (along with all its parts, including the
/// ones not listed in the [`CompleteMultipartUpload::parts`]) is removed.
///
/// If the completion fails, the upload is left intact, so it can be completed
/// again (with the correct [`CompleteMultipartUpload::parts`], for example).
///
/// # Concurrency
///
/// The upload is claimed exclusively for the whole completion, so
/// [`UploadPart`], [`AbortMultipartUpload`] or another
/// [`CompleteMultipartUpload`] of the same upload executed concurrently fail
/// with [`MultipartUploadError::NoSuchUpload`].
#[derive(Clone, Debug)]
pub struct CompleteMultipartUpload {
    /// [`UploadId`] of the multipart upload.
    pub upload_id: UploadId,

    /// [`RelativePath`] of the file being uploaded.
    pub path: RelativePath,

    /// [`CompletedPart`]s to join, in ascending order of their numbers.
    ///
    /// Every part, except the last one, must be at least [`MIN_PART_SIZE`].
    pub parts: Vec<CompletedPart>,
}

#[async_trait]
impl Exec<CompleteMultipartUpload> for Storage {
    type Ok = FileMeta;
    type Err = Traced<MultipartUploadError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(
        &self,
        op: CompleteMultipartUpload,
    ) -> Result<Self::Ok, Self::Err> {
        if !op.parts.is_sorted_by(|a, b| a.number < b.number) {
            return Err(tracerr::new!(MultipartUploadError::InvalidPartOrder));
        }

        let dir = self.existing_upload(op.upload_id, &op.path).await?;
        let claimed = self.claim_upload(&dir).await?;

        let tmp = self.tmp_dir.join(Uuid::new_v4().to_string());
        let dest = self.data_dir.join(op.path);
        let res = async {
            let (src, out) = (claimed.clone(), tmp.clone());
            let meta =
                blocking::unblock(move || join_parts(&src, &op.parts, &out))
                    .await?;
            move_file(&tmp, &dest)
                .await
                .map_err(tracerr::map_from_and_wrap!())?;
            Ok(meta)
        }
        .await;

        if res.is_ok() {
            if let Err(e) = async_fs::remove_dir_all(&claimed).await {
                // It's in the `tmp_dir`, so will be removed on restart anyway.
                tracing::warn!(
                    "Failed to remove completed `{}` upload: {e}",
                    claimed.display(),
                );
            }
        } else {
            // Best-effort cleanup, the original error is more important.
            drop(async_fs::remove_file(&tmp).await);
            if let Err(e) = async_fs::rename(&claimed, &dir).await {
                tracing::error!(
                    "Failed to restore `{}` upload: {e}",
                    dir.display(),
                );
            }
        }
        res
    }
}

/// Joins the provided `parts` of the multipart upload at the provided
/// `dir`ectory into a new file at the provided `dest` path, syncs it to disk,
/// and returns its [`FileMeta`].
///
/// All the `parts` are checked before anything is written.
///
/// # Errors
///
/// - [`MultipartUploadError::InvalidPart`] if any of the `parts` is missing, or
///   has another [`Md5Digest`].
/// - [`MultipartUploadError::EntityTooSmall`] if any of the `parts`, except the
///   last one, is smaller than the [`MIN_PART_SIZE`].
/// - [`MultipartUploadError::Io`] if any filesystem operation fails.
fn join_parts(
    dir: &Path,
    parts: &[CompletedPart],
    dest: &Path,
) -> Result<FileMeta, Traced<MultipartUploadError>> {
    let files = parts
        .iter()
        .map(|part| {
            match PartFile::open(&dir.join(part_file_name(part.number)))
                .map_err(tracerr::from_and_wrap!())?
            {
                Some(f) if f.md5 == part.md5 => Ok(f),
                _ => Err(tracerr::new!(MultipartUploadError::InvalidPart(
                    part.number
                ))),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some((_, part)) = files
        .iter()
        .zip(parts)
        .rev()
        .skip(1)
        .find(|(f, _)| f.size < MIN_PART_SIZE)
    {
        return Err(tracerr::new!(MultipartUploadError::EntityTooSmall(
            part.number
        )));
    }

    let mut out = StdFile::create(dest).map_err(tracerr::from_and_wrap!())?;
    for f in files {
        // `io::copy()` is specialized to copy files in kernel space, where
        // possible.
        _ = io::copy(&mut f.file.take(f.size), &mut out)
            .map_err(tracerr::from_and_wrap!())?;
    }
    out.sync_all().map_err(tracerr::from_and_wrap!())?;

    let meta = out.metadata().map_err(tracerr::from_and_wrap!())?;
    FileMeta::from_metadata(&meta)
        .map_err(tracerr::from_and_wrap!())?
        .ok_or_else(|| {
            tracerr::new!(MultipartUploadError::Io(io::Error::other(
                "joined parts are not a regular file",
            )))
        })
}

/// Operation of a multipart upload abortion, removing all its uploaded parts.
///
/// # Concurrency
///
/// The upload is claimed exclusively before being removed, the same way
/// [`CompleteMultipartUpload`] does, so only one of them succeeds if executed
/// concurrently.
#[derive(Clone, Debug)]
pub struct AbortMultipartUpload {
    /// [`UploadId`] of the multipart upload.
    pub upload_id: UploadId,

    /// [`RelativePath`] of the file being uploaded.
    pub path: RelativePath,
}

#[async_trait]
impl Exec<AbortMultipartUpload> for Storage {
    type Ok = ();
    type Err = Traced<MultipartUploadError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(
        &self,
        op: AbortMultipartUpload,
    ) -> Result<Self::Ok, Self::Err> {
        let dir = self.existing_upload(op.upload_id, &op.path).await?;
        self.remove_upload(&dir).await
    }
}

impl Storage {
    /// [Claims](Storage::claim_upload) the multipart upload at the provided
    /// `dir`ectory and removes it.
    ///
    /// # Errors
    ///
    /// See [`Storage::claim_upload()`].
    async fn remove_upload(
        &self,
        dir: &Path,
    ) -> Result<(), Traced<MultipartUploadError>> {
        let claimed = self.claim_upload(dir).await?;
        if let Err(e) = async_fs::remove_dir_all(&claimed).await {
            // It's in the `tmp_dir`, so will be removed on restart anyway.
            tracing::warn!(
                "Failed to remove aborted `{}` upload: {e}",
                claimed.display(),
            );
        }
        Ok(())
    }
}

/// Operation of listing parts of a multipart upload, in ascending order of
/// their numbers.
#[derive(Clone, Debug)]
pub struct ListParts {
    /// [`UploadId`] of the multipart upload.
    pub upload_id: UploadId,

    /// [`RelativePath`] of the file being uploaded.
    pub path: RelativePath,

    /// Number of the part to list the parts after (exclusively).
    pub part_number_marker: u16,

    /// Maximum number of parts to list.
    pub max_parts: usize,
}

#[async_trait]
impl Exec<ListParts> for Storage {
    type Ok = PartList;
    type Err = Traced<MultipartUploadError>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(&self, op: ListParts) -> Result<Self::Ok, Self::Err> {
        let dir = self.existing_upload(op.upload_id, &op.path).await?;

        blocking::unblock(move || list_parts(&dir, &op)).await
    }
}

/// Lists parts of the multipart upload at the provided `dir`ectory, as
/// described by the provided [`ListParts`] operation.
///
/// # Errors
///
/// - [`MultipartUploadError::NoSuchUpload`] if the upload is completed or
///   aborted concurrently.
/// - [`MultipartUploadError::Io`] if reading the upload directory or any of
///   its parts fails.
fn list_parts(
    dir: &Path,
    op: &ListParts,
) -> Result<PartList, Traced<MultipartUploadError>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if is_missing(&e) => {
            return Err(tracerr::new!(MultipartUploadError::NoSuchUpload));
        }
        Err(e) => return Err(tracerr::new!(MultipartUploadError::Io(e))),
    };
    let mut numbers = vec![];
    for entry in entries {
        let entry = entry.map_err(tracerr::from_and_wrap!())?;
        if let Some(n) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("part-"))
            .and_then(|n| n.parse::<u16>().ok())
            .filter(|n| *n > op.part_number_marker)
        {
            numbers.push(n);
        }
    }
    numbers.sort_unstable();

    let mut list = PartList::default();
    for number in numbers {
        if list.parts.len() >= op.max_parts {
            list.is_truncated = true;
            break;
        }
        let Some(f) = PartFile::open(&dir.join(part_file_name(number)))
            .map_err(tracerr::from_and_wrap!())?
        else {
            // The upload is completed or aborted concurrently.
            return Err(tracerr::new!(MultipartUploadError::NoSuchUpload));
        };
        list.parts.push(ListedPart {
            number,
            md5: f.md5,
            meta: FileMeta { size: f.size, modified: f.modified },
        });
    }
    if list.is_truncated {
        list.next_part_number_marker = list.parts.last().map(|p| p.number);
    }

    Ok(list)
}

/// Result of executing a [`ListParts`] operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PartList {
    /// Listed parts, in ascending order of their numbers.
    pub parts: Vec<ListedPart>,

    /// Indicator whether there are more parts to list after the listed ones,
    /// which haven't been listed due to [`ListParts::max_parts`].
    pub is_truncated: bool,

    /// [`ListParts::part_number_marker`] to list the next parts with, if this
    /// [`PartList`] [is truncated](PartList::is_truncated).
    pub next_part_number_marker: Option<u16>,
}

/// Part listed by a [`ListParts`] operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListedPart {
    /// Number of the part.
    pub number: u16,

    /// [`Md5Digest`] of the part.
    pub md5: Md5Digest,

    /// [`FileMeta`] of the part.
    pub meta: FileMeta,
}

/// Operation of aborting all the multipart uploads created more than the
/// [`AbortExpiredUploads::ttl`] ago, returning the number of aborted uploads.
///
/// Uploads which cannot be checked or removed are skipped with a warning.
#[derive(Clone, Copy, Debug)]
pub struct AbortExpiredUploads {
    /// Maximum age of a multipart upload.
    pub ttl: Duration,
}

#[async_trait]
impl Exec<AbortExpiredUploads> for Storage {
    type Ok = usize;
    type Err = Traced<io::Error>;

    #[allow(clippy::blocks_in_conditions)] // due to `instrument` attribute
    #[tracing::instrument(level = "debug", err(Debug))]
    async fn exec(
        &self,
        op: AbortExpiredUploads,
    ) -> Result<Self::Ok, Self::Err> {
        let dir = self.multipart_dir.clone();
        let expired = blocking::unblock(move || expired_uploads(&dir, op.ttl))
            .await
            .map_err(tracerr::wrap!())?;

        let mut aborted = 0;
        for dir in expired {
            match self.remove_upload(&dir).await {
                Ok(()) => aborted += 1,
                // Completed or aborted concurrently.
                Err(e)
                    if matches!(
                        e.as_ref(),
                        MultipartUploadError::NoSuchUpload,
                    ) => {}
                Err(e) => tracing::warn!(
                    "Failed to abort expired `{}` upload: {e}",
                    dir.display(),
                ),
            }
        }
        Ok(aborted)
    }
}

/// Returns absolute [`Path`]s to the directories of all the multipart uploads
/// in the provided `multipart_dir`, created more than the provided `ttl` ago.
///
/// # Errors
///
/// If reading the `multipart_dir` fails.
fn expired_uploads(
    multipart_dir: &Path,
    ttl: Duration,
) -> io::Result<Vec<PathBuf>> {
    let now = SystemTime::now();

    let mut expired = vec![];
    for entry in fs::read_dir(multipart_dir)? {
        let dir = entry?.path();
        let created = match fs::metadata(dir.join(MANIFEST_FILE))
            .and_then(|m| m.modified())
        {
            Ok(created) => created,
            Err(e) => {
                if !is_missing(&e) {
                    tracing::warn!(
                        "Failed to check `{}` upload: {e}",
                        dir.display(),
                    );
                }
                continue;
            }
        };
        // Uploads created in the future (due to clock changes) are not
        // expired.
        if now.duration_since(created).is_ok_and(|age| age > ttl) {
            expired.push(dir);
        }
    }
    Ok(expired)
}

/// Indicates whether the provided [`io::Error`] means that there is no file at
/// the accessed path.
///
/// [`io::ErrorKind::NotADirectory`] means that some parent of the path is a
/// file, so there is no file at the path either.
fn is_missing(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory)
}

/// Metadata of a file stored in a [`Storage`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileMeta {
    /// Size of the file in bytes.
    pub size: u64,

    /// Last modification time of the file.
    pub modified: SystemTime,
}

impl FileMeta {
    /// Creates a new [`FileMeta`] out of the provided [`fs::Metadata`].
    ///
    /// Returns [`None`] if the [`fs::Metadata`] doesn't describe a regular
    /// file (a directory, for example).
    ///
    /// # Errors
    ///
    /// If the last modification time is not available on this platform.
    fn from_metadata(meta: &fs::Metadata) -> io::Result<Option<Self>> {
        if !meta.is_file() {
            return Ok(None);
        }
        Ok(Some(Self { size: meta.len(), modified: meta.modified()? }))
    }
}

/// Read-only [`File`] along with its [`FileMeta`].
pub struct ReadOnlyFile {
    /// Opened [`File`] itself.
    file: File,

    /// [`FileMeta`] of the opened [`File`].
    meta: FileMeta,
}

impl ReadOnlyFile {
    /// Returns [`FileMeta`] of this [`ReadOnlyFile`].
    #[must_use]
    pub const fn meta(&self) -> &FileMeta {
        &self.meta
    }
}

impl AsyncRead for ReadOnlyFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> task::Poll<io::Result<usize>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncSeek for ReadOnlyFile {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        pos: SeekFrom,
    ) -> task::Poll<io::Result<u64>> {
        Pin::new(&mut self.file).poll_seek(cx, pos)
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
mod get_file_meta_spec {
    use std::{fs, io::SeekFrom};

    use futures::{
        AsyncReadExt as _, AsyncSeekExt as _, executor::block_on, stream,
    };

    use super::{
        CreateFile, CreateSymlink, DeleteFile, Exec as _, FileMeta, GetFile,
        GetFileMeta, Storage,
        spec_util::{Root, path},
    };

    async fn create(storage: &Storage, p: &str, bytes: &[u8]) {
        let bytes = stream::iter([Ok(bytes.to_vec())]);
        storage.exec(CreateFile { path: path(p), bytes }).await.unwrap();
    }

    async fn meta(storage: &Storage, p: &str) -> Option<FileMeta> {
        storage.exec(GetFileMeta { path: path(p) }).await.unwrap()
    }

    #[test]
    fn returns_size_and_modification_time() {
        let root = Root::new();
        let meta = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/dir/key", b"hello").await;

            meta(&storage, "bucket/dir/key").await
        })
        .expect("no `FileMeta` returned");

        let expected =
            fs::metadata(root.0.join("data/bucket/dir/key")).unwrap();
        assert_eq!(meta.size, 5);
        assert_eq!(meta.modified, expected.modified().unwrap());
    }

    #[test]
    fn follows_symlinks() {
        let root = Root::new();
        let (target, link) = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/target", b"hello world").await;
            storage
                .exec(CreateSymlink {
                    src: path("bucket/target"),
                    dest: path("links/link"),
                })
                .await
                .unwrap();

            (
                meta(&storage, "bucket/target").await,
                meta(&storage, "links/link").await,
            )
        });

        assert_eq!(link.expect("no `FileMeta` returned").size, 11);
        assert_eq!(link, target);
    }

    #[test]
    fn returns_none_for_missing_file() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/file", b"bytes").await;

            for p in ["bucket/missing", "missing/key", "bucket/file/key"] {
                assert_eq!(meta(&storage, p).await, None, "`{p}`");
            }
        });
    }

    #[test]
    fn returns_none_for_directory() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/dir/key", b"bytes").await;

            assert_eq!(meta(&storage, "bucket/dir").await, None);
            assert!(
                storage
                    .exec(GetFile { path: path("bucket/dir") })
                    .await
                    .unwrap()
                    .is_none(),
                "directory is returned as a file",
            );
        });
    }

    #[test]
    fn returns_none_for_dangling_symlink() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/target", b"bytes").await;
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

            assert_eq!(meta(&storage, "links/link").await, None);
        });
    }

    #[test]
    fn get_file_is_seekable() {
        let root = Root::new();
        let read = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/key", b"hello world").await;

            let mut file = storage
                .exec(GetFile { path: path("bucket/key") })
                .await
                .unwrap()
                .expect("no `ReadOnlyFile` returned");
            _ = file.seek(SeekFrom::Start(6)).await.unwrap();
            let mut buf = vec![];
            _ = file.read_to_end(&mut buf).await.unwrap();
            buf
        });

        assert_eq!(read, b"world");
    }

    #[test]
    fn matches_meta_of_get_file() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket/key", b"bytes").await;

            let file = storage
                .exec(GetFile { path: path("bucket/key") })
                .await
                .unwrap()
                .expect("no `ReadOnlyFile` returned");

            assert_eq!(Some(*file.meta()), meta(&storage, "bucket/key").await);
        });
    }
}

#[cfg(test)]
mod list_files_spec {
    use std::fs;

    use futures::{executor::block_on, stream};

    use super::{
        CreateFile, CreateSymlink, DeleteFile, Exec as _, FileList, ListFiles,
        Storage,
        spec_util::{Root, path},
    };

    async fn create(storage: &Storage, p: &str, bytes: &[u8]) {
        let bytes = stream::iter([Ok(bytes.to_vec())]);
        storage.exec(CreateFile { path: path(p), bytes }).await.unwrap();
    }

    async fn list(
        storage: &Storage,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> FileList {
        storage
            .exec(ListFiles {
                bucket: path("bucket"),
                prefix: prefix.into(),
                delimiter: delimiter.map(Into::into),
                start_after: start_after.map(Into::into),
                max_keys,
            })
            .await
            .unwrap()
    }

    fn keys(list: &FileList) -> Vec<&str> {
        list.files.iter().map(|f| f.key.as_str()).collect()
    }

    #[test]
    fn lists_keys_in_lexicographic_order() {
        let root = Root::new();
        let list = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            for p in ["a/b", "a-b", "a0", "b/c/d", "a/a", "Z"] {
                create(&storage, &format!("bucket/{p}"), b"bytes").await;
            }
            create(&storage, "other/key", b"bytes").await;

            list(&storage, "", None, None, 1000).await
        });

        assert_eq!(keys(&list), ["Z", "a-b", "a/a", "a/b", "a0", "b/c/d"]);
        assert!(list.common_prefixes.is_empty());
        assert!(!list.is_truncated);
        assert_eq!(list.next_start_after, None);
        assert!(list.files.iter().all(|f| f.meta.size == 5));
    }

    #[test]
    fn filters_by_prefix() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            for p in ["a/b", "a-b", "ab/c", "abc", "b"] {
                create(&storage, &format!("bucket/{p}"), b"bytes").await;
            }

            let l = list(&storage, "a", None, None, 1000).await;
            assert_eq!(keys(&l), ["a-b", "a/b", "ab/c", "abc"]);

            let l = list(&storage, "ab", None, None, 1000).await;
            assert_eq!(keys(&l), ["ab/c", "abc"]);

            let l = list(&storage, "a/", None, None, 1000).await;
            assert_eq!(keys(&l), ["a/b"]);

            let l = list(&storage, "missing", None, None, 1000).await;
            assert_eq!(l, FileList::default());
        });
    }

    #[test]
    fn groups_by_delimiter() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            for p in ["a/b/c", "a/b/d", "a/e", "a-b", "f"] {
                create(&storage, &format!("bucket/{p}"), b"bytes").await;
            }
            fs::create_dir_all(root.0.join("data/bucket/empty/dir")).unwrap();

            let l = list(&storage, "", Some("/"), None, 1000).await;
            assert_eq!(keys(&l), ["a-b", "f"]);
            assert_eq!(l.common_prefixes, ["a/"]);

            let l = list(&storage, "a/", Some("/"), None, 1000).await;
            assert_eq!(keys(&l), ["a/e"]);
            assert_eq!(l.common_prefixes, ["a/b/"]);

            let l = list(&storage, "a", Some("-"), None, 1000).await;
            assert_eq!(keys(&l), ["a/b/c", "a/b/d", "a/e"]);
            assert_eq!(l.common_prefixes, ["a-"]);

            let l = list(&storage, "", Some(""), None, 1000).await;
            assert_eq!(keys(&l).len(), 5);
            assert!(l.common_prefixes.is_empty());
        });
    }

    #[test]
    fn paginates() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            for p in ["a/1", "a/2", "b", "c/1", "d"] {
                create(&storage, &format!("bucket/{p}"), b"bytes").await;
            }

            let (mut files, mut prefixes, mut after) = (vec![], vec![], None);
            let mut pages = 0;
            loop {
                let l =
                    list(&storage, "", Some("/"), after.as_deref(), 2).await;
                pages += 1;
                files.extend(l.files.into_iter().map(|f| f.key));
                prefixes.extend(l.common_prefixes);
                if !l.is_truncated {
                    assert_eq!(l.next_start_after, None);
                    break;
                }
                after =
                    Some(l.next_start_after.expect("no `next_start_after`"));
            }

            assert_eq!(pages, 2);
            assert_eq!(files, ["b", "d"]);
            assert_eq!(prefixes, ["a/", "c/"]);

            let l = list(&storage, "", None, Some("a/1"), 2).await;
            assert_eq!(keys(&l), ["a/2", "b"]);
            assert!(l.is_truncated);
            assert_eq!(l.next_start_after.as_deref(), Some("b"));

            let l = list(&storage, "", None, Some("b"), 3).await;
            assert_eq!(keys(&l), ["c/1", "d"]);
            assert!(!l.is_truncated, "exactly `max_keys` remain");

            let l = list(&storage, "", None, None, 0).await;
            assert!(l.files.is_empty());
            assert!(l.is_truncated);
        });
    }

    #[test]
    fn lists_symlinks_with_target_meta() {
        let root = Root::new();
        let list = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "other/target", b"hello world").await;
            create(&storage, "other/gone", b"bytes").await;
            create(&storage, "other/dir/file", b"bytes").await;
            for (src, dest) in [
                ("other/target", "bucket/link"),
                ("other/gone", "bucket/dangling"),
                ("other/dir", "bucket/dir-link"),
            ] {
                storage
                    .exec(CreateSymlink { src: path(src), dest: path(dest) })
                    .await
                    .unwrap();
            }
            storage
                .exec(DeleteFile { path: path("other/gone") })
                .await
                .unwrap();

            list(&storage, "", Some("/"), None, 1000).await
        });

        assert_eq!(keys(&list), ["link"]);
        assert_eq!(list.files[0].meta.size, 11);
        assert!(list.common_prefixes.is_empty());
    }

    #[test]
    fn returns_empty_list_for_missing_bucket() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            create(&storage, "bucket-other/key", b"bytes").await;

            assert_eq!(
                list(&storage, "", None, None, 1000).await,
                FileList::default(),
            );

            fs::create_dir(root.0.join("data/bucket")).unwrap();
            assert_eq!(
                list(&storage, "", Some("/"), None, 1000).await,
                FileList::default(),
            );
        });
    }
}

#[cfg(test)]
mod multipart_spec {
    use std::{
        fs,
        time::{Duration, SystemTime},
    };

    use futures::{executor::block_on, future, stream};
    use md5::{Digest as _, Md5};

    use super::{
        AbortExpiredUploads, AbortMultipartUpload, CompleteMultipartUpload,
        CompletedPart, CreateMultipartUpload, Exec as _, ListParts,
        MANIFEST_FILE, MIN_PART_SIZE, Md5Digest, MultipartUploadError, Storage,
        UploadId, UploadPart,
        spec_util::{Root, path, tmp_is_empty},
    };

    async fn create(storage: &Storage, p: &str) -> UploadId {
        storage.exec(CreateMultipartUpload { path: path(p) }).await.unwrap()
    }

    async fn upload(
        storage: &Storage,
        id: UploadId,
        p: &str,
        number: u16,
        bytes: &[u8],
    ) -> Result<Md5Digest, MultipartUploadError> {
        storage
            .exec(UploadPart {
                upload_id: id,
                path: path(p),
                part_number: number,
                bytes: stream::iter([Ok(bytes.to_vec())]),
                content_md5: None,
            })
            .await
            .map_err(tracerr::Traced::into_inner)
    }

    async fn complete(
        storage: &Storage,
        id: UploadId,
        p: &str,
        parts: &[(u16, Md5Digest)],
    ) -> Result<super::FileMeta, MultipartUploadError> {
        storage
            .exec(CompleteMultipartUpload {
                upload_id: id,
                path: path(p),
                parts: parts
                    .iter()
                    .map(|&(number, md5)| CompletedPart { number, md5 })
                    .collect(),
            })
            .await
            .map_err(tracerr::Traced::into_inner)
    }

    fn md5(bytes: &[u8]) -> Md5Digest {
        Md5::digest(bytes).into()
    }

    fn part(fill: u8, size: u64) -> Vec<u8> {
        vec![fill; usize::try_from(size).unwrap()]
    }

    fn uploads_count(root: &Root) -> usize {
        fs::read_dir(root.0.join("multipart")).unwrap().count()
    }

    #[test]
    fn joins_parts_on_completion() {
        let root = Root::new();
        let (p1, p2, p3) =
            (part(1, MIN_PART_SIZE), part(2, MIN_PART_SIZE + 1), part(3, 10));
        let meta = block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/dir/key").await;

            // Parts may be uploaded in any order.
            let e3 = upload(&storage, id, "bucket/dir/key", 3, &p3).await;
            let e1 = upload(&storage, id, "bucket/dir/key", 1, &p1).await;
            let e2 = upload(&storage, id, "bucket/dir/key", 2, &p2).await;
            assert_eq!(e1.as_ref().ok(), Some(&md5(&p1)));
            assert!(
                !root.0.join("data/bucket/dir/key").exists(),
                "file is created before completion",
            );

            complete(
                &storage,
                id,
                "bucket/dir/key",
                &[(1, e1.unwrap()), (2, e2.unwrap()), (3, e3.unwrap())],
            )
            .await
            .unwrap()
        });

        let stored = fs::read(root.0.join("data/bucket/dir/key")).unwrap();
        assert!(stored == [p1, p2, p3].concat(), "bytes don't match");
        assert_eq!(meta.size, u64::try_from(stored.len()).unwrap());
        assert_eq!(
            meta.modified,
            fs::metadata(root.0.join("data/bucket/dir/key"))
                .unwrap()
                .modified()
                .unwrap(),
        );
        assert_eq!(uploads_count(&root), 0, "upload is not removed");
        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }

    #[test]
    fn replaces_existing_file_and_part() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            fs::create_dir_all(root.0.join("data/bucket")).unwrap();
            fs::write(root.0.join("data/bucket/key"), b"original").unwrap();

            let id = create(&storage, "bucket/key").await;
            upload(&storage, id, "bucket/key", 1, b"first").await.unwrap();
            let e = upload(&storage, id, "bucket/key", 1, b"second").await;

            complete(&storage, id, "bucket/key", &[(1, e.unwrap())])
                .await
                .unwrap();
        });

        assert_eq!(
            fs::read(root.0.join("data/bucket/key")).unwrap(),
            b"second"
        );
    }

    #[test]
    fn skips_unlisted_parts() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;
            let e1 =
                upload(&storage, id, "bucket/key", 1, &part(1, MIN_PART_SIZE))
                    .await
                    .unwrap();
            upload(&storage, id, "bucket/key", 2, b"skipped").await.unwrap();
            let e3 = upload(&storage, id, "bucket/key", 3, b"last").await;

            complete(&storage, id, "bucket/key", &[(1, e1), (3, e3.unwrap())])
                .await
                .unwrap();
        });

        let stored = fs::read(root.0.join("data/bucket/key")).unwrap();
        assert!(stored == [part(1, MIN_PART_SIZE), b"last".to_vec()].concat());
        assert_eq!(uploads_count(&root), 0, "upload is not removed");
    }

    #[test]
    fn rejects_invalid_completions_keeping_upload() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;
            let e1 = upload(&storage, id, "bucket/key", 1, b"small").await;
            let e2 = upload(&storage, id, "bucket/key", 2, b"last").await;
            let (e1, e2) = (e1.unwrap(), e2.unwrap());

            for (parts, expected) in [
                (vec![(2, e2), (1, e1)], "InvalidPartOrder"),
                (vec![(1, e1), (1, e1)], "InvalidPartOrder"),
                (vec![(1, e1), (3, e2)], "InvalidPart(3)"),
                (vec![(1, e2)], "InvalidPart(1)"),
                (vec![(1, e1), (2, e2)], "EntityTooSmall(1)"),
            ] {
                let res = complete(&storage, id, "bucket/key", &parts).await;
                assert_eq!(
                    format!("{:?}", res.unwrap_err()),
                    expected,
                    "{parts:?}",
                );
            }
            assert!(!root.0.join("data/bucket").exists(), "file is created");
            assert_eq!(uploads_count(&root), 1, "upload is removed");
            assert!(tmp_is_empty(&root), "`tmp/` is not empty");

            // Single part may be of any size.
            complete(&storage, id, "bucket/key", &[(2, e2)]).await.unwrap();
        });

        assert_eq!(fs::read(root.0.join("data/bucket/key")).unwrap(), b"last");
    }

    #[test]
    fn rejects_part_with_bad_digest() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;

            let res = storage
                .exec(UploadPart {
                    upload_id: id,
                    path: path("bucket/key"),
                    part_number: 1,
                    bytes: stream::iter([Ok(b"bytes".to_vec())]),
                    content_md5: Some(md5(b"other")),
                })
                .await
                .map_err(tracerr::Traced::into_inner);
            assert!(
                matches!(res, Err(MultipartUploadError::BadDigest)),
                "{res:?}",
            );

            let list = storage
                .exec(ListParts {
                    upload_id: id,
                    path: path("bucket/key"),
                    part_number_marker: 0,
                    max_parts: 1000,
                })
                .await
                .unwrap();
            assert!(list.parts.is_empty(), "part is stored");

            let res = storage
                .exec(UploadPart {
                    upload_id: id,
                    path: path("bucket/key"),
                    part_number: 1,
                    bytes: stream::iter([Ok(b"bytes".to_vec())]),
                    content_md5: Some(md5(b"bytes")),
                })
                .await;
            assert!(res.is_ok(), "{res:?}");
        });

        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }

    #[test]
    fn aborts_upload() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;
            let e1 = upload(&storage, id, "bucket/key", 1, b"bytes").await;

            storage
                .exec(AbortMultipartUpload {
                    upload_id: id,
                    path: path("bucket/key"),
                })
                .await
                .unwrap();
            assert_eq!(uploads_count(&root), 0, "upload is not removed");

            let res = upload(&storage, id, "bucket/key", 2, b"bytes").await;
            assert!(matches!(res, Err(MultipartUploadError::NoSuchUpload)));
            let res =
                complete(&storage, id, "bucket/key", &[(1, e1.unwrap())]).await;
            assert!(matches!(res, Err(MultipartUploadError::NoSuchUpload)));
            let res = storage
                .exec(AbortMultipartUpload {
                    upload_id: id,
                    path: path("bucket/key"),
                })
                .await
                .map_err(tracerr::Traced::into_inner);
            assert!(matches!(res, Err(MultipartUploadError::NoSuchUpload)));
        });

        assert!(!root.0.join("data/bucket").exists(), "file is created");
        assert!(tmp_is_empty(&root), "`tmp/` is not empty");
    }

    #[test]
    fn rejects_another_path() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;

            for p in ["bucket/other", "other/key", "bucket/key/sub"] {
                let res = upload(&storage, id, p, 1, b"bytes").await;
                assert!(
                    matches!(res, Err(MultipartUploadError::NoSuchUpload)),
                    "`{p}`: {res:?}",
                );
            }

            let unknown = "00000000-0000-4000-8000-000000000000".parse();
            let res =
                upload(&storage, unknown.unwrap(), "bucket/key", 1, b"").await;
            assert!(matches!(res, Err(MultipartUploadError::NoSuchUpload)));
        });
    }

    #[test]
    fn lists_parts() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;
            for n in [3, 1, 10, 2] {
                upload(&storage, id, "bucket/key", n, &part(1, n.into()))
                    .await
                    .unwrap();
            }

            let list = |marker, max_parts| {
                storage.exec(ListParts {
                    upload_id: id,
                    path: path("bucket/key"),
                    part_number_marker: marker,
                    max_parts,
                })
            };

            let l = list(0, 1000).await.unwrap();
            let numbers = l.parts.iter().map(|p| p.number).collect::<Vec<_>>();
            assert_eq!(numbers, [1, 2, 3, 10]);
            assert!(!l.is_truncated);
            assert_eq!(l.next_part_number_marker, None);
            for p in &l.parts {
                assert_eq!(p.meta.size, u64::from(p.number));
                assert_eq!(p.md5, md5(&part(1, p.number.into())));
            }

            let l = list(1, 2).await.unwrap();
            let numbers = l.parts.iter().map(|p| p.number).collect::<Vec<_>>();
            assert_eq!(numbers, [2, 3]);
            assert!(l.is_truncated);
            assert_eq!(l.next_part_number_marker, Some(3));

            let l = list(3, 1).await.unwrap();
            assert_eq!(l.parts.len(), 1);
            assert!(!l.is_truncated, "exactly `max_parts` remain");
        });
    }

    #[test]
    fn aborts_expired_uploads() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let old = create(&storage, "bucket/old").await;
            let new = create(&storage, "bucket/new").await;
            upload(&storage, old, "bucket/old", 1, b"bytes").await.unwrap();

            let manifest =
                root.0.join(format!("multipart/{old}/{MANIFEST_FILE}"));
            fs::File::options()
                .write(true)
                .open(manifest)
                .unwrap()
                .set_modified(SystemTime::now() - Duration::from_secs(7200))
                .unwrap();

            let ttl = Duration::from_secs(3600);
            let aborted =
                storage.exec(AbortExpiredUploads { ttl }).await.unwrap();
            assert_eq!(aborted, 1);
            assert!(!root.0.join(format!("multipart/{old}")).exists());
            assert!(root.0.join(format!("multipart/{new}")).exists());

            let aborted =
                storage.exec(AbortExpiredUploads { ttl }).await.unwrap();
            assert_eq!(aborted, 0);
        });
    }

    #[test]
    fn survives_restart() {
        let root = Root::new();
        block_on(async {
            let storage = Storage::new(&root.0).await.unwrap();
            let id = create(&storage, "bucket/key").await;
            let e1 = upload(&storage, id, "bucket/key", 1, b"bytes").await;

            let storage = Storage::new(&root.0).await.unwrap();
            complete(&storage, id, "bucket/key", &[(1, e1.unwrap())])
                .await
                .unwrap();
        });

        assert_eq!(fs::read(root.0.join("data/bucket/key")).unwrap(), b"bytes");
    }

    #[test]
    fn completes_or_aborts_concurrently() {
        for _ in 0..20 {
            let root = Root::new();
            let (completed, aborted) = block_on(async {
                let storage = Storage::new(&root.0).await.unwrap();
                let id = create(&storage, "bucket/key").await;
                let e1 = upload(&storage, id, "bucket/key", 1, b"bytes").await;

                let abort = storage.exec(AbortMultipartUpload {
                    upload_id: id,
                    path: path("bucket/key"),
                });
                let parts = [(1, e1.unwrap())];
                let complete = complete(&storage, id, "bucket/key", &parts);
                let (completed, aborted) = future::join(complete, abort).await;
                (completed.is_ok(), aborted.is_ok())
            });

            assert!(completed != aborted, "exactly one must succeed");
            assert_eq!(root.0.join("data/bucket/key").exists(), completed);
            if completed {
                assert_eq!(
                    fs::read(root.0.join("data/bucket/key")).unwrap(),
                    b"bytes",
                );
            }
            assert_eq!(uploads_count(&root), 0, "upload is not removed");
            assert!(tmp_is_empty(&root), "`tmp/` is not empty");
        }
    }

    #[test]
    fn parses_only_canonical_upload_ids() {
        let id = "67e55044-10b1-426f-9247-bb680e5fe0c8";
        assert_eq!(id.parse::<UploadId>().unwrap().to_string(), id);

        for id in [
            "",
            "67e5504410b1426f9247bb680e5fe0c8",
            "67E55044-10B1-426F-9247-BB680E5FE0C8",
            "{67e55044-10b1-426f-9247-bb680e5fe0c8}",
            "../67e55044-10b1-426f-9247-bb680e5fe0c8",
        ] {
            assert!(id.parse::<UploadId>().is_err(), "`{id}`");
        }
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
