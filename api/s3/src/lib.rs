//! S3 HTTP API implementation of Baza.

use std::{
    error, fmt, io,
    net::{self, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
};

use baza::{
    CreateFile, CreateSymlink, DeleteFile, DeleteFileError, Exec, FileList,
    FileMeta, GetFile, GetFileMeta, ListFiles, ReadOnlyFile, RelativePath,
    async_trait,
    futures::{Stream, StreamExt as _},
};
use derive_more::with_trait::{Display, Error, From};
use hyper::body::Bytes;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use s3s::{
    S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    auth::SimpleAuth,
    dto::{self, ETag, StreamingBlob, Timestamp},
    s3_error,
    service::S3ServiceBuilder,
};
use secrecy::{ExposeSecret as _, SecretString};
use tokio::net::TcpListener;
use tokio_util::{compat::FuturesAsyncReadCompatExt as _, io::ReaderStream};

/// [`dto::PutObjectInput::metadata`] key where [`CreateSymlink::src`] is
/// expected to be provided.
pub const SYMLINK_META_KEY: &str = "symlink-to";

/// Delay before accepting new connections again, after accepting one has
/// failed (usually because of running out of file descriptors).
const ACCEPT_ERROR_DELAY: Duration = Duration::from_secs(1);

/// Runs [`S3`] HTTP server.
///
/// # Errors
///
/// See [`RunHttpServerError`].
pub async fn run_http_server<S, A>(
    storage: S,
    addr: A,
    access_key: SecretString,
    secret_key: SecretString,
) -> Result<(), RunHttpServerError>
where
    A: ToSocketAddrs,
    S3<S>: s3s::S3,
{
    let mut s3 = S3ServiceBuilder::new(S3(storage));
    s3.set_auth(SimpleAuth::from_single(
        access_key.expose_secret(),
        secret_key.expose_secret(),
    ));
    let service = s3.build();

    let listener = net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(listener)?;
    let http = auto::Builder::new(TokioExecutor::new());

    tracing::info!("Starting S3 HTTP Server");
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::error!("Failed to accept connection: {e}");
                tokio::time::sleep(ACCEPT_ERROR_DELAY).await;
                continue;
            }
        };

        let (http, service) = (http.clone(), service.clone());
        drop(tokio::spawn(async move {
            if let Err(e) =
                http.serve_connection(TokioIo::new(stream), service).await
            {
                tracing::debug!("HTTP connection failed: {e}");
            }
        }));
    }
}

/// Possible error of executing [`run_http_server()`].
#[derive(Debug, Display, Error, From)]
pub enum RunHttpServerError {
    /// Failed to bind address.
    #[display("Failed to bind address: {_0}")]
    BindAddress(io::Error),
}

/// Actual [`s3s::S3`] implementation.
#[derive(Clone, Debug)]
pub struct S3<T>(T);

#[async_trait]
impl<S, E1, E2, E3, E4, E5, E6> s3s::S3 for S3<S>
where
    S: Exec<CreateFile<ObjectBody>, Err = E1>
        + Exec<CreateSymlink, Err = E2>
        + Exec<DeleteFile, Ok = (), Err = E3>
        + Exec<GetFile, Ok = Option<ReadOnlyFile>, Err = E4>
        + Exec<GetFileMeta, Ok = Option<FileMeta>, Err = E5>
        + Exec<ListFiles, Ok = FileList, Err = E6>
        + fmt::Debug
        + Send
        + Sync
        + 'static,
    E1: fmt::Display,
    E2: fmt::Display,
    E3: AsRef<DeleteFileError> + fmt::Display + Send,
    E4: fmt::Display,
    E5: fmt::Display,
    E6: fmt::Display,
{
    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
        ),
    )]
    async fn delete_object(
        &self,
        req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        let input = req.input;
        let path = parse_s3_path(input.bucket, input.key)?;

        self.delete_file(path)
            .await
            .map_err(|e| internal_error("DeleteFile operation failed", e))?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::DeleteObjectOutput::default()))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            count = req.input.delete.objects.len(),
        ),
    )]
    async fn delete_objects(
        &self,
        req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        let input = req.input;
        let quiet = input.delete.quiet.unwrap_or_default();

        let (mut deleted, mut errors) = (vec![], vec![]);
        for obj in input.delete.objects {
            let res = match parse_s3_path(input.bucket.clone(), obj.key.clone())
            {
                Ok(path) => self.delete_file(path).await.map_err(|e| {
                    (
                        S3ErrorCode::InternalError,
                        format!("DeleteFile operation failed: {e}"),
                    )
                }),
                Err(e) => Err((S3ErrorCode::InvalidArgument, e.to_string())),
            };
            match res {
                // Quiet mode reports only errors.
                Ok(()) if quiet => {}
                Ok(()) => deleted.push(dto::DeletedObject {
                    key: Some(obj.key),
                    version_id: obj.version_id,
                    ..dto::DeletedObject::default()
                }),
                Err((code, msg)) => errors.push(dto::Error {
                    code: Some(code.as_str().to_owned()),
                    key: Some(obj.key),
                    message: Some(msg),
                    version_id: obj.version_id,
                }),
            }
        }

        tracing::info!("OK");
        Ok(S3Response::new(dto::DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..dto::DeleteObjectsOutput::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
        ),
    )]
    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let input = req.input;
        let path = parse_s3_path(input.bucket, input.key)?;

        let file = self
            .0
            .exec(GetFile { path })
            .await
            .map_err(|e| internal_error("GetFile operation failed", e))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let headers = ObjectHeaders::try_from(file.meta())?;
        let reader = ReaderStream::new(file.compat());

        tracing::info!("OK");
        Ok(S3Response::new(dto::GetObjectOutput {
            body: Some(StreamingBlob::wrap(reader)),
            content_length: Some(headers.content_length),
            content_type: Some(headers.content_type),
            e_tag: Some(headers.e_tag),
            last_modified: Some(headers.last_modified),
            ..dto::GetObjectOutput::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
        ),
    )]
    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        let input = req.input;
        let path = parse_s3_path(input.bucket, input.key)?;

        let meta = self
            .0
            .exec(GetFileMeta { path })
            .await
            .map_err(|e| internal_error("GetFileMeta operation failed", e))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let headers = ObjectHeaders::try_from(&meta)?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::HeadObjectOutput {
            content_length: Some(headers.content_length),
            content_type: Some(headers.content_type),
            e_tag: Some(headers.e_tag),
            last_modified: Some(headers.last_modified),
            ..dto::HeadObjectOutput::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            delimiter = req.input.delimiter.as_deref(),
            marker = req.input.marker.as_deref(),
            prefix = req.input.prefix.as_deref(),
        ),
    )]
    async fn list_objects(
        &self,
        req: S3Request<dto::ListObjectsInput>,
    ) -> S3Result<S3Response<dto::ListObjectsOutput>> {
        let input = req.input;
        let encoding = KeyEncoding::parse(input.encoding_type.as_ref())?;
        let op = list_files_op(
            input.bucket.clone(),
            input.prefix.clone(),
            input.delimiter.clone(),
            input.marker.clone(),
            input.max_keys,
        )?;
        let max_keys = op.max_keys;

        let list = self
            .0
            .exec(op)
            .await
            .map_err(|e| internal_error("ListFiles operation failed", e))?;
        let (contents, common_prefixes) = list_entries(&list, encoding)?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::ListObjectsOutput {
            common_prefixes: Some(common_prefixes),
            contents: Some(contents),
            delimiter: input.delimiter.map(|d| encoding.encode(&d)),
            encoding_type: input.encoding_type,
            is_truncated: Some(list.is_truncated),
            marker: Some(encoding.encode(&input.marker.unwrap_or_default())),
            max_keys: Some(max_keys.try_into().unwrap_or(i32::MAX)),
            name: Some(input.bucket),
            next_marker: list.next_start_after.map(|m| encoding.encode(&m)),
            prefix: Some(encoding.encode(&input.prefix.unwrap_or_default())),
            ..dto::ListObjectsOutput::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            continuation_token = req.input.continuation_token.as_deref(),
            delimiter = req.input.delimiter.as_deref(),
            prefix = req.input.prefix.as_deref(),
            start_after = req.input.start_after.as_deref(),
        ),
    )]
    async fn list_objects_v2(
        &self,
        req: S3Request<dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<dto::ListObjectsV2Output>> {
        let input = req.input;
        let encoding = KeyEncoding::parse(input.encoding_type.as_ref())?;
        // Continuation token is just the last listed entry, so it always goes
        // after the `start_after` provided along with it in the first request.
        let op = list_files_op(
            input.bucket.clone(),
            input.prefix.clone(),
            input.delimiter.clone(),
            input.continuation_token.clone().or(input.start_after.clone()),
            input.max_keys,
        )?;
        let max_keys = op.max_keys;

        let list = self
            .0
            .exec(op)
            .await
            .map_err(|e| internal_error("ListFiles operation failed", e))?;
        let (contents, common_prefixes) = list_entries(&list, encoding)?;
        let key_count = contents.len() + common_prefixes.len();

        tracing::info!("OK");
        Ok(S3Response::new(dto::ListObjectsV2Output {
            common_prefixes: Some(common_prefixes),
            contents: Some(contents),
            continuation_token: input.continuation_token,
            delimiter: input.delimiter.map(|d| encoding.encode(&d)),
            encoding_type: input.encoding_type,
            is_truncated: Some(list.is_truncated),
            key_count: Some(key_count.try_into().unwrap_or(i32::MAX)),
            max_keys: Some(max_keys.try_into().unwrap_or(i32::MAX)),
            name: Some(input.bucket),
            next_continuation_token: list.next_start_after,
            prefix: Some(encoding.encode(&input.prefix.unwrap_or_default())),
            start_after: input.start_after.map(|s| encoding.encode(&s)),
            ..dto::ListObjectsV2Output::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
        ),
    )]
    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        let input = req.input;
        let path = parse_s3_path(input.bucket, input.key)?;

        if let Some(original) =
            input.metadata.and_then(|mut meta| meta.remove(SYMLINK_META_KEY))
        {
            let op = CreateSymlink {
                src: parse_relative_path(SYMLINK_META_KEY, original)?,
                dest: path,
            };

            self.0
                .exec(op)
                .await
                .map_err(|e| internal_error("Symlink operation failed", e))?;
        } else {
            let op = CreateFile {
                path,
                bytes: ObjectBody(
                    input
                        .body
                        .unwrap_or_else(|| StreamingBlob::from(Bytes::new())),
                ),
            };

            self.0.exec(op).await.map_err(|e| {
                internal_error("CreateFile operation failed", e)
            })?;
        }

        tracing::info!("OK");
        Ok(S3Response::new(dto::PutObjectOutput::default()))
    }
}

impl<S, E> S3<S>
where
    S: Exec<DeleteFile, Ok = (), Err = E>,
    E: AsRef<DeleteFileError> + fmt::Display,
{
    /// Executes the [`DeleteFile`] operation, treating a directory (which is a
    /// prefix of other objects in S3 terms, rather than an object itself) at
    /// the provided `path` as a missing object, so as a success, like S3 does.
    async fn delete_file(&self, path: RelativePath) -> Result<(), E> {
        match self.0.exec(DeleteFile { path }).await {
            Err(e) if matches!(e.as_ref(), DeleteFileError::IsDirectory) => {
                Ok(())
            }
            res => res,
        }
    }
}

/// [`Stream`] of an uploaded object's bytes, as [`CreateFile::bytes`] are
/// expected to be.
///
/// Wraps a [`StreamingBlob`], converting its errors into [`io::Error`]s.
#[derive(Debug)]
pub struct ObjectBody(StreamingBlob);

impl Stream for ObjectBody {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.0
            .poll_next_unpin(cx)
            .map(|chunk| chunk.map(|res| res.map_err(io::Error::other)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// Headers describing an S3 object, shared by [`dto::GetObjectOutput`],
/// [`dto::HeadObjectOutput`] and [`dto::Object`].
#[derive(Debug)]
struct ObjectHeaders {
    /// Size of the object in bytes.
    content_length: i64,

    /// MIME type of the object.
    ///
    /// Always `application/octet-stream`, because it's not stored anywhere.
    content_type: String,

    /// `ETag` of the object.
    ///
    /// It's derived from the size and the last modification time of the
    /// object, rather than being an MD5 digest of its content, so it changes
    /// whenever the object is overwritten, while requiring nothing to be
    /// computed or stored. To not be mistaken for an MD5 digest, it contains a
    /// `-`, as multipart upload `ETag`s do, so clients skip verifying the
    /// content against it.
    e_tag: ETag,

    /// Last modification time of the object.
    last_modified: Timestamp,
}

impl TryFrom<&FileMeta> for ObjectHeaders {
    type Error = S3Error;

    fn try_from(meta: &FileMeta) -> Result<Self, Self::Error> {
        let content_length = i64::try_from(meta.size)
            .map_err(|e| internal_error("Object is too large", e))?;
        // Files modified before UNIX epoch are not expected in practice.
        let mtime = meta
            .modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        Ok(Self {
            content_length,
            content_type: "application/octet-stream".into(),
            e_tag: ETag::Strong(format!("{mtime:x}-{:x}", meta.size)),
            last_modified: meta.modified.into(),
        })
    }
}

/// Default (and maximum) number of keys returned by `ListObjects` and
/// `ListObjectsV2`, as S3 does.
const MAX_KEYS: usize = 1000;

/// Constructs a [`ListFiles`] operation out of the provided `ListObjects` or
/// `ListObjectsV2` request parameters.
///
/// # Errors
///
/// - If the `bucket` is not a valid [`RelativePath`].
/// - If the `max_keys` is negative.
fn list_files_op(
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    start_after: Option<String>,
    max_keys: Option<i32>,
) -> Result<ListFiles, S3Error> {
    let max_keys = max_keys.map_or(Ok(MAX_KEYS), |n| {
        usize::try_from(n).map(|n| n.min(MAX_KEYS)).map_err(|_| {
            s3_error!(InvalidArgument, "`max-keys` cannot be negative")
        })
    })?;

    Ok(ListFiles {
        bucket: parse_relative_path("bucket", bucket)?,
        prefix: prefix.unwrap_or_default(),
        delimiter,
        start_after,
        max_keys,
    })
}

/// Converts the provided [`FileList`] into `Contents` and `CommonPrefixes` of a
/// `ListObjects` or `ListObjectsV2` response, encoding them with the provided
/// [`KeyEncoding`].
///
/// # Errors
///
/// If any of the listed files cannot be represented as [`ObjectHeaders`].
fn list_entries(
    list: &FileList,
    encoding: KeyEncoding,
) -> Result<(Vec<dto::Object>, Vec<dto::CommonPrefix>), S3Error> {
    let contents = list
        .files
        .iter()
        .map(|file| {
            let headers = ObjectHeaders::try_from(&file.meta)?;
            Ok(dto::Object {
                e_tag: Some(headers.e_tag),
                key: Some(encoding.encode(&file.key)),
                last_modified: Some(headers.last_modified),
                size: Some(headers.content_length),
                storage_class: Some(dto::ObjectStorageClass::from_static(
                    dto::ObjectStorageClass::STANDARD,
                )),
                ..dto::Object::default()
            })
        })
        .collect::<Result<_, S3Error>>()?;
    let common_prefixes = list
        .common_prefixes
        .iter()
        .map(|p| dto::CommonPrefix { prefix: Some(encoding.encode(p)) })
        .collect();

    Ok((contents, common_prefixes))
}

/// Encoding of object keys in `ListObjects` and `ListObjectsV2` responses,
/// requested via their `encoding-type` parameter.
#[derive(Clone, Copy, Debug)]
enum KeyEncoding {
    /// Keys are returned as is.
    None,

    /// Keys are URL-encoded (`encoding-type=url`).
    Url,
}

impl KeyEncoding {
    /// Characters to be URL-encoded: everything except unreserved ones (as per
    /// [RFC 3986]) and `/`, like S3 does.
    ///
    /// [RFC 3986]: https://www.rfc-editor.org/rfc/rfc3986#section-2.3
    const URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'/')
        .remove(b'_')
        .remove(b'~');

    /// Parses the provided `encoding-type` request parameter.
    ///
    /// # Errors
    ///
    /// If the `encoding-type` is neither absent, nor `url`.
    fn parse(
        encoding_type: Option<&dto::EncodingType>,
    ) -> Result<Self, S3Error> {
        match encoding_type.map(dto::EncodingType::as_str) {
            None => Ok(Self::None),
            Some(t) if t.eq_ignore_ascii_case(dto::EncodingType::URL) => {
                Ok(Self::Url)
            }
            Some(t) => {
                Err(s3_error!(InvalidArgument, "Invalid `encoding-type`: {t}"))
            }
        }
    }

    /// Encodes the provided object key (or prefix) with this [`KeyEncoding`].
    fn encode(self, key: &str) -> String {
        match self {
            Self::None => key.to_owned(),
            Self::Url => {
                percent_encoding::utf8_percent_encode(key, Self::URL_ENCODE_SET)
                    .to_string()
            }
        }
    }
}

/// Parses the provided `bucket` and `key` into a single [`RelativePath`].
fn parse_s3_path(
    bucket: String,
    key: String,
) -> Result<RelativePath, InvalidArgument> {
    Ok(parse_relative_path("bucket", bucket)?
        .join(parse_relative_path("key", key)?))
}

/// Parses the provided [`String`] into a [`RelativePath`].
fn parse_relative_path(
    attr: &'static str,
    s: String,
) -> Result<RelativePath, InvalidArgument> {
    s.try_into().map_err(|err| InvalidArgument { attr, err: Box::new(err) })
}

/// Invalid [`RelativePath`] provided as the `attr` of a request.
///
/// Converts into a [`S3ErrorCode::InvalidArgument`] [`S3Error`].
#[derive(Debug, Display)]
#[display("Invalid {attr}: {err}")]
struct InvalidArgument {
    /// Name of the invalid request attribute.
    attr: &'static str,

    /// Error of parsing the [`RelativePath`] (usually an
    /// [`InvalidRelativePathError`](baza::InvalidRelativePathError)).
    err: Box<dyn error::Error + Send + Sync>,
}

impl From<InvalidArgument> for S3Error {
    fn from(e: InvalidArgument) -> Self {
        Self::with_message(S3ErrorCode::InvalidArgument, e.to_string())
    }
}

/// Constructs an internal [`S3Error`].
fn internal_error<E: fmt::Display>(msg: &str, e: E) -> S3Error {
    S3Error::with_message(S3ErrorCode::InternalError, format!("{msg}: {e}"))
}
