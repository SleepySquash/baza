//! S3 HTTP API implementation of Baza.

use std::{
    convert::Infallible,
    error, fmt, io,
    net::{TcpListener, ToSocketAddrs},
    time::UNIX_EPOCH,
};

use baza::{
    CreateFile, CreateSymlink, DeleteFile, DeleteFileError, Exec, FileList,
    FileMeta, GetFile, GetFileMeta, ListFiles, ReadOnlyFile, RelativePath,
    async_trait, futures::future,
};
use chrono::{DateTime, SecondsFormat, Utc};
use derive_more::with_trait::{Display, Error, From};
use hyper::{server::Server, service::make_service_fn};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use s3_server::{
    S3Service, S3Storage, SimpleAuth, dto,
    errors::{S3Error, S3ErrorCode, S3StorageError, S3StorageResult},
};
use secrecy::{ExposeSecret as _, SecretString};
use tokio_util::{compat::FuturesAsyncReadCompatExt as _, io::ReaderStream};

/// [`dto::PutObjectRequest::metadata`] key where [`CreateSymlink::src`] is
/// expected to be provided.
pub const SYMLINK_META_KEY: &str = "symlink-to";

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
    S3<S>: S3Storage + Send + Sync + 'static,
{
    let mut auth = SimpleAuth::new();
    auth.register(
        access_key.expose_secret().to_owned(),
        secret_key.expose_secret().to_owned(),
    );

    let mut s3 = S3Service::new(S3(storage));
    s3.set_auth(auth);

    let service = s3.into_shared();
    let listener = TcpListener::bind(addr)?;
    let make_service =
        make_service_fn(move |_| future::ok::<_, Infallible>(service.clone()));

    tracing::info!("Starting S3 HTTP Server");
    Ok(Server::from_tcp(listener)?.serve(make_service).await?)
}

/// Possible error of executing [`run_http_server()`].
#[derive(Debug, Display, Error, From)]
pub enum RunHttpServerError {
    /// Failed to bind address.
    #[display("Failed to bind address: {_0}")]
    BindAddress(io::Error),

    /// [`hyper`] server failure.
    #[display("Hyper server failed: {_0}")]
    Hyper(hyper::Error),
}

/// Actual [`S3Storage`] implementation.
#[derive(Clone, Debug)]
pub struct S3<T>(T);

#[async_trait]
impl<S, E1, E2, E3, E4, E5, E6> S3Storage for S3<S>
where
    S: Exec<CreateFile<dto::ByteStream>, Err = E1>
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
    async fn complete_multipart_upload(
        &self,
        _: dto::CompleteMultipartUploadRequest,
    ) -> S3StorageResult<
        dto::CompleteMultipartUploadOutput,
        dto::CompleteMultipartUploadError,
    > {
        Err(not_implemented("CompleteMultipartUpload").into())
    }

    async fn copy_object(
        &self,
        _: dto::CopyObjectRequest,
    ) -> S3StorageResult<dto::CopyObjectOutput, dto::CopyObjectError> {
        Err(not_implemented("CopyObject").into())
    }

    async fn create_multipart_upload(
        &self,
        _: dto::CreateMultipartUploadRequest,
    ) -> S3StorageResult<
        dto::CreateMultipartUploadOutput,
        dto::CreateMultipartUploadError,
    > {
        Err(not_implemented("CreateMultipartUpload").into())
    }

    async fn create_bucket(
        &self,
        _: dto::CreateBucketRequest,
    ) -> S3StorageResult<dto::CreateBucketOutput, dto::CreateBucketError> {
        Err(not_implemented("CreateBucket").into())
    }

    async fn delete_bucket(
        &self,
        _: dto::DeleteBucketRequest,
    ) -> S3StorageResult<dto::DeleteBucketOutput, dto::DeleteBucketError> {
        Err(not_implemented("DeleteBucket").into())
    }

    #[tracing::instrument(
        skip_all,
        fields(bucket = input.bucket.as_str(), key = input.key.as_str()),
    )]
    async fn delete_object(
        &self,
        input: dto::DeleteObjectRequest,
    ) -> S3StorageResult<dto::DeleteObjectOutput, dto::DeleteObjectError> {
        let path = parse_uri_s3_path(input.bucket, &input.key)?;

        self.delete_file(path)
            .await
            .map_err(|e| internal_error("DeleteFile operation failed", e))?;

        tracing::info!("OK");
        Ok(dto::DeleteObjectOutput::default())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = input.bucket.as_str(),
            count = input.delete.objects.len(),
        ),
    )]
    async fn delete_objects(
        &self,
        input: dto::DeleteObjectsRequest,
    ) -> S3StorageResult<dto::DeleteObjectsOutput, dto::DeleteObjectsError>
    {
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
                Err((code, msg)) => errors.push(rusoto_s3::S3Error {
                    code: Some(code.as_static_str().to_owned()),
                    key: Some(obj.key),
                    message: Some(msg),
                    version_id: obj.version_id,
                }),
            }
        }

        tracing::info!("OK");
        Ok(dto::DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..dto::DeleteObjectsOutput::default()
        })
    }

    async fn get_bucket_location(
        &self,
        _: dto::GetBucketLocationRequest,
    ) -> S3StorageResult<
        dto::GetBucketLocationOutput,
        dto::GetBucketLocationError,
    > {
        Err(not_implemented("GetBucketLocation").into())
    }

    #[tracing::instrument(
        skip_all,
        fields(bucket = input.bucket.as_str(), key = input.key.as_str()),
    )]
    async fn get_object(
        &self,
        input: dto::GetObjectRequest,
    ) -> S3StorageResult<dto::GetObjectOutput, dto::GetObjectError> {
        let path = parse_uri_s3_path(input.bucket, &input.key)?;

        let file = self
            .0
            .exec(GetFile { path })
            .await
            .map_err(|e| internal_error("GetFile operation failed", e))?
            .ok_or(S3StorageError::Operation(
                dto::GetObjectError::NoSuchKey(input.key),
            ))?;

        let headers = ObjectHeaders::try_from(file.meta())?;
        let reader = ReaderStream::new(file.compat());

        tracing::info!("OK");
        Ok(dto::GetObjectOutput {
            body: Some(dto::ByteStream::new(reader)),
            content_length: Some(headers.content_length),
            content_type: Some(headers.content_type),
            e_tag: Some(headers.e_tag),
            last_modified: Some(headers.last_modified),
            ..dto::GetObjectOutput::default()
        })
    }

    async fn head_bucket(
        &self,
        _: dto::HeadBucketRequest,
    ) -> S3StorageResult<dto::HeadBucketOutput, dto::HeadBucketError> {
        Err(not_implemented("HeadBucket").into())
    }

    #[tracing::instrument(
        skip_all,
        fields(bucket = input.bucket.as_str(), key = input.key.as_str()),
    )]
    async fn head_object(
        &self,
        input: dto::HeadObjectRequest,
    ) -> S3StorageResult<dto::HeadObjectOutput, dto::HeadObjectError> {
        let path = parse_uri_s3_path(input.bucket, &input.key)?;

        let meta = self
            .0
            .exec(GetFileMeta { path })
            .await
            .map_err(|e| internal_error("GetFileMeta operation failed", e))?
            .ok_or(S3StorageError::Operation(
                dto::HeadObjectError::NoSuchKey(input.key),
            ))?;

        let headers = ObjectHeaders::try_from(&meta)?;

        tracing::info!("OK");
        Ok(dto::HeadObjectOutput {
            content_length: Some(headers.content_length),
            content_type: Some(headers.content_type),
            e_tag: Some(headers.e_tag),
            last_modified: Some(headers.last_modified),
            ..dto::HeadObjectOutput::default()
        })
    }

    async fn list_buckets(
        &self,
        _: dto::ListBucketsRequest,
    ) -> S3StorageResult<dto::ListBucketsOutput, dto::ListBucketsError> {
        Err(not_implemented("ListBuckets").into())
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = input.bucket.as_str(),
            delimiter = input.delimiter.as_deref(),
            marker = input.marker.as_deref(),
            prefix = input.prefix.as_deref(),
        ),
    )]
    async fn list_objects(
        &self,
        input: dto::ListObjectsRequest,
    ) -> S3StorageResult<dto::ListObjectsOutput, dto::ListObjectsError> {
        let encoding = KeyEncoding::parse(input.encoding_type.as_deref())?;
        let op = list_files_op(
            input.bucket.clone(),
            input.prefix.clone(),
            input.delimiter.clone(),
            input.marker.clone(),
            input.max_keys,
        )?;
        let max_keys = op.max_keys;

        let mut list = self
            .0
            .exec(op)
            .await
            .map_err(|e| internal_error("ListFiles operation failed", e))?;
        one_common_prefix_per_page(&mut list);
        let (contents, common_prefixes) = list_entries(&list, encoding)?;

        tracing::info!("OK");
        Ok(dto::ListObjectsOutput {
            // `s3_server` writes an empty `CommonPrefixes` element otherwise,
            // which clients parse as a single common prefix without a value.
            common_prefixes: (!common_prefixes.is_empty())
                .then_some(common_prefixes),
            contents: Some(contents),
            delimiter: input.delimiter.map(|d| encoding.encode(&d)),
            encoding_type: input.encoding_type,
            is_truncated: Some(list.is_truncated),
            marker: Some(encoding.encode(&input.marker.unwrap_or_default())),
            max_keys: Some(max_keys.try_into().unwrap_or(i64::MAX)),
            name: Some(input.bucket),
            next_marker: list.next_start_after.map(|m| encoding.encode(&m)),
            prefix: Some(encoding.encode(&input.prefix.unwrap_or_default())),
        })
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = input.bucket.as_str(),
            continuation_token = input.continuation_token.as_deref(),
            delimiter = input.delimiter.as_deref(),
            prefix = input.prefix.as_deref(),
            start_after = input.start_after.as_deref(),
        ),
    )]
    async fn list_objects_v2(
        &self,
        input: dto::ListObjectsV2Request,
    ) -> S3StorageResult<dto::ListObjectsV2Output, dto::ListObjectsV2Error>
    {
        let encoding = KeyEncoding::parse(input.encoding_type.as_deref())?;
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

        let mut list = self
            .0
            .exec(op)
            .await
            .map_err(|e| internal_error("ListFiles operation failed", e))?;
        one_common_prefix_per_page(&mut list);
        let (contents, common_prefixes) = list_entries(&list, encoding)?;
        // TODO: `s3_server` writes `max_keys` as `KeyCount`, fix it once
        //       migrated to `s3s`.
        let key_count = contents.len() + common_prefixes.len();

        tracing::info!("OK");
        Ok(dto::ListObjectsV2Output {
            // `s3_server` writes an empty `CommonPrefixes` element otherwise,
            // which clients parse as a single common prefix without a value.
            common_prefixes: (!common_prefixes.is_empty())
                .then_some(common_prefixes),
            contents: Some(contents),
            continuation_token: input.continuation_token,
            delimiter: input.delimiter.map(|d| encoding.encode(&d)),
            encoding_type: input.encoding_type,
            is_truncated: Some(list.is_truncated),
            key_count: Some(key_count.try_into().unwrap_or(i64::MAX)),
            max_keys: Some(max_keys.try_into().unwrap_or(i64::MAX)),
            name: Some(input.bucket),
            next_continuation_token: list.next_start_after,
            prefix: Some(encoding.encode(&input.prefix.unwrap_or_default())),
            start_after: input.start_after.map(|s| encoding.encode(&s)),
        })
    }

    #[tracing::instrument(
       skip_all,
       fields(bucket = input.bucket.as_str(), key = input.key.as_str()),
    )]
    async fn put_object(
        &self,
        input: dto::PutObjectRequest,
    ) -> S3StorageResult<dto::PutObjectOutput, dto::PutObjectError> {
        let path = parse_uri_s3_path(input.bucket, &input.key)?;

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
                bytes: input.body.unwrap_or_else(|| vec![].into()),
            };

            self.0.exec(op).await.map_err(|e| {
                internal_error("CreateFile operation failed", e)
            })?;
        };

        tracing::info!("OK");
        Ok(dto::PutObjectOutput::default())
    }

    async fn upload_part(
        &self,
        _: dto::UploadPartRequest,
    ) -> S3StorageResult<dto::UploadPartOutput, dto::UploadPartError> {
        Err(not_implemented("UploadPart").into())
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
    e_tag: String,

    /// Last modification time of the object, in [RFC 3339] format with
    /// milliseconds and `Z` suffix (like `2009-10-12T17:50:30.000Z`), as
    /// [`s3_server`] expects and S3 returns in object listings.
    ///
    /// [RFC 3339]: https://www.rfc-editor.org/rfc/rfc3339
    last_modified: String,
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
            e_tag: format!("\"{mtime:x}-{:x}\"", meta.size),
            last_modified: DateTime::<Utc>::from(meta.modified)
                .to_rfc3339_opts(SecondsFormat::Millis, true),
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
    max_keys: Option<i64>,
) -> Result<ListFiles, S3Error> {
    let max_keys = max_keys.map_or(Ok(MAX_KEYS), |n| {
        usize::try_from(n).map(|n| n.min(MAX_KEYS)).map_err(|_| {
            S3Error::new(
                S3ErrorCode::InvalidArgument,
                "`max-keys` cannot be negative",
            )
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

/// Truncates the provided [`FileList`] right before its second common prefix
/// (if any), so it contains at most one.
///
/// [`s3_server`] serializes all the `CommonPrefixes` into a single XML element,
/// rather than one element per common prefix, as S3 does, so clients see only
/// one of them. Listing at most one common prefix per page keeps listings
/// correct, just split into more pages, because S3 allows returning fewer
/// entries than `max-keys`.
// TODO: Remove once migrated to `s3s`.
fn one_common_prefix_per_page(list: &mut FileList) {
    if list.common_prefixes.len() < 2 {
        return;
    }
    let second = list.common_prefixes.swap_remove(1);
    list.common_prefixes.truncate(1);

    // Both files and common prefixes are sorted, so the files listed before
    // the second common prefix are the ones less than it.
    let len = list.files.partition_point(|f| f.key < second);
    list.files.truncate(len);

    list.is_truncated = true;
    list.next_start_after = list
        .files
        .last()
        .map(|f| &f.key)
        .max(list.common_prefixes.first())
        .cloned();
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
) -> Result<(Vec<dto::Object>, Vec<rusoto_s3::CommonPrefix>), S3Error> {
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
                storage_class: Some("STANDARD".into()),
                ..dto::Object::default()
            })
        })
        .collect::<Result<_, S3Error>>()?;
    let common_prefixes = list
        .common_prefixes
        .iter()
        .map(|p| rusoto_s3::CommonPrefix { prefix: Some(encoding.encode(p)) })
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
    fn parse(encoding_type: Option<&str>) -> Result<Self, S3Error> {
        match encoding_type {
            None => Ok(Self::None),
            Some(t) if t.eq_ignore_ascii_case("url") => Ok(Self::Url),
            Some(t) => Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("Invalid `encoding-type`: {t}"),
            )),
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

/// Parses the provided `bucket` and percent-encoded `key` (as they're taken
/// from a request URI path) into a single [`RelativePath`].
///
/// [`s3_server`] doesn't decode object keys taken from request URI paths, so
/// they're decoded here.
fn parse_uri_s3_path(
    bucket: String,
    key: &str,
) -> Result<RelativePath, InvalidArgument> {
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|e| InvalidArgument { attr: "key", err: Box::new(e) })?;
    parse_s3_path(bucket, key.into_owned())
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

/// Invalid [`RelativePath`] (or a key not decodable into one) provided as the
/// `attr` of a request.
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
        Self::new(S3ErrorCode::InvalidArgument, e.to_string())
    }
}

impl<E> From<InvalidArgument> for S3StorageError<E> {
    fn from(e: InvalidArgument) -> Self {
        S3Error::from(e).into()
    }
}

/// Constructs a [`S3ErrorCode::NotImplemented`] [`S3Error`] for the provided
/// unsupported S3 `operation`.
fn not_implemented(operation: &str) -> S3Error {
    S3Error::new(
        S3ErrorCode::NotImplemented,
        format!("`{operation}` operation is not implemented"),
    )
}

/// Constructs an internal [`S3Error`].
fn internal_error<E: fmt::Display>(msg: &str, e: E) -> S3Error {
    S3Error::new(S3ErrorCode::InternalError, format!("{msg}: {e}"))
}
