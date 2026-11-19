//! S3 HTTP API implementation of Baza.

use std::{
    error, fmt,
    io::{self, SeekFrom},
    net::{self, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, UNIX_EPOCH},
};

use baza::{
    AbortMultipartUpload, CompleteMultipartUpload, CompletedPart, CreateFile,
    CreateMultipartUpload, CreateSymlink, DeleteFile, DeleteFileError, Exec,
    FileList, FileMeta, GetFile, GetFileMeta, ListFiles, ListParts, Md5Digest,
    MultipartUploadError, PartList, ReadOnlyFile, RelativePath, UploadId,
    UploadPart, async_trait,
    futures::{AsyncReadExt as _, AsyncSeekExt as _, Stream, StreamExt as _},
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

/// `Accept-Ranges` header value of `GetObject` and `HeadObject` responses.
///
/// `GetObject` supports a single byte range only, as S3 does. `HeadObject`
/// ignores the requested range, returning the metadata of the whole object.
const ACCEPT_RANGES: &str = "bytes";

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
impl<S, E1, E2, E3, E4, E5, E6, E7> s3s::S3 for S3<S>
where
    S: Exec<AbortMultipartUpload, Ok = (), Err = E7>
        + Exec<CompleteMultipartUpload, Ok = FileMeta, Err = E7>
        + Exec<CreateFile<ObjectBody>, Err = E1>
        + Exec<CreateMultipartUpload, Ok = UploadId, Err = E7>
        + Exec<CreateSymlink, Err = E2>
        + Exec<DeleteFile, Ok = (), Err = E3>
        + Exec<GetFile, Ok = Option<ReadOnlyFile>, Err = E4>
        + Exec<GetFileMeta, Ok = Option<FileMeta>, Err = E5>
        + Exec<ListFiles, Ok = FileList, Err = E6>
        + Exec<ListParts, Ok = PartList, Err = E7>
        + Exec<UploadPart<ObjectBody>, Ok = Md5Digest, Err = E7>
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
    E7: AsRef<MultipartUploadError> + fmt::Display,
{
    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
            upload_id = req.input.upload_id.as_str(),
        ),
    )]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::AbortMultipartUploadOutput>> {
        let input = req.input;
        let op = AbortMultipartUpload {
            upload_id: parse_upload_id(&input.upload_id)?,
            path: parse_s3_path(input.bucket, input.key)?,
        };

        self.0.exec(op).await.map_err(|e| {
            multipart_error("AbortMultipartUpload operation failed", e)
        })?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::AbortMultipartUploadOutput::default()))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
            upload_id = req.input.upload_id.as_str(),
        ),
    )]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        let input = req.input;
        let parts = input
            .multipart_upload
            .and_then(|u| u.parts)
            .filter(|parts| !parts.is_empty())
            .ok_or_else(|| {
                s3_error!(MalformedXML, "At least one part must be specified")
            })?
            .into_iter()
            .map(|part| {
                let number = part
                    .part_number
                    .ok_or_else(|| {
                        s3_error!(MalformedXML, "`PartNumber` is missing")
                    })
                    .and_then(parse_part_number)?;
                // A malformed `ETag` can never match the one of a part.
                let md5 = part
                    .e_tag
                    .and_then(|t| parse_md5_hex(t.value()))
                    .ok_or_else(|| {
                        s3_error!(
                            InvalidPart,
                            "Part {number} is missing or has another ETag",
                        )
                    })?;
                Ok(CompletedPart { number, md5 })
            })
            .collect::<Result<_, S3Error>>()?;
        let op = CompleteMultipartUpload {
            upload_id: parse_upload_id(&input.upload_id)?,
            path: parse_s3_path(input.bucket.clone(), input.key.clone())?,
            parts,
        };

        let meta = self.0.exec(op).await.map_err(|e| {
            multipart_error("CompleteMultipartUpload operation failed", e)
        })?;
        let headers = ObjectHeaders::try_from(&meta)?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::CompleteMultipartUploadOutput {
            bucket: Some(input.bucket),
            e_tag: Some(headers.e_tag),
            key: Some(input.key),
            ..dto::CompleteMultipartUploadOutput::default()
        }))
    }

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
        ),
    )]
    async fn create_multipart_upload(
        &self,
        req: S3Request<dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CreateMultipartUploadOutput>> {
        let input = req.input;
        let path = parse_s3_path(input.bucket.clone(), input.key.clone())?;

        let id =
            self.0.exec(CreateMultipartUpload { path }).await.map_err(|e| {
                multipart_error("CreateMultipartUpload operation failed", e)
            })?;

        tracing::info!(upload_id = %id, "OK");
        Ok(S3Response::new(dto::CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(id.to_string()),
            ..dto::CreateMultipartUploadOutput::default()
        }))
    }

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

        let mut file = self
            .0
            .exec(GetFile { path })
            .await
            .map_err(|e| internal_error("GetFile operation failed", e))?
            .ok_or_else(|| s3_error!(NoSuchKey))?;

        let headers = ObjectHeaders::try_from(file.meta())?;
        let size = file.meta().size;
        // Unsatisfiable ranges are `InvalidRange` errors (HTTP 416).
        let range = input.range.map(|r| r.check(size)).transpose()?;
        let content_range = range.as_ref().map(|r| {
            format!("bytes {}-{}/{size}", r.start, r.end.saturating_sub(1))
        });
        let range = range.unwrap_or(0..size);
        if range.start > 0 {
            _ = file.seek(SeekFrom::Start(range.start)).await.map_err(|e| {
                internal_error("Failed to seek to the requested range", e)
            })?;
        }
        let content_length = i64::try_from(range.end - range.start)
            .map_err(|e| internal_error("Range is too large", e))?;
        // Reading is limited to the requested range (or to the size the
        // object had when opened, if it's appended to concurrently).
        let reader =
            ReaderStream::new(file.take(range.end - range.start).compat());

        tracing::info!("OK");
        Ok(S3Response::new(dto::GetObjectOutput {
            accept_ranges: Some(ACCEPT_RANGES.into()),
            body: Some(StreamingBlob::wrap(reader)),
            content_length: Some(content_length),
            content_range,
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
            accept_ranges: Some(ACCEPT_RANGES.into()),
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
            part_number_marker = req.input.part_number_marker,
            upload_id = req.input.upload_id.as_str(),
        ),
    )]
    async fn list_parts(
        &self,
        req: S3Request<dto::ListPartsInput>,
    ) -> S3Result<S3Response<dto::ListPartsOutput>> {
        let input = req.input;
        let max_parts = input.max_parts.map_or(Ok(MAX_PARTS), |n| {
            usize::try_from(n).map(|n| n.min(MAX_PARTS)).map_err(|_| {
                s3_error!(InvalidArgument, "`max-parts` cannot be negative")
            })
        })?;
        let part_number_marker =
            input.part_number_marker.map_or(Ok(0), |n| {
                // Markers after the last possible part number list nothing.
                u16::try_from(n.min(MAX_PART_NUMBER.into())).map_err(|_| {
                    s3_error!(
                        InvalidArgument,
                        "`part-number-marker` cannot be negative",
                    )
                })
            })?;
        let op = ListParts {
            upload_id: parse_upload_id(&input.upload_id)?,
            path: parse_s3_path(input.bucket.clone(), input.key.clone())?,
            part_number_marker,
            max_parts,
        };

        let list =
            self.0.exec(op).await.map_err(|e| {
                multipart_error("ListParts operation failed", e)
            })?;
        let parts = list
            .parts
            .iter()
            .map(|part| {
                let size = i64::try_from(part.meta.size)
                    .map_err(|e| internal_error("Part is too large", e))?;
                Ok(dto::Part {
                    e_tag: Some(ETag::Strong(md5_hex(&part.md5))),
                    last_modified: Some(part.meta.modified.into()),
                    part_number: Some(part.number.into()),
                    size: Some(size),
                    ..dto::Part::default()
                })
            })
            .collect::<Result<_, S3Error>>()?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::ListPartsOutput {
            bucket: Some(input.bucket),
            is_truncated: Some(list.is_truncated),
            key: Some(input.key),
            max_parts: Some(max_parts.try_into().unwrap_or(i32::MAX)),
            next_part_number_marker: list
                .next_part_number_marker
                .map(Into::into),
            part_number_marker: input.part_number_marker,
            parts: Some(parts),
            storage_class: Some(dto::StorageClass::from_static(
                dto::StorageClass::STANDARD,
            )),
            upload_id: Some(input.upload_id),
            ..dto::ListPartsOutput::default()
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

    #[tracing::instrument(
        skip_all,
        fields(
            bucket = req.input.bucket.as_str(),
            key = req.input.key.as_str(),
            part_number = req.input.part_number,
            upload_id = req.input.upload_id.as_str(),
        ),
    )]
    async fn upload_part(
        &self,
        req: S3Request<dto::UploadPartInput>,
    ) -> S3Result<S3Response<dto::UploadPartOutput>> {
        let input = req.input;
        let content_md5 = input
            .content_md5
            .map(|md5| {
                base64_simd::STANDARD
                    .decode_to_vec(&md5)
                    .ok()
                    .and_then(|md5| Md5Digest::try_from(md5).ok())
                    .ok_or_else(|| {
                        s3_error!(InvalidDigest, "Invalid `Content-MD5`")
                    })
            })
            .transpose()?;
        let op = UploadPart {
            upload_id: parse_upload_id(&input.upload_id)?,
            path: parse_s3_path(input.bucket, input.key)?,
            part_number: parse_part_number(input.part_number)?,
            bytes: ObjectBody(
                input.body.unwrap_or_else(|| StreamingBlob::from(Bytes::new())),
            ),
            content_md5,
        };

        let md5 =
            self.0.exec(op).await.map_err(|e| {
                multipart_error("UploadPart operation failed", e)
            })?;

        tracing::info!("OK");
        Ok(S3Response::new(dto::UploadPartOutput {
            e_tag: Some(ETag::Strong(md5_hex(&md5))),
            ..dto::UploadPartOutput::default()
        }))
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

/// Maximum part number of a multipart upload, as S3 allows.
const MAX_PART_NUMBER: u16 = 10_000;

/// Default (and maximum) number of parts returned by `ListParts`, as S3 does.
const MAX_PARTS: usize = 1000;

/// Parses the provided multipart upload ID.
///
/// # Errors
///
/// With [`S3ErrorCode::NoSuchUpload`] if the `id` is not a valid [`UploadId`],
/// because no upload can have it, as S3 does.
fn parse_upload_id(id: &str) -> Result<UploadId, S3Error> {
    id.parse().map_err(|_| s3_error!(NoSuchUpload))
}

/// Parses the provided multipart upload part number.
///
/// # Errors
///
/// If the `number` is not within `1..=`[`MAX_PART_NUMBER`].
fn parse_part_number(number: i32) -> Result<u16, S3Error> {
    u16::try_from(number)
        .ok()
        .filter(|n| (1..=MAX_PART_NUMBER).contains(n))
        .ok_or_else(|| {
            s3_error!(
                InvalidArgument,
                "Part number must be an integer between 1 and \
                 {MAX_PART_NUMBER}, inclusive",
            )
        })
}

/// Formats the provided [`Md5Digest`] as a lowercase hex string, which is the
/// `ETag` of a multipart upload part.
fn md5_hex(md5: &Md5Digest) -> String {
    md5.iter().map(|b| format!("{b:02x}")).collect()
}

/// Parses the provided hex string (in any case) as a [`Md5Digest`].
///
/// Returns [`None`] if it's not a valid one.
fn parse_md5_hex(hex: &str) -> Option<Md5Digest> {
    if hex.len() != 2 * Md5Digest::default().len() || !hex.is_ascii() {
        return None;
    }
    let mut md5 = Md5Digest::default();
    for (b, i) in md5.iter_mut().zip((0..hex.len()).step_by(2)) {
        *b = u8::from_str_radix(&hex[i..i + 2], 16).ok()?;
    }
    Some(md5)
}

/// Converts the provided error of a multipart upload operation into an
/// [`S3Error`] with the matching [`S3ErrorCode`].
fn multipart_error<E>(msg: &str, e: E) -> S3Error
where
    E: AsRef<MultipartUploadError> + fmt::Display,
{
    let code = match e.as_ref() {
        MultipartUploadError::BadDigest => S3ErrorCode::BadDigest,
        MultipartUploadError::EntityTooSmall(_) => S3ErrorCode::EntityTooSmall,
        MultipartUploadError::InvalidPart(_) => S3ErrorCode::InvalidPart,
        MultipartUploadError::InvalidPartOrder => S3ErrorCode::InvalidPartOrder,
        MultipartUploadError::Io(_) => return internal_error(msg, e),
        MultipartUploadError::NoSuchUpload => S3ErrorCode::NoSuchUpload,
    };
    S3Error::with_message(code, e.as_ref().to_string())
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
