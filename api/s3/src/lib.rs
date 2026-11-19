//! S3 HTTP API implementation of Baza.

use std::{
    convert::Infallible,
    fmt, io,
    net::{TcpListener, ToSocketAddrs},
};

use baza::{
    CreateFile, CreateSymlink, DeleteFile, DeleteFileError, Exec, GetFile,
    InvalidRelativePathError, ReadOnlyFile, RelativePath, async_trait,
    futures::future,
};
use derive_more::with_trait::{Display, Error, From};
use hyper::{server::Server, service::make_service_fn};
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
impl<S, E1, E2, E3, E4> S3Storage for S3<S>
where
    S: Exec<CreateFile<dto::ByteStream>, Err = E1>
        + Exec<CreateSymlink, Err = E2>
        + Exec<DeleteFile, Ok = (), Err = E3>
        + Exec<GetFile, Ok = Option<ReadOnlyFile>, Err = E4>
        + fmt::Debug
        + Send
        + Sync
        + 'static,
    E1: fmt::Display,
    E2: fmt::Display,
    E3: AsRef<DeleteFileError> + fmt::Display + Send,
    E4: fmt::Display,
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
        let path = parse_s3_path(input.bucket, input.key)?;

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
        let path = parse_s3_path(input.bucket, input.key.clone())?;

        let file = self
            .0
            .exec(GetFile { path })
            .await
            .map_err(|e| internal_error("GetFile operation failed", e))?
            .ok_or(S3StorageError::Operation(
                dto::GetObjectError::NoSuchKey(input.key),
            ))?;

        let reader = ReaderStream::new(file.compat());

        tracing::info!("OK");
        Ok(dto::GetObjectOutput {
            body: Some(dto::ByteStream::new(reader)),
            ..dto::GetObjectOutput::default()
        })
    }

    async fn head_bucket(
        &self,
        _: dto::HeadBucketRequest,
    ) -> S3StorageResult<dto::HeadBucketOutput, dto::HeadBucketError> {
        Err(not_implemented("HeadBucket").into())
    }

    async fn head_object(
        &self,
        _: dto::HeadObjectRequest,
    ) -> S3StorageResult<dto::HeadObjectOutput, dto::HeadObjectError> {
        Err(not_implemented("HeadObject").into())
    }

    async fn list_buckets(
        &self,
        _: dto::ListBucketsRequest,
    ) -> S3StorageResult<dto::ListBucketsOutput, dto::ListBucketsError> {
        Err(not_implemented("ListBuckets").into())
    }

    async fn list_objects(
        &self,
        _: dto::ListObjectsRequest,
    ) -> S3StorageResult<dto::ListObjectsOutput, dto::ListObjectsError> {
        Err(not_implemented("ListObjects").into())
    }

    async fn list_objects_v2(
        &self,
        _: dto::ListObjectsV2Request,
    ) -> S3StorageResult<dto::ListObjectsV2Output, dto::ListObjectsV2Error>
    {
        Err(not_implemented("ListObjectsV2").into())
    }

    #[tracing::instrument(
       skip_all,
       fields(bucket = input.bucket.as_str(), key = input.key.as_str()),
    )]
    async fn put_object(
        &self,
        input: dto::PutObjectRequest,
    ) -> S3StorageResult<dto::PutObjectOutput, dto::PutObjectError> {
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
    s.try_into().map_err(|err| InvalidArgument { attr, err })
}

/// Invalid [`RelativePath`] provided as the `attr` of a request.
///
/// Converts into a [`S3ErrorCode::InvalidArgument`] [`S3Error`].
#[derive(Debug, Display)]
#[display("Invalid {attr}: {err}")]
struct InvalidArgument {
    /// Name of the invalid request attribute.
    attr: &'static str,

    /// Error of parsing the [`RelativePath`].
    err: InvalidRelativePathError,
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
