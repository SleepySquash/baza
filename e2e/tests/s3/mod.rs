//! S3 HTTP API E2E (end-to-end) tests.

use std::{collections::HashMap, fmt, io, mem};

use baza::futures::{StreamExt as _, stream};
use baza_api_s3 as s3;
use cucumber::{gherkin::Step, given, then, when};
use rusoto_core::{HttpClient, RusotoError, region::Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    Delete, DeleteObjectError, DeleteObjectRequest, DeleteObjectsError,
    DeleteObjectsOutput, DeleteObjectsRequest, GetObjectError,
    GetObjectRequest, HeadBucketError, HeadBucketRequest, ObjectIdentifier,
    PutObjectError, PutObjectRequest, S3 as _, S3Client,
};
use tokio::io::AsyncReadExt as _;

use super::{DATA_DIR, World, sample_file};

/// URL of S3 HTTP API to run E2E tests against.
const API_URL: &str = "http://localhost:9294";

#[given(regex = r"^`(\S+)` was uploaded to `(\S+)` bucket as `(\S+)`$")]
#[when(regex = r"^`(\S+)` is uploaded to `(\S+)` bucket as `(\S+)`$")]
async fn file_uploaded(
    w: &mut World,
    sample: String,
    bucket: String,
    key: String,
) {
    put_object(
        bucket,
        w.unique.filename(key),
        sample_file(sample),
        None::<String>,
    )
    .await
}

#[given(regex = r"^there was nothing uploaded to `(\S+)` bucket as `(\S+)`$")]
async fn nothing_uploaded(_: &mut World) {
    // nothing is uploaded by default
}

#[given(regex = "^`(\\S+)` symlink was created on `(\\S+)` bucket \
                  pointing to `(\\S+)`$")]
#[when(regex = "^`(\\S+)` symlink is created on `(\\S+)` bucket \
                 pointing to `(\\S+)`$")]
async fn symlink_is_uploaded(
    w: &mut World,
    key: String,
    bucket: String,
    original: String,
) {
    put_object(
        bucket,
        w.unique.filename(key),
        &[],
        Some(w.unique.filename(original)),
    )
    .await
}

#[then(regex = r"^`(\S+)` is stored as `(\S+)`$")]
async fn file_is_stored(
    w: &mut World,
    sample: String,
    path: String,
) -> io::Result<()> {
    assert_stored(sample, w.unique.filename(path)).await
}

#[then(regex = r"^`(\S+)` is not stored$")]
async fn file_is_not_stored(w: &mut World, path: String) {
    let filename = w.unique.filename(path);
    let res =
        async_fs::symlink_metadata(format!("{DATA_DIR}/{filename}")).await;

    match res {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        _ => panic!("Expected `{filename}` to be absent, got: {res:#?}"),
    }
}

#[then(regex = r"^`(\S+)` directory (exists|doesn't exist)$")]
async fn directory_exists(_: &mut World, path: String, exists: String) {
    let is_dir = async_fs::metadata(format!("{DATA_DIR}/{path}"))
        .await
        .is_ok_and(|m| m.is_dir());

    assert_eq!(is_dir, exists == "exists", "`{path}` directory {exists}");
}

#[then(regex = r"^`(\S+)` is accessible via `(\S+)`$")]
async fn file_is_accessible(
    w: &mut World,
    sample: String,
    path: String,
) -> io::Result<()> {
    // We are forced to handle symlinks manually, because Dockerized application
    // has different absolute paths.
    let filename = w.unique.filename(path);
    let src = async_fs::read_link(format!("{DATA_DIR}/{filename}"))
        .await?
        .display()
        .to_string()
        .split(DATA_DIR.trim_matches('.').trim_matches('/'))
        .nth(1)
        .unwrap()
        .trim_matches('/')
        .to_owned();
    assert_stored(sample, src).await
}

#[when("trying to upload files with the following keys:")]
async fn keys_table(w: &mut World, step: &Step) {
    w.keys_to_check = step
        .table()
        .expect("No data table present in the step")
        .rows
        .iter()
        .map(|row| row[0].clone())
        .collect();
}

#[then("`InvalidArgument` error is returned")]
async fn invalid_argument_error(w: &mut World) {
    stream::iter(mem::take(&mut w.keys_to_check))
        .then(|key| try_put_object("data", key, &[], None::<String>))
        .for_each(|res| async move {
            assert_invalid_argument(res);
        })
        .await;
}

#[when("trying to delete files with the following keys:")]
async fn delete_keys_table(w: &mut World, step: &Step) {
    keys_table(w, step).await;
}

#[then("`InvalidArgument` error is returned for each deletion")]
async fn invalid_argument_deletion_error(w: &mut World) {
    stream::iter(mem::take(&mut w.keys_to_check))
        .then(|key| try_delete_object("data", key))
        .for_each(|res| async move {
            assert_invalid_argument(res);
        })
        .await;
}

#[when(regex = r"^`(\S+)` is deleted from `(\S+)` bucket$")]
async fn file_is_deleted(w: &mut World, key: String, bucket: String) {
    try_delete_object(bucket, w.unique.filename(key))
        .await
        .unwrap_or_else(|e| panic!("`DeleteObjectRequest` failed: {e}"));
}

#[when(regex = r"^`(\S+)` directory is deleted from `(\S+)` bucket$")]
async fn directory_is_deleted(_: &mut World, key: String, bucket: String) {
    // Directory names are not enriched with `World::unique`.
    try_delete_object(bucket, key)
        .await
        .unwrap_or_else(|e| panic!("`DeleteObjectRequest` failed: {e}"));
}

#[when(regex = "^deleting the following keys from `(\\S+)` bucket at once\
                 ( quietly)?:$")]
async fn deleting_keys_at_once(
    w: &mut World,
    bucket: String,
    quietly: String,
    step: &Step,
) {
    let objects = step
        .table()
        .expect("No data table present in the step")
        .rows
        .iter()
        .map(|row| ObjectIdentifier {
            key: w.unique.filename(&row[0]),
            version_id: None,
        })
        .collect();
    let req = DeleteObjectsRequest {
        bucket,
        delete: Delete { objects, quiet: Some(!quietly.is_empty()) },
        ..DeleteObjectsRequest::default()
    };

    w.delete_objects_response = Some(s3_client().delete_objects(req).await);
}

#[then(
    regex = "^the following keys are reported as (deleted|`(\\S+)` errors):$"
)]
async fn keys_are_reported(
    w: &mut World,
    kind: String,
    code: String,
    step: &Step,
) {
    let mut expected: Vec<String> = step
        .table()
        .map(|t| t.rows.iter().map(|row| w.unique.filename(&row[0])).collect())
        .unwrap_or_default();
    expected.sort();

    let out = w.last_delete_objects_output();
    let mut actual = if kind == "deleted" {
        out.deleted
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.key.expect("No `Key` in `Deleted`"))
            .collect::<Vec<_>>()
    } else {
        out.errors
            .unwrap_or_default()
            .into_iter()
            .map(|e| {
                assert_eq!(e.code.as_deref(), Some(code.as_str()), "{e:#?}");
                e.key.expect("No `Key` in `Error`")
            })
            .collect()
    };
    actual.sort();

    assert_eq!(actual, expected);
}

#[then("no keys are reported as deleted")]
async fn no_keys_reported(w: &mut World) {
    let out = w.last_delete_objects_output();

    assert!(out.deleted.unwrap_or_default().is_empty(), "keys are reported");
    assert!(out.errors.unwrap_or_default().is_empty(), "errors are reported");
}

#[when(regex = r"^trying to load `(\S+)` from `(\S+)` bucket$")]
async fn trying_to_load_file(w: &mut World, key: String, bucket: String) {
    w.get_object_response =
        Some(try_get_object(bucket, w.unique.filename(key)).await);
}

#[then(regex = r"^`(\S+)` file is returned$")]
async fn file_is_returned(w: &mut World, name: String) {
    let sample = sample_file(name);

    let file = w
        .last_get_object_response()
        .unwrap_or_else(|e| panic!("`GetObjectRequest` failed: {e}"));

    assert_eq!(sample.len(), file.len());
    assert!(sample == file, "Bytes don't match");
}

#[then(regex = r"^`NoSuchKey` error is returned$")]
async fn error_is_returned(w: &mut World) {
    let res = w.last_get_object_response();
    match res {
        Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => {}
        _ => panic!("Expected `NoSuchKey` error, got: {res:#?}"),
    }
}

#[when(regex = r"^checking whether `(\S+)` bucket exists$")]
async fn checking_bucket_exists(w: &mut World, bucket: String) {
    let req = HeadBucketRequest { bucket, ..HeadBucketRequest::default() };
    w.head_bucket_response = Some(s3_client().head_bucket(req).await);
}

#[then("`NotImplemented` error is returned")]
async fn not_implemented_error(w: &mut World) {
    let res = w.head_bucket_response.take().expect("No `HeadBucketResponse`");
    // `HEAD` responses have no body, so only the status code can be checked.
    match &res {
        Err(RusotoError::Unknown(resp)) if resp.status.as_u16() == 501 => {}
        _ => panic!("Expected `NotImplemented` error, got: {res:#?}"),
    }
}

/// Asserts that the provided sample file is stored at the provided `filename`
/// in the [`DATA_DIR`].
async fn assert_stored(
    sample: impl AsRef<str>,
    filename: impl AsRef<str>,
) -> io::Result<()> {
    let filename = filename.as_ref();
    let stored = async_fs::read(format!("{DATA_DIR}/{filename}")).await?;

    assert!(sample_file(sample) == stored, "Bytes don't match");
    Ok(())
}

fn assert_invalid_argument<E: fmt::Debug>(res: Result<(), RusotoError<E>>) {
    match &res {
        Err(RusotoError::Unknown(resp))
            if resp.body_as_str().contains("InvalidArgument") => {}
        _ => panic!("Expected `InvalidArgument` error, got: {res:#?}"),
    }
}

async fn put_object(
    bucket: impl ToString,
    key: impl ToString,
    body: &[u8],
    symlink_to: Option<impl ToString>,
) {
    try_put_object(bucket, key, body, symlink_to)
        .await
        .unwrap_or_else(|e| panic!("`PutObjectRequest` failed: {e}"));
}

/// Response to a [`DeleteObjectsRequest`].
pub(super) type DeleteObjectsResponse =
    Result<DeleteObjectsOutput, RusotoError<DeleteObjectsError>>;

/// Response to a [`HeadBucketRequest`].
pub(super) type HeadBucketResponse = Result<(), RusotoError<HeadBucketError>>;

/// Response to a [`GetObjectRequest`].
pub(super) type GetObjectResponse =
    Result<Vec<u8>, RusotoError<GetObjectError>>;

async fn try_put_object(
    bucket: impl ToString,
    key: impl ToString,
    body: &[u8],
    symlink_to: Option<impl ToString>,
) -> Result<(), RusotoError<PutObjectError>> {
    let req = PutObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        body: Some(body.to_owned().into()),
        metadata: symlink_to.map(|original| {
            let mut meta = HashMap::with_capacity(1);
            meta.insert(s3::SYMLINK_META_KEY.to_string(), original.to_string());
            meta
        }),
        ..PutObjectRequest::default()
    };

    s3_client().put_object(req).await.map(drop)
}

async fn try_delete_object(
    bucket: impl ToString,
    key: impl ToString,
) -> Result<(), RusotoError<DeleteObjectError>> {
    let req = DeleteObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..DeleteObjectRequest::default()
    };

    s3_client().delete_object(req).await.map(drop)
}

async fn try_get_object(
    bucket: impl ToString,
    key: impl ToString,
) -> GetObjectResponse {
    let req = GetObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..GetObjectRequest::default()
    };

    let resp = s3_client().get_object(req).await?;
    let mut buf = Vec::new();
    resp.body.unwrap().into_async_read().read_to_end(&mut buf).await.unwrap();

    Ok(buf)
}

/// Creates a new [`S3Client`] for performing requests to the S3 HTTP API being
/// tested.
fn s3_client() -> S3Client {
    S3Client::new_with(
        HttpClient::new().expect("Failed to initialize Rusoto HTTP client"),
        StaticProvider::new_minimal("baza".into(), "baza".into()),
        Region::Custom { name: "test".into(), endpoint: API_URL.into() },
    )
}

impl World {
    /// Takes the last [`GetObjectResponse`], stored in this [`World`].
    ///
    /// # Panics
    ///
    /// If there is no [`GetObjectResponse`] in this [`World`].
    fn last_get_object_response(&mut self) -> GetObjectResponse {
        self.get_object_response.take().expect("No `GetObjectResponse`")
    }

    /// Returns the last successful [`DeleteObjectsResponse`], stored in this
    /// [`World`].
    ///
    /// # Panics
    ///
    /// - If there is no [`DeleteObjectsResponse`] in this [`World`].
    /// - If the [`DeleteObjectsResponse`] is an error.
    fn last_delete_objects_output(&self) -> DeleteObjectsOutput {
        self.delete_objects_response
            .as_ref()
            .expect("No `DeleteObjectsResponse`")
            .as_ref()
            .unwrap_or_else(|e| panic!("`DeleteObjectsRequest` failed: {e}"))
            .clone()
    }
}
