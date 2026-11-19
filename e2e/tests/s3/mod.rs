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
    GetObjectRequest, HeadBucketError, HeadBucketRequest, HeadObjectError,
    HeadObjectRequest, ListObjectsRequest, ListObjectsV2Request,
    ObjectIdentifier, PutObjectError, PutObjectRequest, S3 as _, S3Client,
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
    let res = try_get_object(bucket, w.unique.filename(key)).await;
    w.get_object_response = Some(res.map(|(meta, bytes)| {
        w.object_metas.push(meta);
        bytes
    }));
}

#[when(regex = r"^checking `(\S+)` metadata in `(\S+)` bucket$")]
async fn checking_metadata(w: &mut World, key: String, bucket: String) {
    head_object(w, bucket, w.unique.filename(key)).await;
}

#[when(regex = r"^checking `(\S+)` directory metadata in `(\S+)` bucket$")]
async fn checking_directory_metadata(
    w: &mut World,
    key: String,
    bucket: String,
) {
    // Directory names are not enriched with `World::unique`.
    head_object(w, bucket, key).await;
}

#[then(regex = r"^its metadata matches `(\S+)` stored as `(\S+)`$")]
async fn metadata_matches(
    w: &mut World,
    sample: String,
    path: String,
) -> io::Result<()> {
    if let Some(Err(e)) = &w.head_object_response {
        panic!("`HeadObjectRequest` failed: {e}");
    }
    let meta = w.object_metas.last().expect("No object metadata received");

    let filename = w.unique.filename(path);
    let stored = async_fs::metadata(format!("{DATA_DIR}/{filename}")).await?;

    let size = i64::try_from(sample_file(sample).len()).unwrap();
    assert_eq!(meta.content_length, Some(size), "`Content-Length` mismatch");
    assert_eq!(
        meta.last_modified,
        Some(httpdate::fmt_http_date(stored.modified()?)),
        "`Last-Modified` mismatch",
    );
    assert!(
        meta.e_tag.as_ref().is_some_and(|t| !t.is_empty()),
        "No `ETag` returned",
    );
    Ok(())
}

#[then(regex = r"^the ETag (is the same|has changed)$")]
async fn etag_is_compared(w: &mut World, cmp: String) {
    let [.., prev, last] = w.object_metas.as_slice() else {
        panic!("Expected at least 2 object metadata to be received");
    };
    assert!(last.e_tag.is_some(), "No `ETag` returned");

    if cmp == "is the same" {
        assert_eq!(prev.e_tag, last.e_tag, "`ETag` has changed");
    } else {
        assert_ne!(prev.e_tag, last.e_tag, "`ETag` is the same");
    }
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
    if let Some(res) = w.head_object_response.take() {
        // `HEAD` responses have no body, so only the status code can be
        // checked.
        match &res {
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => {}
            Err(RusotoError::Unknown(resp)) if resp.status.as_u16() == 404 => {}
            _ => panic!("Expected `NoSuchKey` error, got: {res:#?}"),
        }
        return;
    }

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

#[given("the following objects were uploaded to a new bucket:")]
async fn objects_uploaded_to_new_bucket(w: &mut World, step: &Step) {
    let bucket = new_bucket(w);
    for row in &step.table().expect("No data table present in the step").rows {
        put_object(&bucket, &row[0], sample_file("rms.jpg"), None::<String>)
            .await;
    }
}

#[given(regex = "^`(\\S+)` symlink was created in the new bucket \
                  pointing to `(\\S+)`$")]
async fn symlink_created_in_new_bucket(
    w: &mut World,
    key: String,
    original: String,
) {
    let bucket = new_bucket(w);
    put_object(&bucket, key, &[], Some(format!("{bucket}/{original}"))).await;
}

#[given(regex = r"^`(\S+)` empty directory was created in the new bucket$")]
async fn empty_dir_created_in_new_bucket(
    w: &mut World,
    path: String,
) -> io::Result<()> {
    let bucket = new_bucket(w);
    async_fs::create_dir_all(format!("{DATA_DIR}/{bucket}/{path}")).await
}

#[given("the new bucket was created empty")]
async fn new_bucket_created_empty(w: &mut World) -> io::Result<()> {
    // There is no `CreateBucket` support, so it's created on disk directly.
    async_fs::create_dir_all(format!("{DATA_DIR}/{}", new_bucket(w))).await
}

#[given(regex = r"^`(\S+)` was deleted from the new bucket$")]
async fn deleted_from_new_bucket(w: &mut World, key: String) {
    try_delete_object(new_bucket(w), key)
        .await
        .unwrap_or_else(|e| panic!("`DeleteObjectRequest` failed: {e}"));
}

#[when(regex = "^listing the new bucket with (ListObjects|ListObjectsV2)\
                 (?: by prefix `(\\S+)`)?\
                 (?: by delimiter `(\\S+)`)?\
                 (?: by (\\d+) keys per page)?\
                 ( with URL encoding)?$")]
async fn listing_new_bucket(
    w: &mut World,
    op: String,
    prefix: String,
    delimiter: String,
    max_keys: String,
    url_encoding: String,
) {
    let params = ListParams {
        bucket: new_bucket(w),
        delimiter: (!delimiter.is_empty()).then_some(delimiter),
        encoding_type: (!url_encoding.is_empty()).then(|| "url".into()),
        max_keys: (!max_keys.is_empty())
            .then(|| max_keys.parse().expect("Invalid number of keys")),
        prefix: (!prefix.is_empty()).then_some(prefix),
    };

    w.list_objects_response = Some(if op == "ListObjects" {
        list_objects(params).await
    } else {
        list_objects_v2(params).await
    });
}

#[then("the following keys are listed:")]
async fn keys_are_listed(w: &mut World, step: &Step) {
    let expected = step
        .table()
        .expect("No data table present in the step")
        .rows
        .iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    let actual = w
        .last_listed_pages()
        .iter()
        .flat_map(|p| p.objects.iter().map(|o| o.key.clone()))
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

#[then("the following common prefixes are listed:")]
async fn common_prefixes_are_listed(w: &mut World, step: &Step) {
    let expected = step
        .table()
        .expect("No data table present in the step")
        .rows
        .iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    let actual = w
        .last_listed_pages()
        .iter()
        .flat_map(|p| p.common_prefixes.iter().cloned())
        .collect::<Vec<_>>();

    assert_eq!(actual, expected);
}

#[then(regex = r"^no (keys|common prefixes) are listed$")]
async fn nothing_is_listed(w: &mut World, what: String) {
    for page in w.last_listed_pages() {
        if what == "keys" {
            assert!(page.objects.is_empty(), "keys are listed: {page:#?}");
        } else {
            assert!(
                page.common_prefixes.is_empty(),
                "common prefixes are listed: {page:#?}",
            );
        }
    }
}

#[then(regex = r"^it's listed in (\d+) pages?$")]
async fn listed_in_pages(w: &mut World, count: usize) {
    assert_eq!(w.last_listed_pages().len(), count, "number of pages");
}

#[then(regex = r"^`(\S+)` is listed with the size of `(\S+)`$")]
async fn listed_with_size(w: &mut World, key: String, sample: String) {
    let size = i64::try_from(sample_file(sample).len()).unwrap();

    let obj = w.last_listed_object(&key);
    assert_eq!(obj.size, Some(size), "`Size` mismatch");
}

#[then(regex = "^`(\\S+)` is listed with the same metadata as \
                 `HeadObject` returns$")]
async fn listed_with_head_object_metadata(w: &mut World, key: String) {
    let obj = w.last_listed_object(&key).clone();

    let req = HeadObjectRequest {
        bucket: new_bucket(w),
        key,
        ..HeadObjectRequest::default()
    };
    let head = s3_client()
        .head_object(req)
        .await
        .unwrap_or_else(|e| panic!("`HeadObjectRequest` failed: {e}"));

    assert_eq!(obj.e_tag, head.e_tag, "`ETag` mismatch");
    assert_eq!(obj.size, head.content_length, "`Size` mismatch");
    let listed = obj.last_modified.as_deref().expect("No `LastModified`");
    let listed = chrono::DateTime::parse_from_rfc3339(listed)
        .unwrap_or_else(|e| panic!("Invalid `LastModified` `{listed}`: {e}"));
    assert_eq!(
        Some(httpdate::fmt_http_date(listed.into())),
        head.last_modified,
        "`LastModified` mismatch",
    );
}

/// Returns the name of the bucket unique to the provided [`World`], which is
/// not shared with other scenarios, so its listings are predictable.
fn new_bucket(w: &World) -> String {
    w.unique.filename("list")
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

/// Response to a [`HeadObjectRequest`].
pub(super) type HeadObjectResponse =
    Result<ObjectMeta, RusotoError<HeadObjectError>>;

/// Metadata of an S3 object, returned by `GetObject` and `HeadObject`.
#[derive(Clone, Debug)]
pub(super) struct ObjectMeta {
    /// `Content-Length` of the object.
    content_length: Option<i64>,

    /// `ETag` of the object.
    e_tag: Option<String>,

    /// `Last-Modified` time of the object.
    last_modified: Option<String>,
}

/// Pages of a full `ListObjects` or `ListObjectsV2` listing, in the order they
/// were received.
pub(super) type ListObjectsResponse = Result<Vec<ListedPage>, String>;

/// Single page of a `ListObjects` or `ListObjectsV2` listing.
#[derive(Clone, Debug)]
pub(super) struct ListedPage {
    /// Listed objects.
    objects: Vec<ListedObject>,

    /// Listed common prefixes.
    common_prefixes: Vec<String>,
}

/// Object listed by `ListObjects` or `ListObjectsV2`.
#[derive(Clone, Debug)]
struct ListedObject {
    /// Key of the object.
    key: String,

    /// `ETag` of the object.
    e_tag: Option<String>,

    /// `LastModified` time of the object.
    last_modified: Option<String>,

    /// Size of the object.
    size: Option<i64>,
}

impl From<rusoto_s3::Object> for ListedObject {
    fn from(obj: rusoto_s3::Object) -> Self {
        Self {
            key: obj.key.expect("No `Key` in `Contents`"),
            e_tag: obj.e_tag,
            last_modified: obj.last_modified,
            size: obj.size,
        }
    }
}

/// Parameters of a `ListObjects` or `ListObjectsV2` listing.
struct ListParams {
    bucket: String,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    max_keys: Option<i64>,
    prefix: Option<String>,
}

/// Maximum number of pages to request in a single listing, preventing infinite
/// loops on broken pagination.
const MAX_LIST_PAGES: usize = 100;

/// Lists all the objects with `ListObjects`, following `NextMarker`s.
async fn list_objects(params: ListParams) -> ListObjectsResponse {
    let mut pages = vec![];
    let mut marker = None;
    while pages.len() < MAX_LIST_PAGES {
        let req = ListObjectsRequest {
            bucket: params.bucket.clone(),
            delimiter: params.delimiter.clone(),
            encoding_type: params.encoding_type.clone(),
            marker: marker.take(),
            max_keys: params.max_keys,
            prefix: params.prefix.clone(),
            ..ListObjectsRequest::default()
        };
        let out = s3_client()
            .list_objects(req)
            .await
            .map_err(|e| format!("`ListObjectsRequest` failed: {e}"))?;

        pages.push(ListedPage {
            objects: out
                .contents
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            common_prefixes: out
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .map(|p| p.prefix.expect("No `Prefix` in `CommonPrefixes`"))
                .collect(),
        });
        if out.is_truncated != Some(true) {
            return Ok(pages);
        }
        marker = Some(out.next_marker.ok_or("No `NextMarker` returned")?);
    }
    Err(format!("More than {MAX_LIST_PAGES} pages listed"))
}

/// Lists all the objects with `ListObjectsV2`, following
/// `NextContinuationToken`s.
async fn list_objects_v2(params: ListParams) -> ListObjectsResponse {
    let mut pages = vec![];
    let mut token = None;
    while pages.len() < MAX_LIST_PAGES {
        let req = ListObjectsV2Request {
            bucket: params.bucket.clone(),
            continuation_token: token.take(),
            delimiter: params.delimiter.clone(),
            encoding_type: params.encoding_type.clone(),
            max_keys: params.max_keys,
            prefix: params.prefix.clone(),
            ..ListObjectsV2Request::default()
        };
        let out = s3_client()
            .list_objects_v2(req)
            .await
            .map_err(|e| format!("`ListObjectsV2Request` failed: {e}"))?;

        pages.push(ListedPage {
            objects: out
                .contents
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            common_prefixes: out
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .map(|p| p.prefix.expect("No `Prefix` in `CommonPrefixes`"))
                .collect(),
        });
        if out.is_truncated != Some(true) {
            return Ok(pages);
        }
        token = Some(
            out.next_continuation_token
                .ok_or("No `NextContinuationToken` returned")?,
        );
    }
    Err(format!("More than {MAX_LIST_PAGES} pages listed"))
}

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
) -> Result<(ObjectMeta, Vec<u8>), RusotoError<GetObjectError>> {
    let req = GetObjectRequest {
        bucket: bucket.to_string(),
        key: key.to_string(),
        ..GetObjectRequest::default()
    };

    let resp = s3_client().get_object(req).await?;
    let mut buf = Vec::new();
    resp.body.unwrap().into_async_read().read_to_end(&mut buf).await.unwrap();

    let meta = ObjectMeta {
        content_length: resp.content_length,
        e_tag: resp.e_tag,
        last_modified: resp.last_modified,
    };
    Ok((meta, buf))
}

/// Performs a [`HeadObjectRequest`], storing its [`HeadObjectResponse`] in the
/// provided [`World`].
async fn head_object(w: &mut World, bucket: String, key: String) {
    let req = HeadObjectRequest { bucket, key, ..HeadObjectRequest::default() };

    let res = s3_client().head_object(req).await.map(|resp| ObjectMeta {
        content_length: resp.content_length,
        e_tag: resp.e_tag,
        last_modified: resp.last_modified,
    });
    if let Ok(meta) = &res {
        w.object_metas.push(meta.clone());
    }
    w.head_object_response = Some(res);
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

    /// Returns pages of the last successful [`ListObjectsResponse`], stored in
    /// this [`World`].
    ///
    /// # Panics
    ///
    /// - If there is no [`ListObjectsResponse`] in this [`World`].
    /// - If the [`ListObjectsResponse`] is an error.
    fn last_listed_pages(&self) -> &[ListedPage] {
        self.list_objects_response
            .as_ref()
            .expect("No `ListObjectsResponse`")
            .as_ref()
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Returns the [`ListedObject`] with the provided `key` from the last
    /// successful [`ListObjectsResponse`], stored in this [`World`].
    ///
    /// # Panics
    ///
    /// - If there is no successful [`ListObjectsResponse`] in this [`World`].
    /// - If there is no [`ListedObject`] with the provided `key` in it.
    fn last_listed_object(&self, key: &str) -> &ListedObject {
        self.last_listed_pages()
            .iter()
            .flat_map(|p| &p.objects)
            .find(|o| o.key == key)
            .unwrap_or_else(|| panic!("`{key}` is not listed"))
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
