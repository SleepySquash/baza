Baza changelog
==============

All user visible changes to this project will be documented in this file. This project uses [Semantic Versioning 2.0.0].




## [0.3.0] · unreleased
[0.3.0]: /../../tree/main

[Diff](/../../compare/v0.2.0...main)

### BC Breaks

- Minimal supported Rust version is 1.96 now (was 1.85), as required by [`s3s`].

### Added

- [S3 API]:
    - `DeleteObject` and `DeleteObjects` methods (deleting a symlink removes only the symlink itself, and empty parent directories are removed).
    - `HeadObject` method (a symlink reports the metadata of the object it points to).
    - `Content-Length`, `Content-Type`, `ETag` and `Last-Modified` headers to `GetObject` responses (`ETag` is derived from the object's size and modification time, rather than being an MD5 digest of its content, and `Content-Type` is always `application/octet-stream`).
    - `ListObjects` and `ListObjectsV2` methods, supporting `prefix`, `delimiter`, `max-keys`, `marker`/`start-after`/`continuation-token` pagination and `encoding-type=url` (symlinks are listed with the metadata of the objects they point to, dangling symlinks are skipped, and a missing bucket is listed as an empty one).

### Changed

- [S3 API]:
    - Switched S3 protocol implementation from unmaintained [`s3-server`] to [`s3s`] 0.17, and HTTP server from [`hyper`] 0.14 to [`hyper`] 1 (also serving HTTP/2 now).
- Deployment:
    - [Docker] image is built from the official `rust` image, instead of `ghcr.io/instrumentisto/rust`.
    - [Docker] image is published to `ghcr.io/sleepysquash/baza` only.

### Fixed

- [S3 API]:
    - Unsupported methods panicking instead of returning a `NotImplemented` error.
    - `PutObject` leaving a partially written object on a failed or interrupted upload (now written to `tmp/` and renamed atomically).
    - `PutObject` of a regular file over an existing symlink writing through to the symlink's target instead of replacing the symlink.
    - `GetObject` of a key that is a prefix of other objects (a directory on disk) failing instead of returning a `NoSuchKey` error.
    - Object keys with URL-encoded characters (like spaces) being stored in their URL-encoded form (`a%20b` instead of `a b`).
    - Signature verification failing with `403` for some keys with URL-encoded characters (like `HeadObject` of a key with a space, sent by `aws-sdk-go-v2`).
    - `ListObjects` and `ListObjectsV2` returning all `CommonPrefixes` in a single XML element, and `ListObjectsV2` returning `max-keys` as `KeyCount`.




## [0.2.0] · 2022-08-23
[0.2.0]: /../../tree/v0.2.0

[Diff](/../../compare/v0.1.0...v0.2.0) | [Milestone](/../../milestone/2)

### Added

- [S3 API]:
    - `GetObject` method. ([#17])
    - Authentication via access and secret keys. ([#17])
- [CLI]:
    - `--access-key` option specifying S3 access key. ([#17])
    - `--secret-key` option specifying S3 secret key. ([#17]) 
- Environment variables:
    - `BAZA_ACCESS_KEY` specifying S3 access key. ([#17])
    - `BAZA_SECRET_KEY` specifying S3 secret key. ([#17]) 




## [0.1.0] · 2022-07-27
[0.1.0]: /../../tree/v0.1.0

[Milestone](/../../milestone/1)

### Added

- [S3 API]:
    - `PutObject` method with `symlink-to` support. ([#1])
- Deployment:
    - [Docker] image. ([#2]) 
    - [Helm] chart. ([#3])
- [CLI]:
    - `-l, --log-level` option specifying logging verbosity level. ([#1])
    - `-p, --port` option specifying port to run on. ([#1]) 
    - `-r, --root` option specifying directory where all buckets will be stored. ([#1]) 

[#1]: /../../pull/1
[#2]: /../../pull/2
[#3]: /../../pull/3
[#17]: /../../pull/17
[#21]: /../../pull/21




[CLI]: https://en.wikipedia.org/wiki/Command-line_interface
[Docker]: https://www.docker.com
[`hyper`]: https://docs.rs/hyper
[`s3-server`]: https://docs.rs/s3-server
[`s3s`]: https://docs.rs/s3s
[Helm]: https://helm.sh
[S3 API]: https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html
[Semantic Versioning 2.0.0]: https://semver.org
