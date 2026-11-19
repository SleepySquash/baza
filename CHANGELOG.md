Baza changelog
==============

All user visible changes to this project will be documented in this file. This project uses [Semantic Versioning 2.0.0].




## [0.3.0] · 2026-11-19
[0.3.0]: /../../tree/v0.3.0

[Diff](/../../compare/v0.2.0...v0.3.0) | [Milestone](/../../milestone/1)

### BC Breaks

- Minimal supported Rust version is 1.96.

### Added

- [S3 API]:
    - `DeleteObject` and `DeleteObjects` methods.
    - `HeadObject` method.
    - `ListObjects` and `ListObjectsV2` methods.
    - `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload` and `ListParts` methods.
    - `Content-MD5` verification in `UploadPart` requests.
    - `Content-Length`, `Content-Type`, `ETag` and `Last-Modified` headers to `GetObject` responses.
    - `Range` support in `GetObject` requests, and `Accept-Ranges` header to `GetObject` and `HeadObject` responses.
    - HTTP/2 support.
- [CLI]:
    - `--multipart-ttl` option specifying time after which incomplete multipart uploads are aborted (24 hours by default).
- Environment variables:
    - `BAZA_MULTIPART_TTL` specifying time after which incomplete multipart uploads are aborted.

### Changed

- [S3 API]:
    - Migrated from [`s3-server`] to [`s3s`].
- Deployment:
    - [Docker] image is published to `ghcr.io/sleepysquash/baza`.

### Fixed

- [S3 API]:
    - Panics on unsupported methods.
    - `PutObject` leaving partially written objects.
    - `PutObject` over a symlink writing to its target.
    - `GetObject` of a directory not returning `NoSuchKey`.
    - Object keys being stored URL-encoded.
    - Signature verification failing for URL-encoded keys.




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
[`s3-server`]: https://docs.rs/s3-server
[`s3s`]: https://docs.rs/s3s
[Helm]: https://helm.sh
[S3 API]: https://docs.aws.amazon.com/AmazonS3/latest/API/Type_API_Reference.html
[Semantic Versioning 2.0.0]: https://semver.org
