Feature: Multipart upload

  Scenario: Multipart upload: joins parts
    Given a multipart upload was created in `data` bucket as `multipart/file`
    When part 1 of 5 MiB is uploaded
    And part 3 of 100 KiB is uploaded
    And part 2 of 5 MiB is uploaded
    And the multipart upload is completed with parts `1, 2, 3`
    Then the multipart upload is stored as `data/multipart/file`
    And the multipart upload is removed
    And the multipart upload has the same ETag as `HeadObject` returns

  Scenario: Multipart upload: single small part
    Given a multipart upload was created in `data` bucket as `multipart/single`
    When part 1 of 10 B is uploaded
    And the multipart upload is completed with parts `1`
    Then the multipart upload is stored as `data/multipart/single`

  Scenario: Multipart upload: overwrites existing file
    Given `rms.jpg` was uploaded to `data` bucket as `multipart/existing`
    And a multipart upload was created in `data` bucket as `multipart/existing`
    When part 1 of 1 KiB is uploaded
    And the multipart upload is completed with parts `1`
    Then the multipart upload is stored as `data/multipart/existing`

  Scenario: Multipart upload: re-uploaded part replaces previous one
    Given a multipart upload was created in `data` bucket as `multipart/reuploaded`
    And part 1 of 1 KiB was uploaded
    When part 1 of 2 KiB is uploaded
    And the multipart upload is completed with parts `1`
    Then the multipart upload is stored as `data/multipart/reuploaded`

  Scenario: Multipart upload: skips unlisted parts
    Given a multipart upload was created in `data` bucket as `multipart/skipped`
    And part 1 of 5 MiB was uploaded
    And part 2 of 1 KiB was uploaded
    And part 3 of 1 KiB was uploaded
    When the multipart upload is completed with parts `1, 3`
    Then the multipart upload is stored as `data/multipart/skipped`
    And the multipart upload is removed

  Scenario: Multipart upload: part ETags are MD5 digests
    Given a multipart upload was created in `data` bucket as `multipart/etags`
    When part 1 of 1 KiB is uploaded
    And part 2 of 10 B is uploaded
    Then the ETags of the uploaded parts are their MD5 digests

  Scenario: Multipart upload: lists parts
    Given a multipart upload was created in `data` bucket as `multipart/listed`
    And part 2 of 2 KiB was uploaded
    And part 1 of 1 KiB was uploaded
    And part 10 of 10 B was uploaded
    Then the uploaded parts are listed in 1 page

  Scenario: Multipart upload: lists parts by pages
    Given a multipart upload was created in `data` bucket as `multipart/paginated`
    And part 3 of 3 KiB was uploaded
    And part 1 of 1 KiB was uploaded
    And part 2 of 2 KiB was uploaded
    Then the uploaded parts are listed by 2 per page in 2 pages

  Scenario: Multipart upload: abort
    Given a multipart upload was created in `data` bucket as `multipart/aborted`
    And part 1 of 1 KiB was uploaded
    When the multipart upload is aborted
    Then the multipart upload is removed
    And `data/multipart/aborted` is not stored
    When trying to upload part 2 of 1 KiB
    Then `NoSuchUpload` error is returned for the multipart upload
    When trying to complete the multipart upload with parts `1`
    Then `NoSuchUpload` error is returned for the multipart upload

  Scenario: Multipart upload: abort after completion
    Given a multipart upload was created in `data` bucket as `multipart/abort-completed`
    And part 1 of 1 KiB was uploaded
    And the multipart upload was completed with parts `1`
    When trying to abort the multipart upload
    Then `NoSuchUpload` error is returned for the multipart upload
    And the multipart upload is stored as `data/multipart/abort-completed`

  Scenario: Multipart upload: wrong part order
    Given a multipart upload was created in `data` bucket as `multipart/unordered`
    And part 1 of 1 KiB was uploaded
    And part 2 of 1 KiB was uploaded
    When trying to complete the multipart upload with parts `2, 1`
    Then `InvalidPartOrder` error is returned for the multipart upload
    And the multipart upload still exists
    And `data/multipart/unordered` is not stored

  Scenario: Multipart upload: missing part
    Given a multipart upload was created in `data` bucket as `multipart/missing`
    And part 1 of 1 KiB was uploaded
    And part 2 of 1 KiB was uploaded
    When trying to complete the multipart upload with parts `1, 3`
    Then `InvalidPart` error is returned for the multipart upload
    And the multipart upload still exists
    And `data/multipart/missing` is not stored

  Scenario: Multipart upload: wrong ETag
    Given a multipart upload was created in `data` bucket as `multipart/wrong-etag`
    And part 1 of 1 KiB was uploaded
    When trying to complete the multipart upload with parts `1` and wrong ETags
    Then `InvalidPart` error is returned for the multipart upload
    And the multipart upload still exists

  Scenario: Multipart upload: too small part
    Given a multipart upload was created in `data` bucket as `multipart/too-small`
    And part 1 of 1 KiB was uploaded
    And part 2 of 1 KiB was uploaded
    When trying to complete the multipart upload with parts `1, 2`
    Then `EntityTooSmall` error is returned for the multipart upload
    And the multipart upload still exists
    And `data/multipart/too-small` is not stored

  Scenario: Multipart upload: failed completion can be retried
    Given a multipart upload was created in `data` bucket as `multipart/retried`
    And part 1 of 1 KiB was uploaded
    When trying to complete the multipart upload with parts `1, 2`
    Then `InvalidPart` error is returned for the multipart upload
    When the multipart upload is completed with parts `1`
    Then the multipart upload is stored as `data/multipart/retried`
    And the multipart upload is removed

  Scenario: Multipart upload: wrong Content-MD5
    Given a multipart upload was created in `data` bucket as `multipart/bad-digest`
    When trying to upload part 1 of 1 KiB with a wrong Content-MD5
    Then `BadDigest` error is returned for the multipart upload
    And no parts are listed

  Scenario: Multipart upload: invalid part numbers
    Given a multipart upload was created in `data` bucket as `multipart/part-numbers`
    When trying to upload part 0 of 1 KiB
    Then `InvalidArgument` error is returned for the multipart upload
    When trying to upload part 10001 of 1 KiB
    Then `InvalidArgument` error is returned for the multipart upload
    And no parts are listed
