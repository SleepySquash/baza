Feature: HeadObject

  Scenario: HeadObject: existing file
    Given `rms.jpg` was uploaded to `data` bucket as `my_file`
    When checking `my_file` metadata in `data` bucket
    Then its metadata matches `rms.jpg` stored as `data/my_file`

  Scenario: HeadObject: symlink
    Given `rms.jpg` was uploaded to `data` bucket as `dir/target`
    And `link` symlink was created on `links` bucket pointing to `data/dir/target`
    When checking `link` metadata in `links` bucket
    Then its metadata matches `rms.jpg` stored as `data/dir/target`

  Scenario: HeadObject: non-existing file
    Given there was nothing uploaded to `data` bucket as `my_file`
    When checking `my_file` metadata in `data` bucket
    Then `NoSuchKey` error is returned

  Scenario: HeadObject: dangling symlink
    Given `rms.jpg` was uploaded to `data` bucket as `dir/target`
    And `link` symlink was created on `links` bucket pointing to `data/dir/target`
    When `dir/target` is deleted from `data` bucket
    And checking `link` metadata in `links` bucket
    Then `NoSuchKey` error is returned

  Scenario: HeadObject: directory
    Given `rms.jpg` was uploaded to `heads` bucket as `prefix/file`
    When checking `prefix` directory metadata in `heads` bucket
    Then `NoSuchKey` error is returned

  Scenario: HeadObject: same ETag as GetObject
    Given `rms.jpg` was uploaded to `data` bucket as `my_file`
    When checking `my_file` metadata in `data` bucket
    And trying to load `my_file` from `data` bucket
    Then the ETag is the same

  Scenario: HeadObject: ETag changes on overwrite
    Given `rms.jpg` was uploaded to `data` bucket as `my_file`
    When checking `my_file` metadata in `data` bucket
    And `ignucius.jpg` is uploaded to `data` bucket as `my_file`
    And checking `my_file` metadata in `data` bucket
    Then the ETag has changed
