Feature: GetObject

  Scenario: GetObject: existing file
    Given `rms.jpg` was uploaded to `data` bucket as `my_file`
    When trying to load `my_file` from `data` bucket
    Then `rms.jpg` file is returned
    And its metadata matches `rms.jpg` stored as `data/my_file`

  Scenario: GetObject: symlink
    Given `rms.jpg` was uploaded to `data` bucket as `dir/target`
    And `link` symlink was created on `links` bucket pointing to `data/dir/target`
    When trying to load `link` from `links` bucket
    Then `rms.jpg` file is returned
    And its metadata matches `rms.jpg` stored as `data/dir/target`

  Scenario: GetObject: non-existing file
    Given there was nothing uploaded to `data` bucket as `my_file`
    When trying to load `my_file` from `data` bucket
    Then `NoSuchKey` error is returned
