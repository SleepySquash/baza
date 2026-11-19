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

  Scenario Outline: GetObject: range <range>
    Given `rms.jpg` was uploaded to `data` bucket as `ranged`
    When trying to load `ranged` from `data` bucket with range `<range>`
    Then bytes `<start>..<end>` of `rms.jpg` are returned with `Content-Range` of `<content_range>`

    Examples:
      | range               | start  | end    | content_range        |
      | bytes=0-99          | 0      | 100    | bytes 0-99/128522    |
      | bytes=100-100       | 100    | 101    | bytes 100-100/128522 |
      | bytes=100000-       | 100000 | 128522 | bytes 100000-128521/128522 |
      | bytes=-1000         | 127522 | 128522 | bytes 127522-128521/128522 |
      | bytes=128000-999999 | 128000 | 128522 | bytes 128000-128521/128522 |
      | bytes=-999999       | 0      | 128522 | bytes 0-128521/128522 |

  Scenario: GetObject: unsatisfiable range
    Given `rms.jpg` was uploaded to `data` bucket as `ranged`
    When trying to load `ranged` from `data` bucket with range `bytes=128522-`
    Then `InvalidRange` error is returned
