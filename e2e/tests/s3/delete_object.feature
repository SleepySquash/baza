Feature: DeleteObject

  Scenario: DeleteObject: regular file
    Given `rms.jpg` was uploaded to `data` bucket as `dir/file`
    When `dir/file` is deleted from `data` bucket
    Then `data/dir/file` is not stored

  Scenario: DeleteObject: deleted file is not returned
    Given `rms.jpg` was uploaded to `data` bucket as `my_file`
    When `my_file` is deleted from `data` bucket
    And trying to load `my_file` from `data` bucket
    Then `NoSuchKey` error is returned

  Scenario: DeleteObject: non-existing file
    Given there was nothing uploaded to `data` bucket as `my_file`
    When `my_file` is deleted from `data` bucket
    Then `data/my_file` is not stored

  Scenario: DeleteObject: symlink
    Given `rms.jpg` was uploaded to `data` bucket as `dir/target`
    And `link` symlink was created on `links` bucket pointing to `data/dir/target`
    When `link` is deleted from `links` bucket
    Then `links/link` is not stored
    And `rms.jpg` is stored as `data/dir/target`

  Scenario: DeleteObject: symlink target leaves dangling symlink
    Given `rms.jpg` was uploaded to `data` bucket as `dir/target`
    And `link` symlink was created on `links` bucket pointing to `data/dir/target`
    When `dir/target` is deleted from `data` bucket
    And trying to load `link` from `links` bucket
    Then `NoSuchKey` error is returned

  Scenario: DeleteObject: directory is not deleted
    Given `rms.jpg` was uploaded to `prefixes` bucket as `prefix/file`
    When `prefix` directory is deleted from `prefixes` bucket
    Then `rms.jpg` is stored as `prefixes/prefix/file`

  Scenario: DeleteObject: prunes empty directories
    Given `rms.jpg` was uploaded to `pruned` bucket as `a/b/file`
    When `a/b/file` is deleted from `pruned` bucket
    Then `pruned` directory doesn't exist

  Scenario: DeleteObject: keeps non-empty directories
    Given `rms.jpg` was uploaded to `pruned-partially` bucket as `a/b/file`
    And `ignucius.jpg` was uploaded to `pruned-partially` bucket as `a/file`
    When `a/b/file` is deleted from `pruned-partially` bucket
    Then `pruned-partially/a/b` directory doesn't exist
    And `pruned-partially/a` directory exists
    And `ignucius.jpg` is stored as `pruned-partially/a/file`

  Scenario: DeleteObject: invalid keys
    When trying to delete files with the following keys:
      | /abc     |
      | ./abc    |
      | abc/./d  |
      | ../abc   |
      | abc/../d |
      | abc//d   |
    Then `InvalidArgument` error is returned for each deletion

  Scenario: DeleteObjects: multiple keys with invalid one
    Given `rms.jpg` was uploaded to `data` bucket as `multi/file1`
    And `ignucius.jpg` was uploaded to `data` bucket as `multi/file2`
    When deleting the following keys from `data` bucket at once:
      | multi/file1   |
      | multi/file2   |
      | multi/missing |
      | ../file       |
    Then the following keys are reported as deleted:
      | multi/file1   |
      | multi/file2   |
      | multi/missing |
    And the following keys are reported as `InvalidArgument` errors:
      | ../file |
    And `data/multi/file1` is not stored
    And `data/multi/file2` is not stored

  Scenario: DeleteObjects: quiet mode
    Given `rms.jpg` was uploaded to `data` bucket as `quiet/file`
    When deleting the following keys from `data` bucket at once quietly:
      | quiet/file |
    Then no keys are reported as deleted
    And `data/quiet/file` is not stored
