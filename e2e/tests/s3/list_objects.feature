Feature: ListObjects

  Scenario Outline: <op>: keys in lexicographic order
    Given the following objects were uploaded to a new bucket:
      | a/b   |
      | a-b   |
      | a0    |
      | a/a   |
      | Z     |
      | b/c/d |
    When listing the new bucket with <op>
    Then the following keys are listed:
      | Z     |
      | a-b   |
      | a/a   |
      | a/b   |
      | a0    |
      | b/c/d |
    And no common prefixes are listed
    And `a/b` is listed with the size of `rms.jpg`
    And `a/b` is listed with the same metadata as `HeadObject` returns

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: prefix
    Given the following objects were uploaded to a new bucket:
      | a/b  |
      | a-b  |
      | ab/c |
      | b    |
    When listing the new bucket with <op> by prefix `a/`
    Then the following keys are listed:
      | a/b |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: delimiter
    Given the following objects were uploaded to a new bucket:
      | a/b/c |
      | a/b/d |
      | a/e   |
      | a-b   |
      | f     |
    And `g/empty` empty directory was created in the new bucket
    When listing the new bucket with <op> by delimiter `/`
    Then the following keys are listed:
      | a-b |
      | f   |
    And the following common prefixes are listed:
      | a/ |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: multiple common prefixes
    Given the following objects were uploaded to a new bucket:
      | a/1 |
      | b   |
      | c/1 |
      | c/2 |
      | d/1 |
      | e   |
    When listing the new bucket with <op> by delimiter `/`
    Then the following keys are listed:
      | b |
      | e |
    And the following common prefixes are listed:
      | a/ |
      | c/ |
      | d/ |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: prefix and delimiter
    Given the following objects were uploaded to a new bucket:
      | a/b/c |
      | a/b/d |
      | a/e   |
      | f     |
    When listing the new bucket with <op> by prefix `a/` by delimiter `/`
    Then the following keys are listed:
      | a/e |
    And the following common prefixes are listed:
      | a/b/ |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: pagination
    Given the following objects were uploaded to a new bucket:
      | k1 |
      | k2 |
      | k3 |
      | k4 |
      | k5 |
    When listing the new bucket with <op> by 2 keys per page
    Then it's listed in 3 pages
    And the following keys are listed:
      | k1 |
      | k2 |
      | k3 |
      | k4 |
      | k5 |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: pagination over common prefixes
    Given the following objects were uploaded to a new bucket:
      | a/1 |
      | a/2 |
      | b   |
      | c/1 |
      | c/2 |
      | d   |
    When listing the new bucket with <op> by delimiter `/` by 1 keys per page
    Then it's listed in 4 pages
    And the following keys are listed:
      | b |
      | d |
    And the following common prefixes are listed:
      | a/ |
      | c/ |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: symlinks
    Given the following objects were uploaded to a new bucket:
      | dir/target |
      | dir/gone   |
    And `link` symlink was created in the new bucket pointing to `dir/target`
    And `dangling` symlink was created in the new bucket pointing to `dir/gone`
    And `dir/gone` was deleted from the new bucket
    When listing the new bucket with <op>
    Then the following keys are listed:
      | dir/target |
      | link       |
    And `link` is listed with the size of `rms.jpg`

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: special characters in keys
    Given the following objects were uploaded to a new bucket:
      | dir/a b+c%d |
    When listing the new bucket with <op>
    Then the following keys are listed:
      | dir/a b+c%d |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: URL encoding
    Given the following objects were uploaded to a new bucket:
      | dir/a b+c |
    When listing the new bucket with <op> with URL encoding
    Then the following keys are listed:
      | dir/a%20b%2Bc |

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: empty bucket
    Given the new bucket was created empty
    When listing the new bucket with <op> by delimiter `/`
    Then no keys are listed
    And no common prefixes are listed

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: bucket emptied by deletion
    Given the following objects were uploaded to a new bucket:
      | dir/file |
    And `dir/file` was deleted from the new bucket
    When listing the new bucket with <op>
    Then no keys are listed

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |

  Scenario Outline: <op>: missing bucket
    When listing the new bucket with <op>
    Then no keys are listed
    And no common prefixes are listed

    Examples:
      | op            |
      | ListObjects   |
      | ListObjectsV2 |
