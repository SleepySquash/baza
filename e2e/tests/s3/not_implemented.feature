Feature: Not implemented methods

  Scenario: HeadBucket: not implemented
    When checking whether `data` bucket exists
    Then `NotImplemented` error is returned
