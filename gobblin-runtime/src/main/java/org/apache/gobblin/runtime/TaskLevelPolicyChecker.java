public static final String TASK_LEVEL_POLICY_RESULT_KEY = "task.level.policy.result";

/**
 * Enum representing the possible data quality status values.
 */
public enum DataQualityStatus {
  PASSED("PASSED"),
  FAILED("FAILED");

  private final String value;

  DataQualityStatus(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public static DataQualityStatus fromValue(String value) {
    for (DataQualityStatus status : values()) {
      if (status.value.equals(value)) {
        return status;
      }
    }
    throw new IllegalArgumentException("Invalid data quality status: " + value);
  }
}

private final List<TaskLevelPolicy> list; 