package gobblin.data.management.copy.replication;

public enum ReplicationCopyMode {
  PULL("pull"),
  PUSH("push");

  private final String name;

  ReplicationCopyMode(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link ReplicationCopyMode} for the given name.
   *
   * @param name the given name
   * @return a {@link ReplicationCopyMode} for the given name
   */
  public static ReplicationCopyMode forName(String name) {
    return ReplicationCopyMode.valueOf(name.toUpperCase());
  }

}
