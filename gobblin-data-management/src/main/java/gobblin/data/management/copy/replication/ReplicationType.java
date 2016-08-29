package gobblin.data.management.copy.replication;

/**
 * An enumeration of possible replication types which is the attribute of {@link ReplicationLocation}
 * 
 * @author mitu
 *
 */
public enum ReplicationType {

  HDFS("hdfs"),
  HIVE("hive");

  private final String name;

  ReplicationType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link ReplicationType} for the given name.
   *
   * @param name the given name
   * @return a {@link ReplicationType} for the given name
   */
  public static ReplicationType forName(String name) {
    if (name.equalsIgnoreCase(HDFS.name)) {
      return HDFS;
    }
    if (name.equalsIgnoreCase(HIVE.name)) {
      return HIVE;
    }
    throw new IllegalArgumentException("No ReplicationType available for name: " + name);
  }

}
