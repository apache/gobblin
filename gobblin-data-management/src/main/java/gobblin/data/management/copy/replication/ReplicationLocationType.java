package gobblin.data.management.copy.replication;

/**
 * An enumeration of possible replication types which is the attribute of {@link ReplicationLocation}
 * 
 * @author mitu
 *
 */
public enum ReplicationLocationType {

  HADOOPFS("hadoopfs"),
  HIVE("hive");

  private final String name;

  ReplicationLocationType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link ReplicationLocationType} for the given name.
   *
   * @param name the given name
   * @return a {@link ReplicationLocationType} for the given name
   */
  public static ReplicationLocationType forName(String name) {
    return ReplicationLocationType.valueOf(name.toUpperCase());
  }

}
