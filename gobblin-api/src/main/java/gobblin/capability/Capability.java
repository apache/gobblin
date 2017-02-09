package gobblin.capability;

import gobblin.annotation.Alpha;


/**
 * Represents a set of functionality a job-creator can ask for. Examples could include
 * encryption, compression, partitioning...
 *
 * Each Capability has a name and then a set of associated configuration properties. An example is
 * the encryption algorithm to use.
 */
@Alpha
public class Capability {
  private final String name;
  private final boolean critical;

  /**
   * Partitioning capability.
   * Expected properties:
   *  partitionSchema: Schema output by the selected partitioner
   */
  public static final Capability PARTITIONED_WRITER = new Capability("PARTITIONED_WRITER", true);

  /**
   * Encryption capability.
   * Expected properties:
   *   type: Type of encryption. Can be "any" which lets writer choose, an encryption algorithm, or "false" to disable.
   *         If not present we assume encryption is disabled for this fork.
   */
  public static final Capability ENCRYPTION = new Capability("ENCRYPTION", true);

  /**
   * Create a new Capability description.
   * @param name Name of the capability
   * @param critical If a capability is marked critical and a job is configured with components that can't satisfy
   *                 the capability, Gobblin will refuse to run the job. If it is not critical then Gobblin will simply
   *                 issue a warning.
   */
  public Capability(String name, boolean critical) {
    this.name = name;
    this.critical = critical;
  }

  public String getName() {
    return name;
  }

  public boolean isCritical() {
    return critical;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Capability that = (Capability) o;

    if (critical != that.critical) {
      return false;
    }
    return name != null ? name.equals(that.name) : that.name == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (critical ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Capability{" + "name='" + name + '\'' + ", critical=" + critical + '}';
  }
}
