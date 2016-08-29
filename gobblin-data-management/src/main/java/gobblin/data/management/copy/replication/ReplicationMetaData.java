package gobblin.data.management.copy.replication;

import com.google.common.base.Optional;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class used to represent the meta data of the replication
 * @author mitu
 *
 */

@AllArgsConstructor
public class ReplicationMetaData {

  @Getter
  private final Optional<String> jira;

  @Getter
  private final Optional<String> owner;

  @Getter
  private final Optional<String> name;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (jira.isPresent()) {
      sb.append("jira:" + jira.get() + ", ");
    }
    if (owner.isPresent()) {
      sb.append("owner:" + owner.get() + ", ");
    }
    if (name.isPresent()) {
      sb.append("name:" + name.get() + ", ");
    }

    return sb.toString();
  }
}
