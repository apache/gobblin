package gobblin.data.management.copy.replication;

import java.util.Map;

import com.google.common.base.Optional;

import lombok.Data;


/**
 * Class used to represent the meta data of the replication
 * @author mitu
 *
 */

@Data
public class ReplicationMetaData {

  private final Optional<Map<String, String>> values;
}
