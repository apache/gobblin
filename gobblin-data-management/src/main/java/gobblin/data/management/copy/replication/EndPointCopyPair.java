package gobblin.data.management.copy.replication;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class EndPointCopyPair {

  private final EndPoint copyFrom;
  private final EndPoint copyTo;
}
