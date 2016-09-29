package gobblin.data.management.copy.replication;

import lombok.Data;

@Data
public class CopyPair {

  private final EndPoint copyFrom;
  private final EndPoint copyTo;
}
