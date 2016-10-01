package gobblin.data.management.copy.replication;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class CopyRoute {

  private final EndPoint copyFrom;
  private final EndPoint copyTo;
}
