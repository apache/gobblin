package gobblin.data.management.copy.replication;

import com.google.common.base.Objects;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class CopyRoute {

  private final EndPoint copyFrom;
  private final EndPoint copyTo;
  
  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("copyFrom", this.getCopyFrom()).add("copyTo", this.getCopyTo())
        .toString();
  }
}
