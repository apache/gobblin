package org.apache.gobblin.temporal.ddm.work;


import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


@NonNull
@RequiredArgsConstructor
@NoArgsConstructor
public class DatasetStats {
  @NonNull
  private long recordsWritten;
  @NonNull private long bytesWritten;
  @NonNull private boolean successfullyCommitted;
}
