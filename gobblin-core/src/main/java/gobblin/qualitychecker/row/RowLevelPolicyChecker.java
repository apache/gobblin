/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.fs.Path;


public class RowLevelPolicyChecker implements Closeable {

  private final List<RowLevelPolicy> list;
  private boolean errFileOpen;
  private RowLevelErrFileWriter writer;

  public RowLevelPolicyChecker(List<RowLevelPolicy> list) {
    this.list = list;
    this.errFileOpen = false;
    this.writer = new RowLevelErrFileWriter();
  }

  public boolean executePolicies(Object record, RowLevelPolicyCheckResults results)
      throws IOException {
    for (RowLevelPolicy p : this.list) {
      RowLevelPolicy.Result result = p.executePolicy(record);
      results.put(p, result);

      if (p.getType().equals(RowLevelPolicy.Type.FAIL) && result.equals(RowLevelPolicy.Result.FAILED)) {
        throw new RuntimeException("RowLevelPolicy " + p + " failed on record " + record);
      }

      if (p.getType().equals(RowLevelPolicy.Type.ERR_FILE)) {
        if (!errFileOpen) {
          this.writer.open(new Path(p.getErrFileLocation(), p.toString().replaceAll("\\.", "-") + ".err"));
          this.writer.write(record);
        } else {
          this.writer.write(record);
        }
        errFileOpen = true;
        return false;
      }
    }
    return true;
  }

  @Override
  public void close()
      throws IOException {
    if (errFileOpen) {
      this.writer.close();
    }
  }
}
