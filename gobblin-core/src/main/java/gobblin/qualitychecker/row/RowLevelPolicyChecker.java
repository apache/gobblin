/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Strings;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.HadoopUtils;


public class RowLevelPolicyChecker implements Closeable, FinalState {

  private final List<RowLevelPolicy> list;
  private final String stateId;
  private final FileSystem fs;
  private boolean errFileOpen;
  private final Closer closer;
  private RowLevelErrFileWriter writer;

  public RowLevelPolicyChecker(List<RowLevelPolicy> list, String stateId, FileSystem fs) {
    this.list = list;
    this.stateId = stateId;
    this.fs = fs;
    this.errFileOpen = false;
    this.closer = Closer.create();
    this.writer = this.closer.register(new RowLevelErrFileWriter(this.fs));
  }

  public boolean executePolicies(Object record, RowLevelPolicyCheckResults results) throws IOException {
    for (RowLevelPolicy p : this.list) {
      RowLevelPolicy.Result result = p.executePolicy(record);
      results.put(p, result);

      if (result.equals(RowLevelPolicy.Result.FAILED)) {
        if (p.getType().equals(RowLevelPolicy.Type.FAIL)) {
          throw new RuntimeException("RowLevelPolicy " + p + " failed on record " + record);
        } else if (p.getType().equals(RowLevelPolicy.Type.ERR_FILE)) {
          if (!this.errFileOpen) {
            this.writer.open(getErrFilePath(p));
            this.writer.write(record);
          } else {
            this.writer.write(record);
          }
          this.errFileOpen = true;
        }
        return false;
      }
    }
    return true;
  }

  private Path getErrFilePath(RowLevelPolicy policy) {
    String errFileName = HadoopUtils.sanitizePath(policy.toString(), "-");
    if (!Strings.isNullOrEmpty(this.stateId)) {
      errFileName += "-" + this.stateId;
    }
    errFileName += ".err";
    return new Path(policy.getErrFileLocation(), errFileName);
  }

  @Override
  public void close() throws IOException {
    if (this.errFileOpen) {
      this.closer.close();
    }
  }

  /**
   * Get final state for this object, obtained by merging the final states of the
   * {@link gobblin.qualitychecker.row.RowLevelPolicy}s used by this object.
   * @return Merged {@link gobblin.configuration.State} of final states for
   *                {@link gobblin.qualitychecker.row.RowLevelPolicy} used by this checker.
   */
  @Override
  public State getFinalState() {
    State state = new State();
    for (RowLevelPolicy policy : this.list) {
      state.addAll(policy.getFinalState());
    }
    return state;
  }
}
