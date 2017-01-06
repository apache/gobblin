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

package gobblin.publisher;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Preconditions;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Path expected from writer:
 * {writerfinaldir}/{topicname}/{dbname_tablename_xxxxx}
 *
 * Publisher output path:
 * {publisherfinaldir}/{dbname.tablename}/{currenttimestamp}
 */
public class TimestampDataPublisher extends BaseDataPublisher {

  private final String timestamp;

  public TimestampDataPublisher(State state) throws IOException {
    super(state);
    timestamp = String.valueOf(System.currentTimeMillis());
  }

  /**
   * Make sure directory exists before running {@link BaseDataPublisher#publishData(WorkUnitState, int, boolean, Set)}
   * so that tables will be moved one at a time rather than all at once
   */
  @Override
  protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
      Set<Path> writerOutputPathsMoved) throws IOException {
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);
    if (!this.publisherFileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
          publisherOutputDir, this.permissions.get(branchId));
    }
    super.publishData(state, branchId, publishSingleTaskData, writerOutputPathsMoved);
  }

  /**
   * Update destination path to put db and table name in format "dbname.tablename" using {@link #getDbTableName(String)}
   * and include timestamp
   *
   * Input dst format: {finaldir}/{schemaName}
   * Output dst format: {finaldir}/{dbname.tablename}/{currenttimestamp}
   */
  @Override
  protected void movePath(ParallelRunner parallelRunner, State state, Path src, Path dst, int branchId)
      throws IOException {

    String outputDir = dst.getParent().toString();
    String schemaName = dst.getName();
    Path newDst = new Path(new Path(outputDir, getDbTableName(schemaName)), timestamp);

    if (!this.publisherFileSystemByBranches.get(branchId).exists(newDst)) {
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
          newDst.getParent(), this.permissions.get(branchId));
    }

    super.movePath(parallelRunner, state, src, newDst, branchId);
  }

  /**
   * Translate schema name to "dbname.tablename" to use in path
   *
   * @param schemaName In format "dbname_tablename_xxxxx"
   * @return db and table name in format "dbname.tablename"
   */
  private String getDbTableName(String schemaName) {
    Preconditions.checkArgument(schemaName.matches(".+_.+_.+"));
    return schemaName.replaceFirst("_", ".").substring(0, schemaName.lastIndexOf('_'));
  }
}
