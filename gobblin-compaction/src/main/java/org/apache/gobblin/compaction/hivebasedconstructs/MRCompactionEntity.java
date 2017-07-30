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

package gobblin.compaction.hivebasedconstructs;

import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import lombok.Getter;


/**
 * Entity that stores information required for launching an {@link gobblin.compaction.mapreduce.MRCompactor} job
 *
 * {@link #primaryKeyList}: Comma delimited list of fields to use as primary key
 * {@link #deltaList}: Comma delimited list of fields to use as deltaList
 * {@link #dataFilesPath}: Location of files associated with table
 * {@link #props}: Other properties to be passed to {@link gobblin.compaction.mapreduce.MRCompactor}
 */
@Getter
public class MRCompactionEntity {
  private final List<String> primaryKeyList;
  private final List<String> deltaList;
  private final Path dataFilesPath;
  private final Properties props;

  public MRCompactionEntity(List<String> primaryKeyList, List<String> deltaList, Path dataFilesPath, Properties props) {
    this.primaryKeyList = primaryKeyList;
    this.deltaList = deltaList;
    this.dataFilesPath = dataFilesPath;
    this.props = props;
  }
}
