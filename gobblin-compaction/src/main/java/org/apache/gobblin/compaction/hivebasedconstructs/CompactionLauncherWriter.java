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

package org.apache.gobblin.compaction.hivebasedconstructs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Joiner;
import org.apache.gobblin.compaction.listeners.CompactorListener;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.compaction.mapreduce.avro.ConfBasedDeltaFieldProvider;


/**
 * {@link DataWriter} that launches an {@link MRCompactor} job given an {@link MRCompactionEntity} specifying config
 * for compaction
 *
 * Delta field is passed using {@link ConfBasedDeltaFieldProvider}
 */
public class CompactionLauncherWriter implements DataWriter<MRCompactionEntity> {
  @Override
  public void write(MRCompactionEntity compactionEntity) throws IOException {
    Preconditions.checkNotNull(compactionEntity);

    List<? extends Tag<?>> list = new ArrayList<>();

    Properties props = new Properties();
    props.putAll(compactionEntity.getProps());
    props.setProperty(ConfBasedDeltaFieldProvider.DELTA_FIELDS_KEY,
        Joiner.on(',').join(compactionEntity.getDeltaList()));
    props.setProperty(MRCompactor.COMPACTION_INPUT_DIR, compactionEntity.getDataFilesPath().toString());

    String dbTableName = compactionEntity.getDataFilesPath().getName();
    String timestamp = String.valueOf(System.currentTimeMillis());

    props.setProperty(MRCompactor.COMPACTION_TMP_DEST_DIR,
        new Path(props.getProperty(MRCompactor.COMPACTION_TMP_DEST_DIR), dbTableName).toString());
    props.setProperty(MRCompactor.COMPACTION_DEST_DIR,
        new Path(new Path(props.getProperty(MRCompactor.COMPACTION_DEST_DIR), dbTableName), timestamp).toString());

    MRCompactor compactor = new MRCompactor(props, list, Optional.<CompactorListener>absent());
    compactor.compact();
  }

  @Override
  public void commit() throws IOException {}

  @Override
  public void cleanup() throws IOException {}

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
