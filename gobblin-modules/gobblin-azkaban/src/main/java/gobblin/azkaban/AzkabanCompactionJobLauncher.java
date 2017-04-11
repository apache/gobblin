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

package gobblin.azkaban;

import java.io.IOException;
import java.util.Properties;

import azkaban.jobExecutor.AbstractJob;

import com.google.common.base.Optional;

import org.apache.log4j.Logger;

import gobblin.compaction.Compactor;
import gobblin.compaction.CompactorFactory;
import gobblin.compaction.CompactorCreationException;
import gobblin.compaction.listeners.CompactorListener;
import gobblin.compaction.listeners.CompactorListenerCreationException;
import gobblin.compaction.listeners.CompactorListenerFactory;
import gobblin.compaction.ReflectionCompactorFactory;
import gobblin.compaction.listeners.ReflectionCompactorListenerFactory;
import gobblin.metrics.Tag;


/**
 * A class for launching a Gobblin MR job for compaction through Azkaban.
 */
public class AzkabanCompactionJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanCompactionJobLauncher.class);

  private final Properties properties;
  private final Compactor compactor;

  public AzkabanCompactionJobLauncher(String jobId, Properties props) {
    super(jobId, LOG);
    this.properties = new Properties();
    this.properties.putAll(props);
    this.compactor = getCompactor(getCompactorFactory(), getCompactorListener(getCompactorListenerFactory()));
  }

  private Compactor getCompactor(CompactorFactory compactorFactory, Optional<CompactorListener> compactorListener) {
    try {
      return compactorFactory
          .createCompactor(this.properties, Tag.fromMap(AzkabanTags.getAzkabanTags()), compactorListener);
    } catch (CompactorCreationException e) {
      throw new RuntimeException("Unable to create compactor", e);
    }
  }

  protected CompactorFactory getCompactorFactory() {
    return new ReflectionCompactorFactory();
  }

  private Optional<CompactorListener> getCompactorListener(CompactorListenerFactory compactorListenerFactory) {
    try {
      return compactorListenerFactory.createCompactorListener(this.properties);
    } catch (CompactorListenerCreationException e) {
      throw new RuntimeException("Unable to create compactor listener", e);
    }
  }

  protected CompactorListenerFactory getCompactorListenerFactory() {
    return new ReflectionCompactorListenerFactory();
  }

  @Override
  public void run() throws Exception {
    this.compactor.compact();
  }

  @Override
  public void cancel() throws IOException {
    this.compactor.cancel();
  }
}
