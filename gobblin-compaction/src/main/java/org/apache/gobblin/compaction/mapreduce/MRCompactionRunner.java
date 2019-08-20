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

package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import org.apache.gobblin.compaction.CliOptions;
import org.apache.gobblin.compaction.Compactor;
import org.apache.gobblin.compaction.CompactorCreationException;
import org.apache.gobblin.compaction.CompactorFactory;
import org.apache.gobblin.compaction.ReflectionCompactorFactory;
import org.apache.gobblin.compaction.listeners.CompactorListener;
import org.apache.gobblin.compaction.listeners.CompactorListenerCreationException;
import org.apache.gobblin.compaction.listeners.CompactorListenerFactory;
import org.apache.gobblin.compaction.listeners.ReflectionCompactorListenerFactory;
import org.apache.gobblin.metrics.Tag;

/**
 * A class for launching a Gobblin MR job for compaction through command line.
 *
 * @author Lorand Bendig
 * @deprecated Please use {@link org.apache.gobblin.compaction.mapreduce.MRCompactionTask}
 *  and {@link org.apache.gobblin.compaction.source.CompactionSource} to launch MR instead.
 *  The new way enjoys simpler logic to trigger the compaction flow and more reliable verification criteria,
 *  instead of using timestamp only before.
 *
 */
@Deprecated
public class MRCompactionRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactionRunner.class);

  private final Properties properties;
  private final Compactor compactor;

  public MRCompactionRunner(Properties properties) {
    this.properties = properties;
    this.compactor = getCompactor(getCompactorFactory(), getCompactorListener(getCompactorListenerFactory()));
  }

  public static void main(String[] args)
      throws IOException, ConfigurationException, ParseException, URISyntaxException {

    Properties jobProperties = CliOptions.parseArgs(MRCompactionRunner.class, args, new Configuration());
    MRCompactionRunner compactionRunner = new MRCompactionRunner(jobProperties);
    compactionRunner.compact();
  }

  public void compact() throws IOException {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          compactor.cancel();
        } catch (IOException e) {
          LOG.warn("Unable to cancel the compactor jobs!", e);
        }
      }
    });
    try {
      compactor.compact();
    } catch (Exception e) {
      compactor.cancel();
    }
  }

  protected CompactorListenerFactory getCompactorListenerFactory() {
    return new ReflectionCompactorListenerFactory();
  }

  protected CompactorFactory getCompactorFactory() {
    return new ReflectionCompactorFactory();
  }

  private Compactor getCompactor(CompactorFactory compactorFactory, Optional<CompactorListener> compactorListener) {
    try {
      return compactorFactory.createCompactor(this.properties, new ArrayList<Tag<String>>(), compactorListener);
    } catch (CompactorCreationException e) {
      throw new RuntimeException("Unable to create compactor", e);
    }
  }

  private Optional<CompactorListener> getCompactorListener(CompactorListenerFactory compactorListenerFactory) {
    try {
      return compactorListenerFactory.createCompactorListener(this.properties);
    } catch (CompactorListenerCreationException e) {
      throw new RuntimeException("Unable to create compactor listener", e);
    }
  }

}
