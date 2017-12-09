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
package org.apache.gobblin.util.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;

/**
 * Hello world!
 */
public class HelloWorldSource implements Source<String, String> {
  public static final String CONFIG_NAMESPACE = "gobblin.source.helloWorld";
  public static final String NUM_HELLOS_KEY = "numHellos";
  public static final String NUM_HELLOS_FULL_KEY = CONFIG_NAMESPACE + "." + NUM_HELLOS_KEY;
  public static final int DEFAULT_NUM_HELLOS = 1;
  public static final String HELLO_ID_KEY = "helloId";
  public static final String HELLO_ID_FULL_KEY = CONFIG_NAMESPACE +"." + HELLO_ID_KEY;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Config rootCfg = ConfigUtils.propertiesToConfig(state.getProperties());
    Config cfg = rootCfg.hasPath(CONFIG_NAMESPACE) ? rootCfg.getConfig(CONFIG_NAMESPACE) :
          ConfigFactory.empty();
    int numHellos = cfg.hasPath(NUM_HELLOS_KEY) ? cfg.getInt(NUM_HELLOS_KEY) : DEFAULT_NUM_HELLOS;

    Extract extract = new Extract(TableType.APPEND_ONLY,
         HelloWorldSource.class.getPackage().getName(),
         HelloWorldSource.class.getSimpleName());
    List<WorkUnit> wus = new ArrayList<>(numHellos);
    for (int i = 1; i <= numHellos; ++i) {
      WorkUnit wu = new WorkUnit(extract);
      wu.setProp(HELLO_ID_FULL_KEY, i);
      wus.add(wu);
    }

    return wus;
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) {
    int helloId = state.getPropAsInt(HELLO_ID_FULL_KEY);
    return new ExtractorImpl(helloId);
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }

  public static class ExtractorImpl implements Extractor<String, String> {
    private final int _helloId;
    private int _recordsEmitted = 0;

    public ExtractorImpl(int helloId) {
      _helloId = helloId;
    }

    @Override public void close() throws IOException {
      // Nothing to do
    }

    @Override public String getSchema() throws IOException {
      return "string";
    }

    @Override public String readRecord(String reuse) {
      if (_recordsEmitted > 0) {
        return null;
      }
      ++_recordsEmitted;
      return helloMessage(_helloId);
    }

    public static String helloMessage(int helloId) {
      return "Hello world " + helloId + " !";
    }

    @Override public long getExpectedRecordCount() {
      return 1;
    }

    @Override public long getHighWatermark() {
      return 0;
    }

  }

}
