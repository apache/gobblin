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

package org.apache.gobblin.runtime.instance.hadoop;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

/**
 * A helper class to load hadoop configuration with possible overrides in typesafe config.
 */
public class HadoopConfigLoader {
  /**
   * Properties in this context will be added to the Hadoop configuration. One can add a suffix
   * ".ROOT" to a property which will be automatically stripped. This is to avoid an incompatibility
   * between typesafe config and Properties where the latter allows "a.b" and "a.b.c" but the former
   * does not. In that case, one can use "a.b.ROOT" and "a.b.c".
   */
  public static final String HADOOP_CONF_OVERRIDES_ROOT = "hadoop-inject";
  public static final String STRIP_SUFFIX = ".ROOT";

  private final Configuration _conf = new Configuration();

  public HadoopConfigLoader() {
    this(ConfigFactory.load());
  }

  public HadoopConfigLoader(Config rootConfig) {
    if (rootConfig.hasPath(HADOOP_CONF_OVERRIDES_ROOT)) {
      addOverrides(_conf, rootConfig.getConfig(HADOOP_CONF_OVERRIDES_ROOT));
    }
  }

  /** Get a copy of the Hadoop configuration with any overrides applied. Note that this is a private
   * copy of the configuration and can be further modified. */
  public Configuration getConf() {
    return new Configuration(_conf);
  }

  static void addOverrides(Configuration conf, Config config) {
    for(Map.Entry<String, ConfigValue> entry: config.entrySet()) {
      String propName = entry.getKey();
      if (propName.endsWith(STRIP_SUFFIX)) {
        propName = propName.substring(0, propName.length() - STRIP_SUFFIX.length());
      }
      conf.set(propName, entry.getValue().unwrapped().toString());
    }
  }


}
