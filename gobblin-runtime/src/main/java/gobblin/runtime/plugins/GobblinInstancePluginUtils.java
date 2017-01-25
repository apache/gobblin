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

package gobblin.runtime.plugins;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.runtime.api.GobblinInstancePluginFactory;
import gobblin.util.ClassAliasResolver;


/**
 * Utilities to instantiate {@link GobblinInstancePluginFactory}s.
 */
public class GobblinInstancePluginUtils {

  private static final ClassAliasResolver<GobblinInstancePluginFactory> RESOLVER =
      new ClassAliasResolver<>(GobblinInstancePluginFactory.class);

  public static final String PLUGINS_KEY = PluginStaticKeys.INSTANCE_CONFIG_PREFIX + "pluginAliases";

  /**
   * Parse a collection of {@link GobblinInstancePluginFactory} from the system configuration by reading the key
   * {@link #PLUGINS_KEY}.
   */
  public static Collection<GobblinInstancePluginFactory> instantiatePluginsFromSysConfig(Config config)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String pluginsStr = config.getString(PLUGINS_KEY);
    List<GobblinInstancePluginFactory> plugins = Lists.newArrayList();
    for (String pluginName : Splitter.on(",").split(pluginsStr)) {
      plugins.add(instantiatePluginByAlias(pluginName));
    }
    return plugins;
  }

  /**
   * Instantiate a {@link GobblinInstancePluginFactory} by alias.
   */
  public static GobblinInstancePluginFactory instantiatePluginByAlias(String alias)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return RESOLVER.resolveClass(alias).newInstance();
  }

}
