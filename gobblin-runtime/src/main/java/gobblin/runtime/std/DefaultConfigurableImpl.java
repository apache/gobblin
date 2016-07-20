/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.std;

import java.util.Properties;

import com.typesafe.config.Config;

import gobblin.runtime.api.Configurable;
import gobblin.util.ConfigUtils;

/**
 * Default immutable implementation for {@link Configurable} interface.
 */
public class DefaultConfigurableImpl implements Configurable {
  final Config _config;
  final Properties _props;

  protected DefaultConfigurableImpl(Config config, Properties props) {
    _config = config;
    _props = props;
  }

  /** {@inheritDoc} */
  @Override
  public Config getConfig() {
    return _config;
  }

  /** {@inheritDoc} */
  @Override
  public Properties getConfigAsProperties() {
    return _props;
  }

  public static DefaultConfigurableImpl createFromConfig(Config srcConfig) {
    return new DefaultConfigurableImpl(srcConfig, ConfigUtils.configToProperties(srcConfig));
  }

  public static DefaultConfigurableImpl createFromProperties(Properties srcConfig) {
    return new DefaultConfigurableImpl(convertPropertiesToConfig(srcConfig), srcConfig);
  }

  public static Config convertPropertiesToConfig(Properties srcConfig) {
    return ConfigUtils.propertiesToConfig(srcConfig);
  }
}
