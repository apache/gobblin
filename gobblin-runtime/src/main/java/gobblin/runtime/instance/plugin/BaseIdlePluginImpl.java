/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.instance.plugin;

import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.annotation.Alias;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.GobblinInstancePlugin;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A base implementation of a plugin that works as {@link AbstractIdleService}. Subclasses must
 * implement the startUp() method and can optionally override shutDown().
 *
 */
@AllArgsConstructor
public abstract class BaseIdlePluginImpl extends AbstractIdleService implements GobblinInstancePlugin {
  @Getter protected final GobblinInstanceDriver instance;

  /** {@inheritDoc} */
  @Override
  protected void shutDown() throws Exception {
    instance.getLog().info("Plugin shutdown: " + this);
  }

  @Override
  public String toString() {
    Alias alias = getClass().getAnnotation(Alias.class);

    return null != alias ? alias.value() : getClass().getName();
  }

}
