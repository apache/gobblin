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

package gobblin.compaction.listeners;

import java.util.Properties;

import com.google.common.base.Optional;

import gobblin.annotation.Alpha;


/**
 * A factory for creating {@link CompactorListener}s.
 */
@Alpha
public interface CompactorListenerFactory {

  /**
   * Creates a {@link CompactorListener}, if none are specified returns {@link Optional#absent()}.
   *
   * @param properties a {@link Properties} object used to create a {@link CompactorListener}.
   *
   * @return {@link Optional#absent()} if no {@link CompactorListener} is present, else returns a {@link CompactorListener}.
   *
   * @throws CompactorListenerCreationException if there is a problem creating the {@link CompactorListener}.
   */
  public Optional<CompactorListener> createCompactorListener(Properties properties)
      throws CompactorListenerCreationException;
}
