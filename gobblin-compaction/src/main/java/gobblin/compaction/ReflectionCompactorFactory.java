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

package gobblin.compaction;

import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import gobblin.compaction.listeners.CompactorListener;
import gobblin.metrics.Tag;


/**
 * Implementation of {@link CompactorFactory} that creates a {@link Compactor} using reflection.
 */
public class ReflectionCompactorFactory implements CompactorFactory {

  @VisibleForTesting
  static final String COMPACTION_COMPACTOR_CLASS = "compaction.compactor.class";
  private static final String DEFAULT_COMPACTION_COMPACTOR_CLASS = "gobblin.compaction.mapreduce.MRCompactor";

  @Override
  public Compactor createCompactor(Properties properties, List<Tag<String>> tags,
      Optional<CompactorListener> compactorListener) throws CompactorCreationException {

    String compactorClassName = properties.getProperty(COMPACTION_COMPACTOR_CLASS, DEFAULT_COMPACTION_COMPACTOR_CLASS);
    try {
      return (Compactor) ConstructorUtils
          .invokeConstructor(Class.forName(compactorClassName), properties, tags, compactorListener);
    } catch (ReflectiveOperationException e) {
      throw new CompactorCreationException(String
          .format("Unable to create Compactor from key \"%s\" with value \"value\"", COMPACTION_COMPACTOR_CLASS,
              compactorClassName), e);
    }
  }
}
