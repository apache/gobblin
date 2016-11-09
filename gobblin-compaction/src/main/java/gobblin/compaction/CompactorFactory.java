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

import com.google.common.base.Optional;

import gobblin.annotation.Alpha;
import gobblin.compaction.listeners.CompactorListener;
import gobblin.metrics.Tag;


/**
 * A factory responsible for creating {@link Compactor}s.
 */
@Alpha
public interface CompactorFactory {

    /**
     * Creates a {@link Compactor}.
     *
     * @param properties a {@link Properties} object used to create the {@link Compactor}
     * @param tags a {@link List} of {@link Tag}s used to create the {@link Compactor}.
     * @param compactorListener a {@link CompactorListener} used to create the {@link Compactor}.
     *
     * @return a {@link Compactor}
     *
     * @throws CompactorCreationException if there is a problem creating the {@link Compactor}
     */
    public Compactor createCompactor(Properties properties, List<Tag<String>> tags,
        Optional<CompactorListener> compactorListener) throws CompactorCreationException;
}
