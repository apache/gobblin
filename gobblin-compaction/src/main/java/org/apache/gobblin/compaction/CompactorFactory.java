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

package org.apache.gobblin.compaction;

import java.util.List;
import java.util.Properties;

import com.google.common.base.Optional;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.compaction.listeners.CompactorListener;
import org.apache.gobblin.metrics.Tag;


/**
 * A factory responsible for creating {@link Compactor}s.
 * @deprecated Please use {@link org.apache.gobblin.compaction.mapreduce.MRCompactionTask}
 *  * and {@link org.apache.gobblin.compaction.source.CompactionSource} to launch MR instead.
 *  * The new way enjoys simpler logic to trigger the compaction flow and more reliable verification criteria,
 *  * instead of using timestamp only before.
 */
@Alpha
@Deprecated
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
