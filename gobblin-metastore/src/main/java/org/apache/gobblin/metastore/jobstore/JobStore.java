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

package org.apache.gobblin.metastore.jobstore;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.gobblin.rest.Job;

import java.io.Closeable;

/**
 * An interface for stores that store job configuration information.
 */
public interface JobStore extends Closeable {

    Job create(Job job) throws Exception;

    Job get(String jobName) throws Exception;

    boolean update(Job job) throws Exception;

    default boolean  mergedUpdate(Job job) throws Exception {
        throw new NotImplementedException("This method is not implemented");
    }

    boolean delete(String jobName) throws Exception;

}
