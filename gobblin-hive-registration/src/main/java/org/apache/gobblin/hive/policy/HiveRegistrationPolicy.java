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

package gobblin.hive.policy;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alpha;
import gobblin.hive.spec.HiveSpec;


/**
 * An interface for generating a {@link HiveSpec} for a {@link Path}.
 *
 * @author Ziyang Liu
 */
@Alpha
public interface HiveRegistrationPolicy {

  /**
   * Get a collection of {@link HiveSpec}s for a {@link Path}, which can be used by {@link gobblin.hive.HiveRegister}
   * to register the given {@link Path}.
   */
  public Collection<HiveSpec> getHiveSpecs(Path path) throws IOException;

}
