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

package gobblin.hive.spec;

import java.util.Collection;

import com.google.common.base.Predicate;

import gobblin.annotation.Alpha;
import gobblin.hive.HiveRegister;


/**
 * A {@link HiveSpec} with a set of {@link Predicate}s. If any of the {@link Predicate}s returns false,
 * the Hive registration will be skipped.
 */
@Alpha
public interface HiveSpecWithPredicates extends HiveSpec {

  /**
   * A {@link Collection} of {@link Predicate}s.  If any of the {@link Predicate}s returns false,
   * the Hive registration will be skipped.
   */
  public Collection<Predicate<HiveRegister>> getPredicates();

}
