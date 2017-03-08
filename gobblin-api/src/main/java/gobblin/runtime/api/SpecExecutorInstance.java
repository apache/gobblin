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

package gobblin.runtime.api;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Future;

import com.typesafe.config.Config;

import gobblin.annotation.Alpha;


/**
 * Defines a SpecExecutorInstance (typically a standalone instance, cluster or Azkaban deployment)
 * that can execute a {@link Spec}.
 */
@Alpha
public interface SpecExecutorInstance {
  /** An URI identifying the SpecExecutorInstance. */
  URI getUri();

  /** Human-readable description of the SpecExecutorInstance .*/
  Future<String> getDescription();

  /** SpecExecutorInstance config as a typesafe config object. */
  Future<Config> getConfig();

  /** Health of SpecExecutorInstance. */
  Future<String> getHealth();

  /** Source : Destination processing capabilities of SpecExecutorInstance. */
  Future<? extends Map<String, String>> getCapabilities();

  public static enum Verb {
    ADD(1, "add"),
    UPDATE(2, "update"),
    DELETE(3, "delete");

    private int _id;
    private String _verb;

    Verb(int id, String verb) {
      _id = id;
      _verb = verb;
    }

    public int getId() {
      return _id;
    }

    public String getVerb() {
      return _verb;
    }
  }
}
