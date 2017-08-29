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

package org.apache.gobblin.runtime.api;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Future;

import com.typesafe.config.Config;


/**
 * Defines a representation of JobSpec-Executor in GaaS.
 * A triplet of <Technology, location, communication mechanism> uniquely defines an object of SpecExecutor.
 * e.g. <Lumos, Holdem, Rest> represents a Executor that moves data by Lumos, running on Holdem can be reached by Rest.
 */
public interface SpecExecutor {
  /** An URI identifying the SpecExecutor. */
  URI getUri();

  /** Human-readable description of the SpecExecutor .*/
  Future<String> getDescription();

  /** SpecExecutor config as a typesafe config object. */
  Future<Config> getConfig();

  /** SpecExecutor attributes include Location of SpecExecutor and the Type of it (Technology it used for data movement,
   * like, gobblin-standalone/gobblin-cluster
   * SpecExecutor attributes are supposed to be read-only once instantiated.
   * */
  Config getAttrs();

  /** Health of SpecExecutor. */
  Future<String> getHealth();

  /** Source : Destination processing capabilities of SpecExecutor. */
  Future<? extends Map<ServiceNode, ServiceNode>> getCapabilities();

  /** A communication socket for generating spec to assigned physical executors, paired with
   * a consumer on the physical executor side. */
  Future<? extends SpecProducer> getProducer();

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