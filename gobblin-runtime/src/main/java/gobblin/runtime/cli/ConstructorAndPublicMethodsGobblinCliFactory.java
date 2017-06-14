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

package gobblin.runtime.cli;

import org.apache.commons.cli.Option;

import gobblin.runtime.embedded.EmbeddedGobblin;


/**
 * A helper class for automatically inferring {@link Option}s from the constructor and public methods in a class.
 *
 * For method inference, see {@link PublicMethodsGobblinCliFactory}.
 *
 * {@link Option}s are inferred from the constructor as follows:
 * 1. The helper will search for exactly one constructor with only String arguments and which is annotated with
 *    {@link CliObjectSupport}.
 * 2. For each parameter of the constructor, the helper will create a required {@link Option}.
 *
 * For an example usage see {@link EmbeddedGobblin.CliFactory}.
 */
public abstract class ConstructorAndPublicMethodsGobblinCliFactory
    extends ConstructorAndPublicMethodsCliObjectFactory<EmbeddedGobblin> implements EmbeddedGobblinCliFactory {
  public ConstructorAndPublicMethodsGobblinCliFactory(Class<? extends EmbeddedGobblin> klazz) {
    super(klazz);
  }
}
