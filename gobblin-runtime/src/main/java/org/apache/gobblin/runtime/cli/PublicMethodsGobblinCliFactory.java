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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.embedded.EmbeddedGobblin;


/**
 * A helper class for automatically inferring {@link Option}s from the public methods in a class.
 *
 * For each public method in the class to infer with exactly zero or one String parameter, the helper will create
 * an optional {@link Option}. Using the annotation {@link CliObjectOption} the helper can automatically
 * add a description to the {@link Option}. Annotating a method with {@link NotOnCli} will prevent the helper from
 * creating an {@link Option} from it.
 *
 * For an example usage see {@link EmbeddedGobblin.CliFactory}
 */
public abstract class PublicMethodsGobblinCliFactory extends PublicMethodsCliObjectFactory<EmbeddedGobblin>
    implements EmbeddedGobblinCliFactory{

  public PublicMethodsGobblinCliFactory(Class<? extends EmbeddedGobblin> klazz) {
    super(klazz);
  }

  @Override
  public EmbeddedGobblin constructObject(CommandLine cli) throws IOException {
    try {
      return constructEmbeddedGobblin(cli);
    } catch (JobTemplate.TemplateException exc) {
      throw new IOException(exc);
    }
  }

  public abstract EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli) throws JobTemplate.TemplateException, IOException;

}
