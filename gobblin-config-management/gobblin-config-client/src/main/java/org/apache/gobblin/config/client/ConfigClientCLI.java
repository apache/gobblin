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

package gobblin.config.client;

import java.net.URI;
import java.net.URISyntaxException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import gobblin.annotation.Alias;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.runtime.cli.CliApplication;
import gobblin.runtime.cli.CliObjectFactory;
import gobblin.runtime.cli.CliObjectSupport;
import gobblin.runtime.cli.ConstructorAndPublicMethodsCliObjectFactory;


/**
 * A CLI for the {@link ConfigClient}. Can be used to get resolved configurations for a uri.
 */
@Alias(value = "config", description = "Query the config library")
public class ConfigClientCLI implements CliApplication {

  @Override
  public void run(String[] args) throws Exception {
    CliObjectFactory<Command> factory = new ConstructorAndPublicMethodsCliObjectFactory<>(Command.class);
    Command command = factory.buildObject(args, 1, true, args[0]);

    ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.READ_FRESHEST);

    if (command.resolvedConfig) {
      Config resolvedConfig = configClient.getConfig(command.uri);
      System.out.println(resolvedConfig.root().render(ConfigRenderOptions.defaults()));
    }
  }

  /**
   * The parsed user command.
   */
  public static class Command {

    private final URI uri;
    private boolean resolvedConfig = false;

    @CliObjectSupport(argumentNames = "configUri")
    public Command(String uri) throws URISyntaxException {
      this.uri = new URI(uri);
    }

    public void resolvedConfig() {
      this.resolvedConfig = true;
    }
  }
}
