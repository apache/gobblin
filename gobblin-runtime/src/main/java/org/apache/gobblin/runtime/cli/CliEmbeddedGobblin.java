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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;

import com.google.common.base.Strings;

import gobblin.annotation.Alias;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.util.ClassAliasResolver;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link CliApplication} that runs a Gobblin flow using {@link EmbeddedGobblin}.
 *
 * Usage:
 * java -jar gobblin.jar run [app] [OPTIONS]
 *
 * The cli can run arbitrary applications by either setting each configuration manually (using the -setConfiguration
 * option), or with the help of a template (using the -setTemplate option). However, since this requires knowing
 * the exact configurations needed for a job, developers can implement more specific applications with simpler options
 * for the user.
 *
 * {@link CliEmbeddedGobblin} will create an {@link EmbeddedGobblin} using a {@link EmbeddedGobblinCliFactory}
 * found by class name or {@link Alias}. It will
 * parse command line arguments using the options obtained from {@link EmbeddedGobblinCliFactory#getOptions()}, then
 * instantiate {@link EmbeddedGobblin} using {@link EmbeddedGobblinCliFactory#buildObject(CommandLine)}, and
 * finally run the application synchronously.
 */
@Alias(value = "run", description = "Run a Gobblin application.")
@Slf4j
public class CliEmbeddedGobblin implements CliApplication {

  public static final String LIST_QUICK_APPS = "listQuickApps";

  @Override
  public void run(String[] args) throws IOException {

    int startOptions = 1;

    Class<? extends EmbeddedGobblinCliFactory> factoryClass;
    String alias = "";
    if (args.length >= 2 && !args[1].startsWith("-")) {
      alias = args[1];
      startOptions = 2;
    }

    if (alias.equals(LIST_QUICK_APPS)) {
      listQuickApps();
      return;
    }

    EmbeddedGobblinCliFactory factory;
    if (!Strings.isNullOrEmpty(alias)) {
      try {
        ClassAliasResolver<EmbeddedGobblinCliFactory> resolver = new ClassAliasResolver<>(EmbeddedGobblinCliFactory.class);
        factoryClass = resolver.resolveClass(alias);
        factory = factoryClass.newInstance();
      } catch (ReflectiveOperationException roe) {
        System.out.println("Error: Could not find job with alias " + alias);
        System.out.println("For a list of jobs available: \"gobblin run " + LIST_QUICK_APPS + "\"");
        return;
      }
    } else {
      factory = new EmbeddedGobblin.CliFactory();
    }

    String usage;
    if (Strings.isNullOrEmpty(alias)) {
      usage = "gobblin run [listQuickApps] [<quick-app>] " + factory.getUsageString();
    } else {
      usage = "gobblin run " + alias + " " + factory.getUsageString();
    }

    EmbeddedGobblin embeddedGobblin = factory.buildObject(args, startOptions, true, usage);

    try {
      embeddedGobblin.run();
    } catch (InterruptedException | TimeoutException | ExecutionException exc) {
      throw new RuntimeException("Failed to run Gobblin job.", exc);
    }
  }

  private List<Alias> getAllAliases() {
    ClassAliasResolver<EmbeddedGobblinCliFactory> resolver = new ClassAliasResolver<>(EmbeddedGobblinCliFactory.class);
    return resolver.getAliasObjects();
  }

  private void listQuickApps() {
    List<Alias> aliases = getAllAliases();
    System.out.println("Usage: gobblin run <quick-app-name> [OPTIONS]");
    System.out.println("Available quick apps:");
    for (Alias thisAlias : aliases) {
      System.out.println(String.format("\t%s\t-\t%s", thisAlias.value(), thisAlias.description()));
    }
  }

}
