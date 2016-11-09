/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.cli;

import com.google.common.collect.Sets;

import gobblin.annotation.Alias;
import gobblin.util.ClassAliasResolver;


/**
 * Instantiates a {@link CliApplication} and runs it.
 */
public class GobblinCli {

  public static void main(String[] args) {
    ClassAliasResolver<CliApplication> resolver = new ClassAliasResolver<>(CliApplication.class);

    if (args.length < 1 || Sets.newHashSet("-h", "--help").contains(args[0])) {
      printUsage(resolver);
      return;
    }

    String alias = args[0];

    try {
      CliApplication application = resolver.resolveClass(alias).newInstance();
      application.run(args);
    } catch (ReflectiveOperationException roe) {
      System.err.println("Could not find an application with alias " + alias);
      printUsage(resolver);
    }
  }

  private static void printUsage(ClassAliasResolver<CliApplication> resolver) {
    System.out.println("Usage: gobblin <command>");
    System.out.println("Available commands:");
    for (Alias alias : resolver.getAliasObjects()) {
      System.out.println("\t" + alias.value() + "\t" + alias.description());
    }
  }

}
