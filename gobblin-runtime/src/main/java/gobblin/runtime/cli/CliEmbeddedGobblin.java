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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.annotation.Alias;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.util.ClassAliasResolver;

import lombok.Data;


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
 * {@link CliEmbeddedGobblin} will load any extension of {@link EmbeddedGobblin} using an alias or a class name. It will
 * automatically parse a set of options from the constructor and public methods of that class, and the parse the cli
 * arguments using these options:
 * * {@link CliEmbeddedGobblin} will look for a constructor of the class annotated with {@link EmbeddedGobblinCliSupport}
 *   and with only {@link String} parameters. For each parameter, it will create a required option.
 * * {@link CliEmbeddedGobblin} will look for all public methods with none or exactly one {@link String} parameter
 *   that is not annotated with {@link NotOnCli}. For each such method, it will create an optional option with name
 *   equal to the method name (unless a different name is specified using {@link EmbeddedGobblinCliOption}).
 *
 * See {@link gobblin.runtime.embedded.EmbeddedGobblinDistcp} for an example.
 */
@Alias(value = "run", description = "Run a Gobblin application.")
public class CliEmbeddedGobblin implements CliApplication {

  private static final Option HELP = Option.builder("help").build();

  private static final List<String> BLACKLISTED_FROM_CLI = ImmutableList.of(
      "getClass", "hashCode", "notify", "notifyAll", "toString", "wait"
  );

  @Override
  public void run(String[] args) {

    int startOptions = 1;

    Class<? extends EmbeddedGobblin> klazz;
    String alias = "";
    if (args.length >= 2 && !args[1].startsWith("-")) {
      alias = args[1];
      startOptions = 2;
    }

    if (alias.equals("listQuickApps")) {
      listQuickApps();
      return;
    }

    if (!Strings.isNullOrEmpty(alias)) {
      try {
        ClassAliasResolver<EmbeddedGobblin> resolver = new ClassAliasResolver<>(EmbeddedGobblin.class);
        klazz = resolver.resolveClass(alias);
      } catch (ReflectiveOperationException roe) {
        throw new RuntimeException("Could not find job with alias " + alias);
      }
    } else {
      klazz = EmbeddedGobblin.class;
    }

    Constructor<? extends EmbeddedGobblin> selectedConstructor = null;
    for (Constructor<?> constructor : klazz.getConstructors()) {
      if (canUseConstructor(constructor)) {
        if (selectedConstructor == null) {
          selectedConstructor = (Constructor<? extends EmbeddedGobblin>) constructor;
        } else {
          throw new RuntimeException("Multiple usable constructors for " + klazz.getName());
        }
      }
    }
    if (selectedConstructor == null) {
      throw new RuntimeException("There is no usable constructor for " + klazz.getName());
    }

    Options options = new Options();
    options.addOption(HELP);
    final Map<String, OptionDescriptor> descriptorMap = Maps.newHashMap();
    List<Option> requiredOptions = Lists.newArrayList();

    int constructorIdx = 0;
    for (String argument : selectedConstructor.getAnnotation(EmbeddedGobblinCliSupport.class).argumentNames()) {
      Option option = Option.builder(argument).required().hasArg().build();
      options.addOption(option);
      requiredOptions.add(option);
      descriptorMap.put(argument, new OptionDescriptor(OptionType.CONSTRUCTOR, constructorIdx, null));
      constructorIdx++;
    }

    for (Method method : klazz.getMethods()) {
      if (canUseMethod(method)) {
        EmbeddedGobblinCliOption annotation = method.isAnnotationPresent(EmbeddedGobblinCliOption.class) ?
            method.getAnnotation(EmbeddedGobblinCliOption.class) : null;
        String optionName = annotation == null || Strings.isNullOrEmpty(annotation.name())
            ? method.getName() : annotation.name();
        String description = annotation == null ? "" : annotation.description();
        Option.Builder builder = Option.builder(optionName).desc(description);
        boolean hasArg = method.getParameterTypes().length > 0;
        if (hasArg) {
          builder.hasArg();
        }
        options.addOption(builder.build());
        descriptorMap.put(optionName, new OptionDescriptor(hasArg ? OptionType.ARG : OptionType.NO_ARG, 0, method));
      }
    }

    CommandLine cli;
    try {
      CommandLineParser parser = new DefaultParser();
      cli = parser.parse(options, Arrays.copyOfRange(args, startOptions, args.length));
    } catch (ParseException pe) {
      System.out.println( "Command line parse exception: " + pe.getMessage() );
      printUsage(alias, requiredOptions, options, descriptorMap);
      return;
    }

    if (cli.hasOption(HELP.getOpt())) {
      printUsage(alias, requiredOptions, options, descriptorMap);
      return;
    }

    String[] constructorArgs = new String[selectedConstructor.getParameterTypes().length];
    for (Option option : cli.getOptions()) {
      OptionDescriptor descriptor = descriptorMap.get(option.getOpt());
      if (descriptor.getTpe() == OptionType.CONSTRUCTOR) {
        constructorArgs[descriptor.getIdx()] = option.getValue();
      }
    }

    EmbeddedGobblin embeddedGobblin;
    try {
      embeddedGobblin = selectedConstructor.newInstance((Object[]) constructorArgs);
      for (Option option : cli.getOptions()) {
        OptionDescriptor descriptor = descriptorMap.get(option.getOpt());
        if (descriptor.getTpe() == OptionType.ARG) {
          descriptor.getMethod().invoke(embeddedGobblin, option.getValue());
        } else if (descriptor.getTpe() == OptionType.NO_ARG) {
          descriptor.getMethod().invoke(embeddedGobblin);
        }
      }
    } catch (IllegalAccessException | InvocationTargetException | InstantiationException exc) {
      throw new RuntimeException("Could not instantiate " + klazz.getName(), exc);
    }

    try {
      embeddedGobblin.run();
    } catch (InterruptedException | TimeoutException | ExecutionException exc) {
      throw new RuntimeException("Failed to run Gobblin job.", exc);
    }
  }

  private List<Alias> getAllAliases() {
    ClassAliasResolver<EmbeddedGobblin> resolver = new ClassAliasResolver<>(EmbeddedGobblin.class);
    return resolver.getAliasObjects();
  }

  private void listQuickApps() {
    List<Alias> aliases = getAllAliases();
    System.out.println("Usage: gobblin run <quick-app-name> [OPTIONS]");
    System.out.println("Available quick apps:");
    for (Alias thisAlias : aliases) {
      System.out.println(String.format("\t%s\t%s", thisAlias.value(), thisAlias.description()));
    }
  }

  private void printUsage(String alias, List<Option> requiredOptions, Options options,
      final Map<String, OptionDescriptor> descriptorMap) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new Comparator<Option>() {
      @Override
      public int compare(Option o1, Option o2) {
        OptionDescriptor o1desc = descriptorMap.get(o1.getOpt());
        OptionDescriptor o2desc = descriptorMap.get(o2.getOpt());

        OptionType tpe1 = o1desc == null ? OptionType.NO_ARG : o1desc.getTpe();
        OptionType tpe2 = o2desc == null ? OptionType.NO_ARG : o2desc.getTpe();

        if (tpe1 == OptionType.CONSTRUCTOR && tpe2 != OptionType.CONSTRUCTOR) {
          return -1;
        }
        if (tpe1 != OptionType.CONSTRUCTOR && tpe2 == OptionType.CONSTRUCTOR) {
          return 1;
        }
        return o1.getOpt().compareTo(o2.getOpt());
      }
    });

    StringBuilder usage = new StringBuilder("gobblin run [listQuickApps] [<quick-app>] ");
    if (!Strings.isNullOrEmpty(alias)) {
      usage.append(alias).append(" ");
    }
    for (Option option : requiredOptions) {
      usage.append("-").append(option.getOpt()).append(" <").append(option.getOpt()).append("> ");
    }
    usage.append("[OPTIONS]");

    formatter.printHelp(usage.toString(), options);
  }

  private enum OptionType {
    CONSTRUCTOR, ARG, NO_ARG
  }

  @Data
  private static class OptionDescriptor {
    private final OptionType tpe;
    private final int idx;
    private final Method method;
  }

  private boolean canUseConstructor(Constructor<?> constructor) {
    if (!Modifier.isPublic(constructor.getModifiers())) {
      return false;
    }
    if (!constructor.isAnnotationPresent(EmbeddedGobblinCliSupport.class)) {
      return false;
    }
    for (Class<?> param : constructor.getParameterTypes()) {
      if (param != String.class) {
        return false;
      }
    }
    return constructor.getParameterTypes().length ==
        constructor.getAnnotation(EmbeddedGobblinCliSupport.class).argumentNames().length;
  }

  private boolean canUseMethod(Method method) {
    if (!Modifier.isPublic(method.getModifiers())) {
      return false;
    }
    if (BLACKLISTED_FROM_CLI.contains(method.getName())) {
      return false;
    }
    if (method.isAnnotationPresent(NotOnCli.class)) {
      return false;
    }
    Class<?>[] parameters = method.getParameterTypes();
    if (parameters.length > 2) {
      return false;
    }
    if (parameters.length == 1 && parameters[0] != String.class) {
      return false;
    }
    return true;
  }

}
