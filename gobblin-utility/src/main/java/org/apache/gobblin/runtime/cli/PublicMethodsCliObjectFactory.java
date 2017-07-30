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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A helper class for automatically inferring {@link Option}s from the public methods in a class.
 *
 * For each public method in the class to infer with exactly zero or one String parameter, the helper will create
 * an optional {@link Option}. Using the annotation {@link CliObjectOption} the helper can automatically
 * add a description to the {@link Option}. Annotating a method with {@link NotOnCli} will prevent the helper from
 * creating an {@link Option} from it.
 */
@Slf4j
public abstract class PublicMethodsCliObjectFactory<T> implements CliObjectFactory<T> {

  private static final Option HELP = Option.builder("h").longOpt("help").build();
  private static final Option USE_LOG = Option.builder("l").desc("Uses log to print out erros in the base CLI code.").build();

  private static final List<String> BLACKLISTED_FROM_CLI = ImmutableList.of(
      "getClass", "hashCode", "notify", "notifyAll", "toString", "wait"
  );

  protected final Class<? extends T> klazz;
  @Getter
  private final Options options;
  private final Map<String, Method> methodsMap;

  public PublicMethodsCliObjectFactory(Class<? extends T> klazz) {
    this.klazz = klazz;
    this.methodsMap = Maps.newHashMap();
    this.options = inferOptionsFromMethods();
  }

  @Override
  public T buildObject(CommandLine cli) {
    try {
      T obj = constructObject(cli);
      applyCommandLineOptions(cli, obj);
      return obj;
    } catch (IOException exc) {
      throw new RuntimeException("Could not instantiate " + this.klazz.getSimpleName(), exc);
    }
  }

  @Override
  public T buildObject(String[] args, int offset, boolean printUsage, String usage) throws IOException {
    Options options = new Options();
    options.addOption(HELP);
    options.addOption(USE_LOG);
    for (Option opt : getOptions().getOptions()) {
      options.addOption(opt);
    }

    CommandLine cli;
    try {
      CommandLineParser parser = new DefaultParser();
      cli = parser.parse(options, Arrays.copyOfRange(args, offset, args.length));
    } catch (ParseException pe) {
      if (printUsage) {
        System.out.println("Command line parse exception: " + pe.getMessage());
        printUsage(usage, options);
      }
      throw new IOException(pe);
    }

    if (cli.hasOption(HELP.getOpt())) {
      if (printUsage) {
        printUsage(usage, options);
      }
      throw new HelpArgumentFound();
    }

    try {
      return buildObject(cli);
    } catch (Throwable t) {
      if (cli.hasOption(USE_LOG.getOpt())) {
        log.error("Failed to instantiate " + this.klazz.getName(), t);
      } else {
        System.out.println("Error: " + t.getMessage());
      }
      if (printUsage) {
        printUsage(usage, options);
      }
      throw new IOException(t);
    }
  }

  protected abstract T constructObject(CommandLine cli) throws IOException;

  @Override
  public String getUsageString() {
    return "[OPTIONS]";
  }

  /**
   * For each method for which the helper created an {@link Option} and for which the input {@link CommandLine} contains
   * that option, this method will automatically call the method on the input object with the correct
   * arguments.
   */
  public void applyCommandLineOptions(CommandLine cli, T embeddedGobblin) {
    try {
      for (Option option : cli.getOptions()) {
        if (!this.methodsMap.containsKey(option.getOpt())) {
          // Option added by cli driver itself.
          continue;
        }
        if (option.hasArg()) {
          this.methodsMap.get(option.getOpt()).invoke(embeddedGobblin, option.getValue());
        } else {
          this.methodsMap.get(option.getOpt()).invoke(embeddedGobblin);
        }
      }
    } catch (IllegalAccessException | InvocationTargetException exc) {
      throw new RuntimeException("Could not apply options to " + embeddedGobblin.getClass().getName(), exc);
    }
  }

  private Options inferOptionsFromMethods() {
    Options options = new Options();

    for (Method method : klazz.getMethods()) {
      if (canUseMethod(method)) {
        CliObjectOption annotation = method.isAnnotationPresent(CliObjectOption.class) ?
            method.getAnnotation(CliObjectOption.class) : null;
        String optionName = annotation == null || Strings.isNullOrEmpty(annotation.name())
            ? method.getName() : annotation.name();
        String description = annotation == null ? "" : annotation.description();
        Option.Builder builder = Option.builder(optionName).desc(description);
        boolean hasArg = method.getParameterTypes().length > 0;
        if (hasArg) {
          builder.hasArg();
        }
        Option option = builder.build();
        options.addOption(option);
        this.methodsMap.put(option.getOpt(), method);
      }
    }

    return options;
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
    if (parameters.length >= 2) {
      return false;
    }
    if (parameters.length == 1 && parameters[0] != String.class) {
      return false;
    }
    return true;
  }

  private void printUsage(String usage, Options options) {

    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new Comparator<Option>() {
      @Override
      public int compare(Option o1, Option o2) {
        if (o1.isRequired() && !o2.isRequired()) {
          return -1;
        }
        if (!o1.isRequired() && o2.isRequired()) {
          return 1;
        }
        return o1.getOpt().compareTo(o2.getOpt());
      }
    });

    formatter.printHelp(usage, options);
  }

}
