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
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.embedded.EmbeddedGobblin;

import lombok.Getter;



/**
 * A helper class for automatically inferring {@link Option}s from the public methods in a class.
 *
 * For each public method in the class to infer with exactly zero or one String parameter, the helper will create
 * an optional {@link Option}. Using the annotation {@link EmbeddedGobblinCliOption} the helper can automatically
 * add a description to the {@link Option}. Annotating a method with {@link NotOnCli} will prevent the helper from
 * creating an {@link Option} from it.
 *
 * For an example usage see {@link EmbeddedGobblin.CliFactory}
 */
public abstract class PublicMethodsGobblinCliFactory implements EmbeddedGobblinCliFactory {

  private static final List<String> BLACKLISTED_FROM_CLI = ImmutableList.of(
      "getClass", "hashCode", "notify", "notifyAll", "toString", "wait"
  );

  protected final Class<? extends EmbeddedGobblin> klazz;
  @Getter
  private final Options options;
  private final Map<String, Method> methodsMap;

  public PublicMethodsGobblinCliFactory(Class<? extends EmbeddedGobblin> klazz) {
    this.klazz = klazz;
    this.methodsMap = Maps.newHashMap();
    this.options = inferOptionsFromMethods();
  }

  @Override
  public EmbeddedGobblin buildEmbeddedGobblin(CommandLine cli) {
    try {
      EmbeddedGobblin embeddedGobblin = constructEmbeddedGobblin(cli);
      applyCommandLineOptions(cli, embeddedGobblin);
      return embeddedGobblin;
    } catch (IOException | JobTemplate.TemplateException exc) {
      throw new RuntimeException("Could not instantiate " + this.klazz.getSimpleName(), exc);
    }
  }

  public abstract EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli) throws JobTemplate.TemplateException, IOException;

  @Override
  public String getUsageString() {
    return "[OPTIONS]";
  }

  /**
   * For each method for which the helper created an {@link Option} and for which the input {@link CommandLine} contains
   * that option, this method will automatically call the method on the input {@link EmbeddedGobblin} with the correct
   * arguments.
   */
  public void applyCommandLineOptions(CommandLine cli, EmbeddedGobblin embeddedGobblin) {
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
    if (parameters.length > 2) {
      return false;
    }
    if (parameters.length == 1 && parameters[0] != String.class) {
      return false;
    }
    return true;
  }

}
