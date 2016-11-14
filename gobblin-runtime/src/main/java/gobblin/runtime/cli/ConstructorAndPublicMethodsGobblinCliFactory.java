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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.google.common.collect.Maps;

import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.embedded.EmbeddedGobblin;


/**
 * A helper class for automatically inferring {@link Option}s from the constructor and public methods in a class.
 *
 * For method inference, see {@link PublicMethodsGobblinCliFactory}.
 *
 * {@link Option}s are inferred from the constructor as follows:
 * 1. The helper will search for exactly one constructor with only String arguments and which is annotated with
 *    {@link EmbeddedGobblinCliSupport}.
 * 2. For each parameter of the constructor, the helper will create a required {@link Option}.
 *
 * For an example usage see {@link EmbeddedGobblin.CliFactory}.
 */
public abstract class ConstructorAndPublicMethodsGobblinCliFactory extends PublicMethodsGobblinCliFactory {

  private final Constructor<? extends EmbeddedGobblin> constructor;
  private final Map<String, Integer> constructoArgumentsMap;
  private final Options options;

  public ConstructorAndPublicMethodsGobblinCliFactory(Class<? extends EmbeddedGobblin> klazz) {
    super(klazz);
    this.constructoArgumentsMap = Maps.newHashMap();
    this.options = super.getOptions();
    this.constructor = inferConstructorOptions(this.options);
  }

  @Override
  public EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli)
      throws JobTemplate.TemplateException, IOException {
    return buildInstance(cli);
  }

  @Override
  public Options getOptions() {
    return this.options;
  }

  /**
   * Builds an instance of {@link EmbeddedGobblin} using the selected constructor getting the constructor
   * parameters from the {@link CommandLine}.
   *
   * Note: this method will also automatically call {@link #applyCommandLineOptions(CommandLine, EmbeddedGobblin)} on
   * the constructed {@link EmbeddedGobblin}.
   */
  public EmbeddedGobblin buildInstance(CommandLine cli) {
    String[] constructorArgs = new String[this.constructor.getParameterTypes().length];
    for (Option option : cli.getOptions()) {
      if (this.constructoArgumentsMap.containsKey(option.getOpt())) {
        int idx = this.constructoArgumentsMap.get(option.getOpt());
        constructorArgs[idx] = option.getValue();
      }
    }

    EmbeddedGobblin embeddedGobblin;
    try {
      embeddedGobblin = this.constructor.newInstance((Object[]) constructorArgs);
      return embeddedGobblin;
    } catch (IllegalAccessException | InvocationTargetException | InstantiationException exc) {
      throw new RuntimeException("Could not instantiate " + this.klazz.getName(), exc);
    }
  }

  private Constructor<? extends EmbeddedGobblin> inferConstructorOptions(Options otherOptions) {
    Constructor<? extends EmbeddedGobblin> selectedConstructor = null;
    for (Constructor<?> constructor : this.klazz.getConstructors()) {
      if (canUseConstructor(constructor)) {
        if (selectedConstructor == null) {
          selectedConstructor = (Constructor<? extends EmbeddedGobblin>) constructor;
        } else {
          throw new RuntimeException("Multiple usable constructors for " + this.klazz.getName());
        }
      }
    }
    if (selectedConstructor == null) {
      throw new RuntimeException("There is no usable constructor for " + this.klazz.getName());
    }

    int constructorIdx = 0;
    for (String argument : selectedConstructor.getAnnotation(EmbeddedGobblinCliSupport.class).argumentNames()) {
      Option option = Option.builder(argument).required().hasArg().build();
      otherOptions.addOption(option);
      constructoArgumentsMap.put(option.getOpt(), constructorIdx);
      constructorIdx++;
    }

    return selectedConstructor;
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
}
