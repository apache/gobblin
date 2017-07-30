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
package gobblin.util.reflection;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.reflect.ConstructorUtils;


/**
 * Helper methods to instantiate classes
 */
@Slf4j
public class GobblinConstructorUtils {

  /**
   * Convenience method on top of {@link ConstructorUtils#invokeConstructor(Class, Object[])} that returns a new
   * instance of the <code>cls</code> based on a constructor priority order. Each {@link List} in the
   * <code>constructorArgs</code> array contains the arguments for a constructor of <code>cls</code>. The first
   * constructor whose signature matches the argument types will be invoked.
   *
   * @param cls the class to be instantiated
   * @param constructorArgs An array of constructor argument list. Order defines the priority of a constructor.
   * @return
   *
   * @throws NoSuchMethodException if no constructor matched was found
   */
  @SafeVarargs
  public static <T> T invokeFirstConstructor(Class<T> cls, List<Object>... constructorArgs)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    for (List<Object> args : constructorArgs) {

      Class<?>[] parameterTypes = new Class[args.size()];
      for (int i = 0; i < args.size(); i++) {
        parameterTypes[i] = args.get(i).getClass();
      }

      if (ConstructorUtils.getMatchingAccessibleConstructor(cls, parameterTypes) != null) {
        return ConstructorUtils.invokeConstructor(cls, args.toArray(new Object[args.size()]));
      }
    }
    throw new NoSuchMethodException("No accessible constructor found");
  }

  /**
   * Returns a new instance of the <code>cls</code> based on a set of arguments. The method will search for a
   * constructor accepting the first k arguments in <code>args</code> for every k from args.length to 0, and will
   * invoke the first constructor found.
   *
   * For example, {@link #invokeLongestConstructor}(cls, myString, myInt) will first attempt to create an object with
   * of class <code>cls</code> with constructor <init>(String, int), if it fails it will attempt <init>(String), and
   * finally <init>().
   *
   * @param cls the class to instantiate.
   * @param args the arguments to use for instantiation.
   * @throws ReflectiveOperationException
   */
  public static <T> T invokeLongestConstructor(Class<T> cls, Object... args) throws ReflectiveOperationException {

    Class<?>[] parameterTypes = new Class[args.length];
    for (int i = 0; i < args.length; i++) {
      parameterTypes[i] = args[i].getClass();
    }

    for (int i = args.length; i >= 0; i--) {
      if (ConstructorUtils.getMatchingAccessibleConstructor(cls, Arrays.copyOfRange(parameterTypes, 0, i)) != null) {
        log.info(
            String.format("Found accessible constructor for class %s with parameter types %s.", cls,
                Arrays.toString(Arrays.copyOfRange(parameterTypes, 0, i))));
        return ConstructorUtils.invokeConstructor(cls, Arrays.copyOfRange(args, 0, i));
      }
    }
    throw new NoSuchMethodException(String.format("No accessible constructor for class %s with parameters a subset of %s.",
        cls, Arrays.toString(parameterTypes)));
  }

  /**
   * Utility method to create an instance of <code>clsName</code> using the constructor matching the arguments, <code>args</code>
   *
   * @param superType of <code>clsName</code>. The new instance is cast to superType
   * @param clsName complete cannonical name of the class to be instantiated
   * @param args constructor args to be used
   *
   * @throws IllegalArgumentException if there was an issue creating the instance due to
   * {@link NoSuchMethodException}, {@link InvocationTargetException},{@link InstantiationException},
   *  {@link ClassNotFoundException}
   *
   * @return A new instance of <code>clsName</code>
   */
  @SuppressWarnings("unchecked")
  public static <T> T invokeConstructor(final Class<T> superType, final String clsName, Object... args) {

    try {
      return (T) ConstructorUtils.invokeConstructor(Class.forName(clsName), args);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
