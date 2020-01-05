package org.apache.gobblin.test.config;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reflections.Reflections;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 *
 * @param <T> : The type of objects that need to be retrieved from the Factory
 * @param <A> : The annotation applied to the classes that allows them to be discovered and registered
 */
@Slf4j
public class MagicFactory<T, A extends Annotation> {

  private final String factoryName;
  private final String namespace;
  private final Class instanceClass;
  private final Map<String, Class<? extends T>> registry = new HashMap<>();
  private final Map<Class<? extends T>, Class> configRegistry = new HashMap<>();

  /**
   *
   * @param factoryName: The name of the factory (useful for logging)
   * @param namespace: The namespace of the classpath hierarchy that will be inspected for registrations
   * @param instanceClass: The class that all instances derive from
   * @param annotationClass: The annotation class that contains the descriptive registration info
   * @param uuidGenerator: A function that given a provided annotation can return a stream of String uuids that a class should register as
   */
  protected MagicFactory(
      String factoryName,
      String namespace,
      Class<T> instanceClass,
      Class<A> annotationClass,
      Function<A, Stream<String>> uuidGenerator,
      Function<A, Class> configurationClassGenerator)
  {
    this.factoryName = factoryName;
    this.namespace = namespace;
    this.instanceClass = instanceClass;
    Reflections reflections = new Reflections(namespace);
    final Set<Class<? extends T>> subTypesOf = reflections.getSubTypesOf(instanceClass);
    for (Class<? extends T> subType: subTypesOf) {
      A annotation = subType.getAnnotation(annotationClass);
      if (annotation !=null) {
        uuidGenerator.apply(annotation).forEach(uuid -> registerInstance(uuid, subType));
        this.configRegistry.put(subType, configurationClassGenerator.apply(annotation));
      }
    }
  }



  private void registerInstance(String name, @NonNull Class<? extends T> instanceClass) {
    if (this.registry.containsKey(name)) {
      throw new RuntimeException("Factory already has a thing registered under name " + name);
    }
    this.registry.put(name, instanceClass);
  }

  protected Class getInstanceClass(String uuid) {
    Class<? extends T> instanceClass = this.registry.get(uuid);
    if (instanceClass == null) {
      log.error("Could not find an entry for {} in the {} registry", uuid, this.factoryName);
    }
    return instanceClass;
  }

  protected Class getConfigClass(Class instanceClass) {
    return this.configRegistry.get(instanceClass);
  }

  protected T getThing(String uuid, Config config)
      throws ClassNotFoundException {
    Class<? extends T> instanceClass = getInstanceClass(uuid);
    Class configClass = getConfigClass(instanceClass);

    T instance;
    if (configClass.equals(Config.class)) {
      instance = GobblinConstructorUtils.invokeConstructor(instanceClass, instanceClass.getName(), config);
    } else {
      Object classConfig = ConfigBeanFactory.create(config, configClass);
      instance = GobblinConstructorUtils.invokeConstructor(instanceClass, instanceClass.getName(), classConfig);
    }
    return instance;
  }

}
