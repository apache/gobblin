package org.apache.gobblin.test.config;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reflections.Reflections;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A factory that uses annotations to auto-register classes and returns constructed instances
 * @param <T> : The type of objects that need to be retrieved from the Factory
 * @param <A> : The annotation applied to the classes that allows them to be discovered and registered
 */
@Slf4j
public class AnnotationRegisteredFactory<T, A extends Annotation> {

  private final String factoryName;
  // The alias -> class registry
  private final Map<String, Class<? extends T>> registry = new HashMap<>();
  /**
   *
   * @param factoryName: The name of the factory (useful for logging)
   * @param namespace: The namespace of the classpath hierarchy that will be inspected for registrations
   * @param instanceClass: The class that all instances derive from
   * @param annotationClass: The annotation class that contains the descriptive registration info
   * @param uuidGenerator: A function that given a provided annotation can return a stream of String uuids that the class should register as serving
   */
  public AnnotationRegisteredFactory(
      String factoryName,
      String namespace,
      Class<T> instanceClass,
      Class<A> annotationClass,
      Function<A, Stream<String>> uuidGenerator)
  {
    this.factoryName = factoryName;
    Reflections reflections = new Reflections(namespace);
    final Set<Class<? extends T>> subTypesOf = reflections.getSubTypesOf(instanceClass);
    for (Class<? extends T> subType: subTypesOf) {
      A annotation = subType.getAnnotation(annotationClass);
      if (annotation !=null) {
        uuidGenerator.apply(annotation)
            .filter(Objects::nonNull) // filter out uuids that are null
            .forEach(uuid -> registerInstance(uuid, subType));
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

  public boolean canProduce(String uuid) {
    return this.registry.containsKey(uuid);
  }

  public T getThing(String uuid, Object... args)
  {
      Class<? extends T> instanceClass = getInstanceClass(uuid);
      T instance;
      // Constructing with args passed in
      instance = GobblinConstructorUtils.invokeConstructor(instanceClass, instanceClass.getName(), args);
      return instance;
  }
}
