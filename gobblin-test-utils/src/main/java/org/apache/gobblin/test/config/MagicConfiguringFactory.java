package org.apache.gobblin.test.config;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reflections.Reflections;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


public class MagicConfiguringFactory<T, A extends Annotation> extends AnnotationRegisteredFactory<T, A> {
  // The class -> configClass class registry
  private final Map<Class<? extends T>, Class> configRegistry = new HashMap<>();

  public MagicConfiguringFactory(
      String factoryName,
      String namespace,
      Class<T> instanceClass,
      Class<A> annotationClass,
      Function<A, Stream<String>> uuidGenerator,
      Function<A, Class> configurationClassGenerator)
  {
    super(factoryName, namespace, instanceClass, annotationClass, uuidGenerator);
    Reflections reflections = new Reflections(namespace);
    final Set<Class<? extends T>> subTypesOf = reflections.getSubTypesOf(instanceClass);
    Preconditions.checkArgument(configurationClassGenerator != null,
        "Configuration Class Generator should not be null. Use "
            + AnnotationRegisteredFactory.class.getCanonicalName() +  " instead.");
    for (Class<? extends T> subType: subTypesOf) {
      A annotation = subType.getAnnotation(annotationClass);
      if (annotation !=null) {
          this.configRegistry.put(subType, configurationClassGenerator.apply(annotation));
      }
    }
  }

  protected Class getConfigClass(Class instanceClass) {
    return this.configRegistry.get(instanceClass);
  }

  public T getConfiguredThing(String uuid, Config config)
      throws ClassNotFoundException {
    Class<? extends T> instanceClass = getInstanceClass(uuid);
    T instance;
      Class configClass = getConfigClass(instanceClass);
      if (configClass.equals(Config.class)) {
        instance = GobblinConstructorUtils.invokeConstructor(instanceClass, instanceClass.getName(), config);
      } else {
        Object classConfig = ConfigBeanFactory.create(config, configClass);
        instance = GobblinConstructorUtils.invokeConstructor(instanceClass, instanceClass.getName(), classConfig);
      }
    return instance;
  }


}
