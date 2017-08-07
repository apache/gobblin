package gobblin.runtime.spec_executorInstance;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecProducer;
import gobblin.util.CompletedFuture;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemorySpecProducer implements SpecProducer<Spec>, Serializable {
  private final Map<URI, Spec> provisionedSpecs;
  private Config config;

  private static final long serialVersionUID = 6106269076155338045L;

  public InMemorySpecProducer(Config config) {
    this.config = config;
    this.provisionedSpecs = Maps.newHashMap();
  }

  @Override
  public Future<?> addSpec(Spec addedSpec) {
    provisionedSpecs.put(addedSpec.getUri(), addedSpec);
    log.info(String.format("Added Spec: %s with Uri: %s for execution on this executor.", addedSpec, addedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> updateSpec(Spec updatedSpec) {
    if (!provisionedSpecs.containsKey(updatedSpec.getUri())) {
      throw new RuntimeException("Spec not found: " + updatedSpec.getUri());
    }
    provisionedSpecs.put(updatedSpec.getUri(), updatedSpec);
    log.info(String.format("Updated Spec: %s with Uri: %s for execution on this executor.", updatedSpec, updatedSpec.getUri()));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<?> deleteSpec(URI deletedSpecURI) {
    if (!provisionedSpecs.containsKey(deletedSpecURI)) {
      throw new RuntimeException("Spec not found: " + deletedSpecURI);
    }
    provisionedSpecs.remove(deletedSpecURI);
    log.info(String.format("Deleted Spec with Uri: %s from this executor.", deletedSpecURI));

    return new CompletedFuture(Boolean.TRUE, null);
  }

  @Override
  public Future<? extends List<Spec>> listSpecs() {
    return new CompletedFuture<>(Lists.newArrayList(provisionedSpecs.values()), null);
  }

}
