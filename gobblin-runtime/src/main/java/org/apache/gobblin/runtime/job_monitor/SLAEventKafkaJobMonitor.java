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

package org.apache.gobblin.runtime.job_monitor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.codahale.metrics.Counter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.sla.SlaEventKeys;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecMonitor;
import org.apache.gobblin.runtime.api.JobSpecMonitorFactory;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import lombok.Getter;


/**
 * A {@link KafkaJobMonitor} that parses SLA {@link GobblinTrackingEvent}s and generates {@link JobSpec}s. Used
 * to trigger jobs on data availability.
 */
@Getter
public class SLAEventKafkaJobMonitor extends KafkaAvroJobMonitor<GobblinTrackingEvent> {

  public static final String CONFIG_PREFIX = "gobblin.jobMonitor.slaEvent";

  public static final String DATASET_URN_FILTER_KEY = "filter.urn";
  public static final String EVENT_NAME_FILTER_KEY = "filter.name";

  public static final String TEMPLATE_KEY = "job_template";
  public static final String EXTRACT_KEYS = "extract_keys";
  public static final String BASE_URI_KEY = "baseUri";
  public static final String TOPIC_KEY = "topic";
  public static final String SCHEMA_VERSION_READER_CLASS = "versionReaderClass";

  private static final Config DEFAULTS = ConfigFactory.parseMap(ImmutableMap.of(
      BASE_URI_KEY, SLAEventKafkaJobMonitor.class.getSimpleName(),
      SCHEMA_VERSION_READER_CLASS, NoopSchemaVersionWriter.class.getName()));

  private final Optional<Pattern> urnFilter;
  private final Optional<Pattern> nameFilter;
  private final URI baseURI;
  private final URI template;
  private final Map<String, String> extractKeys;

  private Counter rejectedEvents;

  public static class Factory implements JobSpecMonitorFactory {

    @Override
    public JobSpecMonitor forJobCatalog(GobblinInstanceDriver instanceDriver, MutableJobCatalog jobCatalog)
        throws IOException {
      Config config = instanceDriver.getSysConfig().getConfig().getConfig(CONFIG_PREFIX).withFallback(DEFAULTS);
      return forConfig(config, jobCatalog);
    }

    /**
     * Create a {@link SLAEventKafkaJobMonitor} from an input {@link Config}. Useful for multiple monitors, where
     * the configuration of each monitor is scoped.
     * @param localScopeConfig The sub-{@link Config} for this monitor without any namespacing (e.g. the key for
     *                           topic should simply be "topic").
     * @throws IOException
     */
    public JobSpecMonitor forConfig(Config localScopeConfig, MutableJobCatalog jobCatalog) throws IOException {

      Preconditions.checkArgument(localScopeConfig.hasPath(TEMPLATE_KEY));
      Preconditions.checkArgument(localScopeConfig.hasPath(TOPIC_KEY));

      String topic = localScopeConfig.getString(TOPIC_KEY);

      URI baseUri;
      try {
        baseUri = new URI(localScopeConfig.getString(BASE_URI_KEY));
      } catch (URISyntaxException use) {
        throw new IOException("Invalid base URI " + localScopeConfig.getString(BASE_URI_KEY), use);
      }

      String templateURIString = localScopeConfig.getString(TEMPLATE_KEY);
      URI template;
      try {
        template = new URI(templateURIString);
      } catch (URISyntaxException uri) {
        throw new IOException("Invalid template URI " + templateURIString);
      }

      ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
      if (localScopeConfig.hasPath(EXTRACT_KEYS)) {
        Config extractKeys = localScopeConfig.getConfig(EXTRACT_KEYS);
        for (Map.Entry<String, ConfigValue> entry : extractKeys.entrySet()) {
          Object unwrappedValue = entry.getValue().unwrapped();
          if (unwrappedValue instanceof String) {
            mapBuilder.put(entry.getKey(), (String) unwrappedValue);
          }
        }
      }
      Map<String, String> extractKeys = mapBuilder.build();

      Optional<Pattern> urnFilter = localScopeConfig.hasPath(DATASET_URN_FILTER_KEY)
          ? Optional.of(Pattern.compile(localScopeConfig.getString(DATASET_URN_FILTER_KEY)))
          : Optional.<Pattern>absent();
      Optional<Pattern> nameFilter = localScopeConfig.hasPath(EVENT_NAME_FILTER_KEY)
          ? Optional.of(Pattern.compile(localScopeConfig.getString(EVENT_NAME_FILTER_KEY)))
          : Optional.<Pattern>absent();

      SchemaVersionWriter versionWriter;
      try {
        versionWriter = (SchemaVersionWriter) GobblinConstructorUtils.
            invokeLongestConstructor(Class.forName(localScopeConfig.getString(SCHEMA_VERSION_READER_CLASS)), localScopeConfig);
      } catch (ReflectiveOperationException roe) {
        throw new IllegalArgumentException(roe);
      }

      return new SLAEventKafkaJobMonitor(topic, jobCatalog, baseUri, localScopeConfig, versionWriter,
          urnFilter, nameFilter, template, extractKeys);
    }
  }

  protected SLAEventKafkaJobMonitor(String topic, MutableJobCatalog catalog, URI baseURI, Config limitedScopeConfig,
      SchemaVersionWriter<?> versionWriter, Optional<Pattern> urnFilter, Optional<Pattern> nameFilter, URI template,
      Map<String, String> extractKeys) throws IOException {
    super(topic, catalog, limitedScopeConfig, GobblinTrackingEvent.SCHEMA$, versionWriter);

    this.baseURI = baseURI;
    this.urnFilter = urnFilter;
    this.nameFilter = nameFilter;
    this.template = template;
    this.extractKeys = extractKeys;
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.rejectedEvents = getMetricContext().counter(RuntimeMetrics.GOBBLIN_JOB_MONITOR_SLAEVENT_REJECTEDEVENTS);
  }

  @Override
  public Collection<Either<JobSpec, URI>> parseJobSpec(GobblinTrackingEvent event) {

    if (!acceptEvent(event)) {
      this.rejectedEvents.inc();
      return Lists.newArrayList();
    }

    String datasetURN = event.getMetadata().get(SlaEventKeys.DATASET_URN_KEY);
    URI jobSpecURI = PathUtils.mergePaths(new Path(this.baseURI), new Path(datasetURN)).toUri();

    Map<String, String> jobConfigMap = Maps.newHashMap();
    for (Map.Entry<String, String> entry : this.extractKeys.entrySet()) {
      if (event.getMetadata().containsKey(entry.getKey())) {
        jobConfigMap.put(entry.getValue(), event.getMetadata().get(entry.getKey()));
      }
    }
    Config jobConfig = ConfigFactory.parseMap(jobConfigMap);

    JobSpec jobSpec = JobSpec.builder(jobSpecURI).withTemplate(this.template).withConfig(jobConfig).build();

    return Lists.newArrayList(Either.<JobSpec, URI>left(jobSpec));
  }

  /**
   * Filter for {@link GobblinTrackingEvent}. Used to quickly determine whether an event should be used to produce
   * a {@link JobSpec}.
   */
  protected boolean acceptEvent(GobblinTrackingEvent event) {

    if (!event.getMetadata().containsKey(SlaEventKeys.DATASET_URN_KEY)) {
      return false;
    }

    String datasetURN = event.getMetadata().get(SlaEventKeys.DATASET_URN_KEY);

    if (this.urnFilter.isPresent() && !this.urnFilter.get().matcher(datasetURN).find()) {
      return false;
    }

    if (this.nameFilter.isPresent() && !this.nameFilter.get().matcher(event.getName()).find()) {
      return false;
    }

    return true;
  }
}
