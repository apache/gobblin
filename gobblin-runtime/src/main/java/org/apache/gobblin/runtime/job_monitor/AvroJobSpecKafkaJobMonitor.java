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
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecMonitor;
import org.apache.gobblin.runtime.api.JobSpecMonitorFactory;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.api.SpecExecutor.Verb;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link KafkaJobMonitor} that parses {@link AvroJobSpec}s and generates {@link JobSpec}s.
 */
@Getter
@Slf4j
public class AvroJobSpecKafkaJobMonitor extends KafkaAvroJobMonitor<AvroJobSpec> {

  public static final String CONFIG_PREFIX = "gobblin.jobMonitor.avroJobSpec";
  public static final String TOPIC_KEY = "topic";
  public static final String SCHEMA_VERSION_READER_CLASS = "versionReaderClass";
  protected static final String VERB_KEY = "Verb";

  private static final Config DEFAULTS = ConfigFactory.parseMap(ImmutableMap.of(
      SCHEMA_VERSION_READER_CLASS, FixedSchemaVersionWriter.class.getName()));

  public static class Factory implements JobSpecMonitorFactory {
    @Override
    public JobSpecMonitor forJobCatalog(GobblinInstanceDriver instanceDriver, MutableJobCatalog jobCatalog)
        throws IOException {
      Config config = instanceDriver.getSysConfig().getConfig().getConfig(CONFIG_PREFIX).withFallback(DEFAULTS);
      return forConfig(config, jobCatalog);
    }

    /**
     * Create a {@link AvroJobSpecKafkaJobMonitor} from an input {@link Config}. Useful for multiple monitors, where
     * the configuration of each monitor is scoped.
     * @param localScopeConfig The sub-{@link Config} for this monitor without any namespacing (e.g. the key for
     *                           topic should simply be "topic").
     * @throws IOException
     */
    public JobSpecMonitor forConfig(Config localScopeConfig, MutableJobCatalog jobCatalog) throws IOException {
      Preconditions.checkArgument(localScopeConfig.hasPath(TOPIC_KEY));
      Config config = localScopeConfig.withFallback(DEFAULTS);

      String topic = config.getString(TOPIC_KEY);

      SchemaVersionWriter versionWriter;
      try {
        versionWriter = (SchemaVersionWriter) GobblinConstructorUtils.
            invokeLongestConstructor(Class.forName(config.getString(SCHEMA_VERSION_READER_CLASS)), config);
      } catch (ReflectiveOperationException roe) {
        throw new IllegalArgumentException(roe);
      }

      return new AvroJobSpecKafkaJobMonitor(topic, jobCatalog, config, versionWriter);
    }
  }

  protected AvroJobSpecKafkaJobMonitor(String topic, MutableJobCatalog catalog, Config limitedScopeConfig,
      SchemaVersionWriter<?> versionWriter) throws IOException {
    super(topic, catalog, limitedScopeConfig, AvroJobSpec.SCHEMA$, versionWriter);
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
  }

  /**
   * Creates a {@link JobSpec} or {@link URI} from the {@link AvroJobSpec} record.
   * @param record the record as an {@link AvroJobSpec}
   * @return a {@link JobSpec} or {@link URI} wrapped in a {@link Collection} of {@link Either}
   */
  @Override
  public Collection<Either<JobSpec, URI>> parseJobSpec(AvroJobSpec record) {
    JobSpec.Builder jobSpecBuilder = JobSpec.builder(record.getUri());

    Properties props = new Properties();
    props.putAll(record.getProperties());
    jobSpecBuilder.withJobCatalogURI(record.getUri()).withVersion(record.getVersion())
        .withDescription(record.getDescription()).withConfigAsProperties(props);

    if (!record.getTemplateUri().isEmpty()) {
      try {
        jobSpecBuilder.withTemplate(new URI(record.getTemplateUri()));
      } catch (URISyntaxException e) {
        log.error("could not parse template URI " + record.getTemplateUri());
      }
    }

    String verbName = record.getMetadata().get(VERB_KEY);
    Verb verb = Verb.valueOf(verbName);

    JobSpec jobSpec = jobSpecBuilder.build();

    log.info("Parsed job spec " + jobSpec.toString());

    if (verb == Verb.ADD || verb == Verb.UPDATE) {
      return Lists.newArrayList(Either.<JobSpec, URI>left(jobSpec));
    } else {
      return Lists.newArrayList(Either.<JobSpec, URI>right(jobSpec.getUri()));
    }
  }
}