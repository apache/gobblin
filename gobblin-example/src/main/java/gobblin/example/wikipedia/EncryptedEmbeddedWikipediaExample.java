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

package gobblin.example.wikipedia;

import avro.shaded.com.google.common.base.Joiner;
import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.publisher.BaseDataPublisher;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.cli.EmbeddedGobblinCliOption;
import gobblin.runtime.cli.EmbeddedGobblinCliSupport;
import gobblin.runtime.cli.PublicMethodsGobblinCliFactory;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.runtime.template.ResourceBasedJobTemplate;
import gobblin.writer.AvroDataWriterBuilder;
import gobblin.writer.Destination;
import gobblin.writer.WriterOutputFormat;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.commons.cli.CommandLine;


/**
 * Embedded Gobblin to run Wikipedia example.
 */
public class EncryptedEmbeddedWikipediaExample extends EmbeddedGobblin {

  @Alias(value = "encrypted_wikipedia", description = "Gobblin example that downloads revisions from Wikipedia.")
  public static class CliFactory extends PublicMethodsGobblinCliFactory {

    public CliFactory() {
      super(EncryptedEmbeddedWikipediaExample.class);
    }

    @Override
    public EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli) throws JobTemplate.TemplateException, IOException {
      String[] leftoverArgs = cli.getArgs();
      if (leftoverArgs.length < 1) {
        throw new RuntimeException("Unexpected number of arguments.");
      }
      return new EncryptedEmbeddedWikipediaExample(leftoverArgs);
    }

    @Override
    public String getUsageString() {
      return "[OPTIONS] <article-title> [<article-title> ...]";
    }
  }

  @EmbeddedGobblinCliSupport(argumentNames = {"topics"})
  public EncryptedEmbeddedWikipediaExample(String... topics) throws JobTemplate.TemplateException, IOException {
    super("Wikipedia");
    try {
      setTemplate(ResourceBasedJobTemplate.forResourcePath("wikipedia.template"));
    } catch (URISyntaxException | SpecNotFoundException exc) {
      throw new RuntimeException("Could not instantiate an " + EncryptedEmbeddedWikipediaExample.class.getName(), exc);
    }
    this.setConfiguration("titles", Joiner.on(",").join(topics));
  }

  /**
   * Set bootstrap lookback, i.e. oldest revision to pull.
   */
  @EmbeddedGobblinCliOption(description = "Sets the period for which articles should be pulled in ISO time format (e.g. P2D, PT1H)")
  public EncryptedEmbeddedWikipediaExample lookback(String isoLookback) {
    this.setConfiguration(WikipediaExtractor.BOOTSTRAP_PERIOD, isoLookback);
    return this;
  }

  /**
   * Write output to avro files at the given input location.
   */
  @EmbeddedGobblinCliOption(description = "Write output to Avro files. Specify the output directory as argument.")
  public EncryptedEmbeddedWikipediaExample avroOutput(String outputPath) {
    this.setConfiguration(ConfigurationKeys.WRITER_BUILDER_CLASS, AvroDataWriterBuilder.class.getName());
    this.setConfiguration(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, Destination.DestinationType.HDFS.name());
    this.setConfiguration(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, WriterOutputFormat.AVRO.name());
    this.setConfiguration(ConfigurationKeys.WRITER_PARTITIONER_CLASS, WikipediaPartitioner.class.getName());
    this.setConfiguration(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE, BaseDataPublisher.class.getName());
    this.setConfiguration(ConfigurationKeys.CONVERTER_CLASSES_KEY, WikipediaConverter.class.getName());
    this.setConfiguration(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, outputPath);
    this.setConfiguration(ConfigurationKeys.WRITER_ENABLE_ENCRYPT, "aes_rotating");
    this.setConfiguration(ConfigurationKeys.WRITER_PREFIX + ".ks_path", "/tmp/keystore");
    return this;
  }

}
