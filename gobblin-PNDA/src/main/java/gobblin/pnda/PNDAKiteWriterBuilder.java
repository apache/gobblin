/*
 * Copyright (c) 2016 Cisco and/or its affiliates.
 *
 * This software is licensed to you under the terms of the Apache License,
 * Version 2.0 (the "License"). You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * The code, technical concepts, and all information contained herein, are the
 * property of Cisco Technology, Inc. and/or its affiliated entities, under
 * various laws including copyright, international treaties, patent, and/or
 * contract. Any use of the material herein must be in accordance with the
 * terms of the License.  All rights not expressly granted by the License are
 * reserved.
 *
 * Unless required by applicable law or agreed to separately in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.pnda;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.DatasetNotFoundException;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import gobblin.pnda.PNDAKiteWriter;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes in Kite data.
 */
@SuppressWarnings("unused")
public class PNDAKiteWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {
  public static final String KITE_WRITER_DATASET_URI = "kite.writer.dataset.uri";

  @Override
  public final DataWriter<GenericRecord> build() throws IOException {
      State properties = this.destination.getProperties();

      String datasetURI = properties.getProp(KITE_WRITER_DATASET_URI);
      try {
          Dataset events = Datasets.load(datasetURI);
          return new PNDAKiteWriter(properties, events);
      } catch (DatasetNotFoundException error) {
          throw new RuntimeException(
                          String.format("Unable to read dataset '%s'", datasetURI), error);
      }
  }
}
