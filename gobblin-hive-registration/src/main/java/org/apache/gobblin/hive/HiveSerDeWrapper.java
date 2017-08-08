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

package org.apache.gobblin.hive;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;


/**
 * A wrapper around {@link SerDe} that bundles input format, output format and file extension with a {@link SerDe},
 * and provides additional functionalities.
 *
 * @author Ziyang Liu
 */
@Alpha
@SuppressWarnings("deprecation")
public class HiveSerDeWrapper {

  private static final String SERDE_SERIALIZER_PREFIX = "serde.serializer.";
  private static final String SERDE_DESERIALIZER_PREFIX = "serde.deserializer.";

  public static final String SERDE_SERIALIZER_TYPE = SERDE_SERIALIZER_PREFIX + "type";
  public static final String SERDE_SERIALIZER_INPUT_FORMAT_TYPE = SERDE_SERIALIZER_PREFIX + "input.format.type";
  public static final String SERDE_SERIALIZER_OUTPUT_FORMAT_TYPE = SERDE_SERIALIZER_PREFIX + "output.format.type";

  public static final String SERDE_DESERIALIZER_TYPE = SERDE_DESERIALIZER_PREFIX + "type";
  public static final String SERDE_DESERIALIZER_INPUT_FORMAT_TYPE = SERDE_DESERIALIZER_PREFIX + "input.format.type";
  public static final String SERDE_DESERIALIZER_OUTPUT_FORMAT_TYPE = SERDE_DESERIALIZER_PREFIX + "output.format.type";

  public enum BuiltInHiveSerDe {

    AVRO(AvroSerDe.class.getName(), AvroContainerInputFormat.class.getName(),
        AvroContainerOutputFormat.class.getName()),
    ORC(OrcSerde.class.getName(), OrcInputFormat.class.getName(), OrcOutputFormat.class.getName()),
    PARQUET(ParquetHiveSerDe.class.getName(), MapredParquetInputFormat.class.getName(),
        MapredParquetOutputFormat.class.getName()),
    TEXTFILE(LazySimpleSerDe.class.getName(), TextInputFormat.class.getName(),
        HiveIgnoreKeyTextOutputFormat.class.getName());

    private final String serDeClassName;
    private final String inputFormatClassName;
    private final String outputFormatClassName;

    private BuiltInHiveSerDe(String serDeClassName, String inputFormatClassName, String outputFormatClassName) {
      this.serDeClassName = serDeClassName;
      this.inputFormatClassName = inputFormatClassName;
      this.outputFormatClassName = outputFormatClassName;
    }

    @Override
    public String toString() {
      return this.serDeClassName;
    }
  }

  private Optional<SerDe> serDe = Optional.absent();
  private final String serDeClassName;
  private final String inputFormatClassName;
  private final String outputFormatClassName;

  private HiveSerDeWrapper(BuiltInHiveSerDe hiveSerDe) {
    this(hiveSerDe.serDeClassName, hiveSerDe.inputFormatClassName, hiveSerDe.outputFormatClassName);
  }

  private HiveSerDeWrapper(String serDeClassName, String inputFormatClassName, String outputFormatClassName) {
    this.serDeClassName = serDeClassName;
    this.inputFormatClassName = inputFormatClassName;
    this.outputFormatClassName = outputFormatClassName;
  }

  /**
   * Get the {@link SerDe} instance associated with this {@link HiveSerDeWrapper}.
   * This method performs lazy initialization.
   */
  public SerDe getSerDe() throws IOException {
    if (!this.serDe.isPresent()) {
      try {
        this.serDe = Optional.of(SerDe.class.cast(Class.forName(this.serDeClassName).newInstance()));
      } catch (Throwable t) {
        throw new IOException("Failed to instantiate SerDe " + this.serDeClassName, t);
      }
    }
    return this.serDe.get();
  }

  /**
   * Get the input format class name associated with this {@link HiveSerDeWrapper}.
   */
  public String getInputFormatClassName() {
    return this.inputFormatClassName;
  }

  /**
   * Get the output format class name associated with this {@link HiveSerDeWrapper}.
   */
  public String getOutputFormatClassName() {
    return this.outputFormatClassName;
  }

  /**
   * Get an instance of {@link HiveSerDeWrapper}.
   *
   * @param serDeType The SerDe type. This should be one of the available {@link HiveSerDeWrapper.BuiltInHiveSerDe}s.
   */
  public static HiveSerDeWrapper get(String serDeType) {
    return get(serDeType, Optional.<String> absent(), Optional.<String> absent());
  }

  /**
   * Get an instance of {@link HiveSerDeWrapper}.
   *
   * @param serDeType The SerDe type. If serDeType is one of the available {@link HiveSerDeWrapper.BuiltInHiveSerDe},
   * the other three parameters are not used. Otherwise, serDeType should be the class name of a {@link SerDe},
   * and the other three parameters must be present.
   */
  public static HiveSerDeWrapper get(String serDeType, Optional<String> inputFormatClassName,
      Optional<String> outputFormatClassName) {
    Optional<BuiltInHiveSerDe> hiveSerDe = Enums.getIfPresent(BuiltInHiveSerDe.class, serDeType.toUpperCase());
    if (hiveSerDe.isPresent()) {
      return new HiveSerDeWrapper(hiveSerDe.get());
    }
    Preconditions.checkArgument(inputFormatClassName.isPresent(),
        "Missing input format class name for SerDe " + serDeType);
    Preconditions.checkArgument(outputFormatClassName.isPresent(),
        "Missing output format class name for SerDe " + serDeType);
    return new HiveSerDeWrapper(serDeType, inputFormatClassName.get(), outputFormatClassName.get());
  }

  /**
   * Get an instance of {@link HiveSerDeWrapper} from a {@link State}.
   *
   * @param state The state should contain property {@link #SERDE_SERIALIZER_TYPE}, and optionally contain properties
   * {@link #SERDE_SERIALIZER_INPUT_FORMAT_TYPE}, {@link #SERDE_SERIALIZER_OUTPUT_FORMAT_TYPE} and
   */
  public static HiveSerDeWrapper getSerializer(State state) {
    Preconditions.checkArgument(state.contains(SERDE_SERIALIZER_TYPE),
        "Missing required property " + SERDE_SERIALIZER_TYPE);
    return get(state.getProp(SERDE_SERIALIZER_TYPE),
        Optional.fromNullable(state.getProp(SERDE_SERIALIZER_INPUT_FORMAT_TYPE)),
        Optional.fromNullable(state.getProp(SERDE_SERIALIZER_OUTPUT_FORMAT_TYPE)));
  }

  /**
   * Get an instance of {@link HiveSerDeWrapper} from a {@link State}.
   *
   * @param state The state should contain property {@link #SERDE_DESERIALIZER_TYPE}, and optionally contain properties
   * {@link #SERDE_DESERIALIZER_INPUT_FORMAT_TYPE}, {@link #SERDE_DESERIALIZER_OUTPUT_FORMAT_TYPE} and
   */
  public static HiveSerDeWrapper getDeserializer(State state) {
    Preconditions.checkArgument(state.contains(SERDE_DESERIALIZER_TYPE),
        "Missing required property " + SERDE_DESERIALIZER_TYPE);
    return get(state.getProp(SERDE_DESERIALIZER_TYPE),
        Optional.fromNullable(state.getProp(SERDE_DESERIALIZER_INPUT_FORMAT_TYPE)),
        Optional.fromNullable(state.getProp(SERDE_DESERIALIZER_OUTPUT_FORMAT_TYPE)));
  }
}
