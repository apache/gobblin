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

package gobblin.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import gobblin.codec.StreamCodec;
import gobblin.compression.CompressionConfigParser;
import gobblin.compression.CompressionFactory;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.EncryptionFactory;


/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes bytes.
 *
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriterBuilder extends FsDataWriterBuilder<String, Object> {
  /**
   * Build a {@link gobblin.writer.DataWriter}.
   *
   * @return the built {@link gobblin.writer.DataWriter}
   * @throws java.io.IOException if there is anything wrong building the writer
   */
  @Override
  public DataWriter<Object> build() throws IOException {
    return new MetadataWriterWrapper<byte[]>(new SimpleDataWriter(this, this.destination.getProperties()),
        byte[].class,
        this.branches,
        this.branch,
        this.destination.getProperties());
  }

  @Override
  protected List<StreamCodec> buildEncoders() {
    Preconditions.checkNotNull(this.destination, "Destination must be set before building encoders");

    List<StreamCodec> encoders = new ArrayList<>();

    // TODO: refactor this when capability support comes back in

    // Compress first since compressing encrypted data will give no benefit
    Map<String, Object> compressionConfig =
        CompressionConfigParser.getConfigForBranch(this.destination.getProperties(), this.branches, this.branch);
    if (compressionConfig != null) {
      encoders.add(CompressionFactory.buildStreamCompressor(compressionConfig));
    }

    Map<String, Object> encryptionConfig = EncryptionConfigParser
        .getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, this.destination.getProperties(), this.branches,
            this.branch);
    if (encryptionConfig != null) {
      encoders.add(EncryptionFactory.buildStreamCryptoProvider(encryptionConfig));
    }

    return encoders;
  }
}
