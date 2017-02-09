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
package gobblin.data.management.copy.writer;

import gobblin.capability.EncryptionCapabilityParser;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import gobblin.capability.Capability;


public class FileAwareInputStreamDataWriterBuilderTest {
  @Test
  public void testCapabilitySupport() {
    FileAwareInputStreamDataWriterBuilder builder = new FileAwareInputStreamDataWriterBuilder();

    Assert.assertTrue(builder.supportsCapability(Capability.ENCRYPTION,
        ImmutableMap.<String, Object>of(EncryptionCapabilityParser.ENCRYPTION_TYPE_PROPERTY, "insecure_shift")
    ));
    Assert.assertTrue(builder.supportsCapability(Capability.ENCRYPTION,
        ImmutableMap.<String, Object>of(EncryptionCapabilityParser.ENCRYPTION_TYPE_PROPERTY, "any")
    ));

  }
}
