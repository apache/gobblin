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
package org.apache.gobblin.compat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Strings;

import org.apache.gobblin.compat.hadoop.TextSerializer;


public class TextSerializerTest {
  private static final String[] textsToSerialize = new String[]{"abracadabra", Strings.repeat("longString", 128000)};

  @Test
  public void testSerialize()
      throws IOException {

    // Use our serializer, verify Hadoop deserializer can read it back
    for (String textToSerialize : textsToSerialize) {
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(bOs);

      TextSerializer.writeStringAsText(dataOutputStream, textToSerialize);
      dataOutputStream.close();

      ByteArrayInputStream bIn = new ByteArrayInputStream(bOs.toByteArray());
      DataInputStream dataInputStream = new DataInputStream(bIn);

      Text hadoopText = new Text();
      hadoopText.readFields(dataInputStream);

      Assert.assertEquals(hadoopText.toString(), textToSerialize);
    }
  }

  @Test
  public void testDeserialize() throws IOException {
    // Use Hadoop's serializer, verify our deserializer can read the string back
    for (String textToSerialize : textsToSerialize) {
      ByteArrayOutputStream bOs = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(bOs);

      Text hadoopText = new Text();
      hadoopText.set(textToSerialize);
      hadoopText.write(dataOutputStream);
      dataOutputStream.close();

      ByteArrayInputStream bIn = new ByteArrayInputStream(bOs.toByteArray());
      DataInputStream dataInputStream = new DataInputStream(bIn);

      String deserializedString = TextSerializer.readTextAsString(dataInputStream);

      Assert.assertEquals(deserializedString, textToSerialize);
    }
  }
}
