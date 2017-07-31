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

package org.apache.gobblin.test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TestUtils {

  private static final Random rng = new Random();

  public static byte[] generateRandomBytes() {
    int length = rng.nextInt(200);
    return generateRandomBytes(length);
  }

  public static byte[] generateRandomBytes(int numBytes) {
    byte[] messageBytes = new byte[numBytes];
    rng.nextBytes(messageBytes);
    return messageBytes;
  }


  /**
   * TODO: Currently generates a static schema avro record.
  **/
  public static GenericRecord generateRandomAvroRecord() {

    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    String fieldName = "field1";
    Schema fieldSchema = Schema.create(Schema.Type.STRING);
    String docString = "doc";
    fields.add(new Schema.Field(fieldName, fieldSchema, docString, null));
    Schema schema = Schema.createRecord("name", docString, "test",false);
    schema.setFields(fields);

    GenericData.Record record = new GenericData.Record(schema);
    record.put("field1", "foobar");

    return record;

  }


  /**
   * Returns a free port number on localhost.
   *
   * Heavily inspired from org.eclipse.jdt.launching.SocketUtil (to avoid a dependency to JDT just because of this).
   * Slightly improved with close() missing in JDT. And throws exception instead of returning -1.
   *
   * @return a free port number on localhost
   * @throws IllegalStateException if unable to find a free port
   */
  public static int findFreePort() {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      try {
        socket.close();
      } catch (IOException e) {
        // Ignore IOException on close()
      }
      return port;
    } catch (IOException e) {
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
        }
      }
    }
    throw new IllegalStateException("Could not find a free TCP/IP port");
  }

}
