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
package gobblin.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import lombok.extern.slf4j.Slf4j;


/**
 * Credential store that reads a JSON map that looks like:
 * {
 *   "1": "<hex encoded key>",
 *   "2": "<hex encoded key>"
 * }
 */
@Slf4j
public class JsonCredentialStore implements CredentialStore {
  private static final ObjectMapper defaultMapper = new ObjectMapper();
  public final static String TAG = "json";

  private Map<String, byte[]> credentials;

  /**
   * Instantiate a new keystore using the file at the provided path
   */
  public JsonCredentialStore(String path, KeyToStringCodec codec) throws IOException {
    this(new Path(path), codec);
  }

  /**
   * Instantiate a new keystore using the file at the provided path
   */
  public JsonCredentialStore(Path path, KeyToStringCodec codec) throws IOException {
    credentials =  new HashMap<>();

    FileSystem fs = path.getFileSystem(new Configuration());
    try (InputStream in = fs.open(path)) {
      ObjectMapper jsonParser = defaultMapper;
      JsonNode tree = jsonParser.readTree(in);
      if (!tree.isObject()) {
        throw new IllegalArgumentException("Json in " + path.toString() + " is not an object!");
      }

      Iterator<Map.Entry<String, JsonNode>> it = tree.getFields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> field = it.next();
        String keyId = field.getKey();
        byte[] key = codec.decodeKey(field.getValue().getTextValue());

        credentials.put(keyId, key);
      }
    }

    log.info("Initialized keystore from {} with {} keys", path.toString(), credentials.size());
  }

  @Override
  public byte[] getEncodedKey(String id) {
    return credentials.get(id);
  }

  @Override
  public Map<String, byte[]> getAllEncodedKeys() {
    return Collections.unmodifiableMap(credentials);
  }
}
