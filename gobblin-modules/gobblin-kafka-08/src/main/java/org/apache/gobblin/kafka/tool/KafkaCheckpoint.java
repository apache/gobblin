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

package gobblin.kafka.tool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.MapType;
import org.codehaus.jackson.map.type.TypeFactory;


/**
 * A class to store kafka checkpoints.
 * Knows how to serialize and deserialize itself.
 */
public class KafkaCheckpoint {
  private final HashMap<Integer, Long> _partitionOffsetMap;
  private static final ObjectMapper _mapper = new ObjectMapper();

  public static KafkaCheckpoint emptyCheckpoint() {return new KafkaCheckpoint(new HashMap<Integer, Long>());}

  public KafkaCheckpoint(HashMap<Integer, Long> partitionOffsetMap) {
    _partitionOffsetMap = partitionOffsetMap;
  }

  public void update(int partition, long offset) {
    _partitionOffsetMap.put(partition, offset);
  }

  public static KafkaCheckpoint deserialize(InputStream inputStream)
      throws IOException {
    TypeFactory typeFactory = _mapper.getTypeFactory();
    MapType mapType = typeFactory.constructMapType(HashMap.class, Integer.class, Long.class);
    HashMap<Integer, Long> checkpoint = _mapper.readValue(inputStream, mapType);
    return new KafkaCheckpoint(checkpoint);
  }

  public static void serialize(KafkaCheckpoint checkpoint, OutputStream outputStream)
      throws IOException {
    _mapper.writeValue(outputStream, checkpoint._partitionOffsetMap);
  }

  public static void serialize(KafkaCheckpoint checkpoint, File outputFile)
      throws IOException {
    _mapper.writeValue(outputFile, checkpoint._partitionOffsetMap);
  }

  public boolean isEmpty()
  {
    return _partitionOffsetMap.isEmpty();
  }


}
