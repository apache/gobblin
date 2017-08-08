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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.source.extractor.Extractor;

public class KafkaSimpleJsonExtractor extends KafkaSimpleExtractor implements Extractor<String, byte[]> {

    private static final Gson gson = new Gson();
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public KafkaSimpleJsonExtractor(WorkUnitState state) {
        super(state);
    }

    @Override
    protected byte[] decodeRecord(ByteArrayBasedKafkaRecord messageAndOffset) throws IOException {
        long offset = messageAndOffset.getOffset();

        byte[] keyBytes = messageAndOffset.getKeyBytes();
        String key = (keyBytes == null) ? "" : new String(keyBytes, CHARSET);

        byte[] payloadBytes = messageAndOffset.getMessageBytes();
        String payload = (payloadBytes == null) ? "" : new String(payloadBytes, CHARSET);

        KafkaRecord record = new KafkaRecord(offset, key, payload);

        byte[] decodedRecord = gson.toJson(record).getBytes(CHARSET);
        return decodedRecord;
    }

}
