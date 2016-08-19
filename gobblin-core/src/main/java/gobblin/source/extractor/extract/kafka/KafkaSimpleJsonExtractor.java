/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import kafka.message.MessageAndOffset;

public class KafkaSimpleJsonExtractor extends KafkaSimpleExtractor implements Extractor<String, byte[]> {

    private static final Gson gson = new Gson();
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public KafkaSimpleJsonExtractor(WorkUnitState state) {
        super(state);
    }

    @Override
    protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
        long offset = messageAndOffset.offset();

        byte[] keyBytes = getBytes(messageAndOffset.message().key());
        String key = (keyBytes == null) ? "" : new String(keyBytes, CHARSET);

        byte[] payloadBytes = getBytes(messageAndOffset.message().payload());
        String payload = (payloadBytes == null) ? "" : new String(payloadBytes, CHARSET);

        KafkaRecord record = new KafkaRecord(offset, key, payload);

        byte[] decodedRecord = gson.toJson(record).getBytes(CHARSET);
        return decodedRecord;
    }

}
