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

package org.apache.gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.kafka.writer.KafkaDataWriter;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;


public class HighLevelGobblinKafkaProducer<K, V> {
    private final static String METRICS_PRODUCER_WRITER_TYPE = "metrics.producer.writer.type";
    private final static String DEFAULT_METRICS_PRODUCER_WRITER_TYPE = "org.apache.gobblin.kafka.writer.Kafka08DataWriter";
    private KafkaDataWriter<K, V> writer;
    public HighLevelGobblinKafkaProducer(Properties props) {
       writer = GobblinConstructorUtils.invokeConstructor(KafkaDataWriter.class,
           props.getProperty(METRICS_PRODUCER_WRITER_TYPE, DEFAULT_METRICS_PRODUCER_WRITER_TYPE),
           props);
    }
    public Future<WriteResponse> sendMessage(V value, Function<V, K> mapFunction, WriteCallback callback){
        return writer.write(new ImmutablePair<K, V>(mapFunction.apply(value), value), callback);
    }

    public void flush() throws IOException {
        this.writer.flush();
    }


}
