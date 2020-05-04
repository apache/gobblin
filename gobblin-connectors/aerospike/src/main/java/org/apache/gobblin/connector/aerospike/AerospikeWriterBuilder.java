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

package org.apache.gobblin.connector.aerospike;

import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.DataWriterBuilder;

import java.util.Properties;

/**
 * Aerospike writer builder class
 */
public class AerospikeWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

    @Override
    public AerospikeAvroWriter build() {
        State state = this.destination.getProperties();
        Properties taskProps = state.getProperties();
        Config config = ConfigUtils.propertiesToConfig(taskProps);

        AerospikeConfig aerospikeConfig = new AerospikeConfig(config);
        return new AerospikeAvroWriter(aerospikeConfig, this.getSchema());
    }

    @Override
    public Schema getSchema() {
        return super.getSchema();
    }

}
