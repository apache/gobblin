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
package org.apache.gobblin.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.gobblin.configuration.State;

import java.io.IOException;

public class IcebergWriter<D> extends FsDataWriter<D>{

    public IcebergWriter(IcebergDataWriterBuilder builder, State properties)
            throws IOException {
        super(builder, properties);

    }

    @Override
    public long recordsWritten() {
        return 0;
    }
}
