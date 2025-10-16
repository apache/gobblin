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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.base.Optional;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.source.extractor.filebased.FileBasedExtractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.util.WriterUtils;

/**
 * Extractor for file streaming mode that creates FileAwareInputStream for each file.
 * 
 * This extractor is used when {@code iceberg.record.processing.enabled=false} to stream
 * OpenHouse table files as binary data to destinations like Azure, HDFS</p>
 * 
 * Each "record" is a {@link FileAwareInputStream} representing one file from
 * the OpenHouse table. The downstream writer handles streaming the file content.
 */
@Slf4j
public class IcebergFileStreamExtractor extends FileBasedExtractor<String, FileAwareInputStream> {

    public IcebergFileStreamExtractor(WorkUnitState workUnitState) throws IOException {
        super(workUnitState, new IcebergFileStreamHelper(workUnitState));
    }

    @Override
    public String getSchema() {
        // For file streaming, schema is not used by IdentityConverter; returning a constant
        return "FileAwareInputStream";
    }

    @Override
    public Iterator<FileAwareInputStream> downloadFile(String filePath) throws IOException {
        throw new NotImplementedException("Not yet implemented");
    }

}
