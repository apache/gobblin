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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.FileBasedSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.HadoopUtils;

/**
 * Unified Iceberg source that supports both file streaming (eg. Azure/HDFS copy) and record processing (eg. writing to SQL).
 * 
 * File Streaming Mode ({@code iceberg.record.processing.enabled=false})
 * Returns {@link IcebergFileStreamExtractor} that streams files as {@link FileAwareInputStream}
 * Uses {@link org.apache.gobblin.converter.IdentityConverter} for pass-through
 * 
 * Record Processing Mode ({@code iceberg.record.processing.enabled=true}):
 * Returns future record extractor that reads records
 * Uses converter chain for transformation (SQL, Avro, etc.)
 * Note: Not yet implemented - this is for future extension
 * 
 * Example configurations:
 * # File streaming to Azure
 * source.class=org.apache.gobblin.data.management.copy.iceberg.IcebergSource
 * iceberg.record.processing.enabled=false
 * converter.classes=org.apache.gobblin.converter.IdentityConverter
 */
@Slf4j
public class IcebergSource extends FileBasedSource<String, FileAwareInputStream> {

    public static final String ICEBERG_DATABASE_NAME = "iceberg.database.name";
    public static final String ICEBERG_TABLE_NAME = "iceberg.table.name";
    public static final String ICEBERG_CATALOG_URI = "iceberg.catalog.uri";
    public static final String ICEBERG_CATALOG_CLASS = "iceberg.catalog.class";
    public static final String DEFAULT_ICEBERG_CATALOG_CLASS = "org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog";
    public static final String ICEBERG_RECORD_PROCESSING_ENABLED = "iceberg.record.processing.enabled";
    public static final boolean DEFAULT_RECORD_PROCESSING_ENABLED = false;

    private Optional<LineageInfo> lineageInfo;

    /**
     * Initialize file system helper based on mode (streaming vs record processing)
     */
    @Override
    public void initFileSystemHelper(State state) throws FileBasedHelperException {
        // For file streaming mode, we use IcebergFileStreamHelper
        // For record processing mode, we'll use a different helper (future implementation)
        boolean recordProcessingEnabled = state.getPropAsBoolean(
            ICEBERG_RECORD_PROCESSING_ENABLED, DEFAULT_RECORD_PROCESSING_ENABLED);
            
        if (recordProcessingEnabled) {
            // Future: Initialize helper for record processing
            throw new UnsupportedOperationException("Record processing mode not yet implemented. " +
                "This will be added when SQL/Data Lake destinations are required.");
        } else {
            // Initialize helper for file streaming - now implements TimestampAwareFileBasedHelper
            this.fsHelper = new IcebergFileStreamHelper(state);
            this.fsHelper.connect();
        }
    }

    /**
     * Get work units by discovering files from Iceberg table
     * @param state is the source state
     * @return List<WorkUnit> list of work units
     */
    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        this.lineageInfo = LineageInfo.getLineageInfo(state.getBroker());
        
        try {
            // Initialize file system helper
            initFileSystemHelper(state);
            
            // Validate configuration
            validateConfiguration(state);
            
            // Connect to OpenHouse and discover files
            IcebergCatalog catalog = createCatalog(state);
            String database = state.getProp(ICEBERG_DATABASE_NAME);
            String table = state.getProp(ICEBERG_TABLE_NAME);
            
            IcebergTable icebergTable = catalog.openTable(database, table);
            Preconditions.checkArgument(catalog.tableAlreadyExists(icebergTable), 
                String.format("OpenHouse table not found: %s.%s", database, table));
            
            List<String> allFilePaths = discoverAllFilePaths(icebergTable);
            log.info("Discovered {} files from OpenHouse table {}.{}", allFilePaths.size(), database, table);
            
            // Create work units (common path for both streaming and future record processing)
            return createWorkUnitsFromFiles(allFilePaths, state, icebergTable);
            
        } catch (Exception e) {
            log.error("Failed to create work units for OpenHouse table", e);
            throw new RuntimeException("Failed to create work units", e);
        }
    }

    /**
     * Get extractor based on mode (streaming vs record processing)
     *
     * @param state a {@link org.apache.gobblin.configuration.WorkUnitState} carrying properties needed by the returned {@link Extractor}
     * @return
     * @throws IOException
     */
    @Override
    public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {
        boolean recordProcessingEnabled = state.getPropAsBoolean(
            ICEBERG_RECORD_PROCESSING_ENABLED, DEFAULT_RECORD_PROCESSING_ENABLED);
            
        if (recordProcessingEnabled) {
            // Return record processing extractor
            throw new UnsupportedOperationException("Record processing mode not yet implemented.");
        } else {
            // Return file streaming extractor
            return new IcebergFileStreamExtractor(state);
        }
    }

    /**
     * Discover all files from OpenHouse table
     */
    private List<String> discoverAllFilePaths(IcebergTable icebergTable) throws IOException {
        List<String> allFilePaths = Lists.newArrayList();
        
        // Get current snapshot info
        IcebergSnapshotInfo currentSnapshot = icebergTable.getCurrentSnapshotInfo();
        
        // Add manifest list path
        allFilePaths.add(currentSnapshot.getManifestListPath());
        
        // Add metadata file path if present
        currentSnapshot.getMetadataPath().ifPresent(allFilePaths::add);
        
        // Add manifest files and their data files
        for (IcebergSnapshotInfo.ManifestFileInfo manifestInfo : currentSnapshot.getManifestFiles()) {
            allFilePaths.add(manifestInfo.getManifestFilePath());
            allFilePaths.addAll(manifestInfo.getListedFilePaths());
        }
        
        return allFilePaths;
    }

    /**
     * Create work units from discovered file paths
     */
    private List<WorkUnit> createWorkUnitsFromFiles(List<String> filePaths, SourceState state, IcebergTable table) {
        List<WorkUnit> workUnits = Lists.newArrayList();
        
        String nameSpace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "iceberg");
        String tableName = table.getTableId().name();
        Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, nameSpace, tableName);

        for (int i = 0; i < filePaths.size(); i++) {
            WorkUnit workUnit = new WorkUnit(extract);
            
            // Set files for extractor
            workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, filePaths.get(i));
            
            // Add lineage information
            addLineageSourceInfo(state, workUnit, table);
            
            workUnits.add(workUnit);
        }
        
        return workUnits;
    }

    /**
     * Create catalog using existing IcebergDatasetFinder logic
     */
    private IcebergCatalog createCatalog(SourceState state) throws IOException {
        String catalogPrefix = "iceberg.catalog.";
        Map<String, String> catalogProperties = buildMapFromPrefixChildren(state.getProperties(), catalogPrefix);
        
        Configuration configuration = HadoopUtils.getConfFromProperties(state.getProperties());
        String catalogClassName = catalogProperties.getOrDefault("class", DEFAULT_ICEBERG_CATALOG_CLASS);
        
        return IcebergCatalogFactory.create(catalogClassName, catalogProperties, configuration);
    }

    /**
     * Build map of properties with given prefix
     *
     */
    private Map<String, String> buildMapFromPrefixChildren(Properties properties, String configPrefix) {
        Map<String, String> catalogProperties = Maps.newHashMap();
        
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(configPrefix)) {
                String relativeKey = key.substring(configPrefix.length());
                catalogProperties.put(relativeKey, (String) entry.getValue());
            }
        }
        
        String catalogUri = catalogProperties.get("uri");
        Preconditions.checkNotNull(catalogUri, "Catalog URI is required");
        
        return catalogProperties;
    }

    /**
     * Add lineage information
     */
    private void addLineageSourceInfo(SourceState sourceState, WorkUnit workUnit, IcebergTable table) {
        if (lineageInfo.isPresent()) {
            String catalogUri = sourceState.getProp(ICEBERG_CATALOG_URI);
            String database = sourceState.getProp(ICEBERG_DATABASE_NAME);
            String tableName = sourceState.getProp(ICEBERG_TABLE_NAME);
            
            DatasetDescriptor source = new DatasetDescriptor(
                DatasetConstants.PLATFORM_ICEBERG,
                URI.create(catalogUri),
                database + "." + tableName
            );
            
            source.addMetadata("catalog.uri", catalogUri);
            source.addMetadata("table.location", getTableLocation(table));
            
            lineageInfo.get().setSource(source, workUnit);
        }
    }

    /**
     * Get table location from Iceberg table metadata
     * @param table the Iceberg table
     * @return table location or "unknown" if not available
     */
    private String getTableLocation(IcebergTable table) {
        try {
            return table.accessTableMetadata().location();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Validate required configuration properties
     */
    private void validateConfiguration(SourceState state) {
        String database = state.getProp(ICEBERG_DATABASE_NAME);
        String table = state.getProp(ICEBERG_TABLE_NAME);
        String catalogUri = state.getProp(ICEBERG_CATALOG_URI);
        
        if (StringUtils.isBlank(database)) {
            throw new IllegalArgumentException("iceberg.database.name is required");
        }
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("iceberg.table.name is required");
        }
        if (StringUtils.isBlank(catalogUri)) {
            throw new IllegalArgumentException("iceberg.catalog.uri is required");
        }
    }

    /**
     *  Get current filesystem snapshot(list of files) from Iceberg table
     */
    @Override
    public List<String> getcurrentFsSnapshot(State state) {
        // Required by FileBasedSource - return discovered files
        try {
            IcebergCatalog catalog = createCatalog((SourceState) state);
            String database = state.getProp(ICEBERG_DATABASE_NAME);
            String table = state.getProp(ICEBERG_TABLE_NAME);
            IcebergTable icebergTable = catalog.openTable(database, table);
            
            return discoverAllFilePaths(icebergTable);
        } catch (Exception e) {
            log.error("Failed to get current filesystem snapshot", e);
            return Lists.newArrayList();
        }
    }

}
