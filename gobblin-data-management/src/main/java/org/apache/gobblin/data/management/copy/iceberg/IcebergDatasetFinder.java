package org.apache.gobblin.data.management.copy.iceberg;

import azkaban.utils.StringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
@AllArgsConstructor
public class IcebergDatasetFinder implements IterableDatasetFinder<IcebergDataset> {

  public static final String ICEBERG_DATASET_PREFIX = "iceberg.dataset";
  public static final String ICEBERG_METASTORE_URI_KEY = ICEBERG_DATASET_PREFIX + ".hive.metastore.uri";
  public static final String ICEBERG_CATALOG_TYPE = ICEBERG_DATASET_PREFIX + ".catalog.type";
  public static final String DEFAULT_ICEBERG_CATALOG_TYPE = CatalogType.HIVE.name();
  public static final String ICEBERG_DB_NAME = DatasetConstants.PLATFORM_ICEBERG + ".database.name";
  public static final String ICEBERG_TABLE_NAME = DatasetConstants.PLATFORM_ICEBERG + ".table.name";


  private String dbName;
  private String tblName;
  private final Properties properties;
  protected final FileSystem fs;

  public enum CatalogType {
    HADOOP,
    HIVE
  }

  @Override
  public List<IcebergDataset> findDatasets() throws IOException {
    List<IcebergDataset> matchingDatasets = new ArrayList<>();
    this.dbName = properties.getProperty(ICEBERG_DB_NAME, "");
    this.tblName = properties.getProperty(ICEBERG_TABLE_NAME, "");

    Configuration configuration = HadoopUtils.getConfFromProperties(properties);

    // TODO property type for Catalog to pick the Hive Catalog
    IcebergCatalog icebergCatalog = IcebergCatalogFactory.create(configuration);

    IcebergTable icebergTable = icebergCatalog.openTable(dbName, tblName);

    matchingDatasets.add(new IcebergDataset(dbName, tblName, icebergTable, properties, fs));
    log.info("Found {} matching datasets: {}", matchingDatasets.size(), matchingDatasets);

    return matchingDatasets;
  }

  @Override
  public Path commonDatasetRoot() {
    return null;
  }

  @Override
  public Iterator<IcebergDataset> getDatasetsIterator() throws IOException {
    return findDatasets().iterator();
  }
}
