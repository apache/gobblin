package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
@AllArgsConstructor
public class IcebergDatasetFinder implements IterableDatasetFinder<IcebergDataset> {

  private final String dbName;
  private final String tblName;
  private final Properties properties;
  protected final FileSystem fs;


  @Override
  public List<IcebergDataset> findDatasets() throws IOException {
    List<IcebergDataset> matchingDatasets = new ArrayList<>();
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
