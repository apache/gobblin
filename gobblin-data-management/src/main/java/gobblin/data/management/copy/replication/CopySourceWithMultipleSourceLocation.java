package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableDatasetBase;
import gobblin.data.management.copy.extractor.EmptyExtractor;
import gobblin.data.management.copy.extractor.FileAwareInputStreamExtractor;
import gobblin.data.management.copy.publisher.CopyEventSubmitterHelper;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkGenerator;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkHelper;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.FileSet;
import gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.dataset.IterableDatasetFinderImpl;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitWeighter;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.PathUtils;
import gobblin.util.WriterUtils;
import gobblin.util.binpacking.FieldWeighter;
import gobblin.util.binpacking.WorstFitDecreasingBinPacking;
import gobblin.util.executors.IteratorExecutor;
import gobblin.util.guid.Guid;
import gobblin.util.iterators.InterruptibleIterator;
import gobblin.data.management.copy.ConcurrentBoundedWorkUnitList;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;

@Slf4j
public class CopySourceWithMultipleSourceLocation extends CopySource{

  public static final String CONFIG_STORE_ROOT = "config.store.root";
  public static final String CONFIG_STORE_REPLICATION_ROOT = "config.store.replication.root";
  public static final String CONFIG_STORE_REPLICATION_TAG = "config.store.replication.tag";

  public static void displayAllConfig(Config c){
    for(Map.Entry<String, ConfigValue> entry :c.entrySet()){
      log.info("AAA key " + entry.getKey() + " value " + entry.getValue());
    }
  }

  private DataFlowTopology.CopyRoute getCopyRoute(DataFlowTopology topology)throws IOException{
    FileSystem executionCluster = FileSystem.get(new Configuration());
    URI executionClusterURI = executionCluster.getUri();
    List<DataFlowTopology.CopyRoute> routes = topology.getRoutes();
    for(DataFlowTopology.CopyRoute route: routes){
      HdfsReplicationLocation hdfsForCopyDestination = (HdfsReplicationLocation)(route.getCopyDestination().getReplicationLocation());
      try {
        URI destURI = new URI(hdfsForCopyDestination.getClustername());
        if(destURI.equals(executionClusterURI)){
          return route;
        }
      } catch (URISyntaxException e) {
        log.warn("Can not generate namenode URI from " + hdfsForCopyDestination.getClustername());
      }
    }

    throw new RuntimeException("can not find route for execution cluster " + executionClusterURI);
  }

  @Override
  protected boolean generateWorkunitsBasedOnConfig(final SourceState originState, 
      final ConcurrentBoundedWorkUnitList workUnitList, final long minWorkUnitWeight) throws IOException{
    List<Config> configs = this.generateReplicationConfigs(originState);
    
    for(Config config: configs){
      ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(config);
      if(!generateWorkunitsPerReplicationDataset(originState,workUnitList, minWorkUnitWeight,  rc)){
        return false;
      }
    }
    return true;
//    String storeRoot;
//    ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
//    try {
//
//      if(originState.contains(CONFIG_STORE_ROOT)){
//        storeRoot = originState.getProp(CONFIG_STORE_ROOT);
//      }
//      else{
//        throw new RuntimeException("Can not find config store root");
//      }
//
//      //public Collection<URI> getImportedBy(URI configKeyUri, boolean recursive)
//      Collection<URI> importedBy = configClient.getImportedBy(new URI(storeRoot+"/tag/testTag"), true);
//      for(URI u: importedBy){
//        log.info("AAA imported by is " + u);
//      }
//
//      log.info("generate workunit for /data/derived/data_derived_test_src");
//      Config c = configClient.getConfig(storeRoot+"/data/derived/data_derived_test_src/");
//      //displayAllConfig(c);
//      ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);
//
//      generateWorkunitsPerReplicationDataset(originState,workUnitList, minWorkUnitWeight,  rc);
//
//      log.info("generate workunit for /data/derived/supertitleToSkills");
//      c = configClient.getConfig(storeRoot+"/data/derived/supertitleToSkills/");
//      rc = ReplicationConfiguration.buildFromConfig(c);
//      return generateWorkunitsPerReplicationDataset(originState,workUnitList, minWorkUnitWeight,  rc);
//
//
//    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
//        | URISyntaxException e) {
//      log.error("Caught error " + e.getMessage());
//      throw new RuntimeException(e);
//    }
  }

  private List<Config> generateReplicationConfigs(final SourceState originState){
    try {
      String storeRoot;
      String replicationRootPath;
      String replicationTag;
      ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
      if(originState.contains(CONFIG_STORE_ROOT) && originState.contains(CONFIG_STORE_REPLICATION_ROOT) && originState.contains(CONFIG_STORE_REPLICATION_TAG)){
        storeRoot = originState.getProp(CONFIG_STORE_ROOT);
        replicationRootPath = new URI(storeRoot + originState.getProp(CONFIG_STORE_REPLICATION_ROOT)).getPath();
        replicationTag = storeRoot + originState.getProp(CONFIG_STORE_REPLICATION_TAG);
      }
      else{
        throw new RuntimeException(String.format("Can not find required config values for %s, %s, %s", CONFIG_STORE_ROOT, CONFIG_STORE_REPLICATION_ROOT, CONFIG_STORE_REPLICATION_TAG));
      }

      // get all the URIs which imports {@link #replicationTag}
      Collection<URI> importedBy = configClient.getImportedBy(new URI(replicationTag), true);

      List<URI> replicationDataset = new ArrayList<URI>(importedBy);
      Comparator<URI> pathLengthComparator = new Comparator<URI>() {
        public int compare(URI c1, URI c2) {
          return c1.getPath().length() - c2.getPath().length(); 
        }
      };

      // sort the URI based on the path length to make sure the parent path appear before children
      Collections.sort(replicationDataset, pathLengthComparator);
      
      Set<URI> leafDataset = new HashSet<URI>();
      
      for(URI u: replicationDataset){
        // due to the sort, at most one element need to be removed 
        URI needToRemove = null;
        
        // valid dataset URI
        if(u.getPath().startsWith(replicationRootPath)){
          for(URI leaf: leafDataset){
            if(PathUtils.isAncestor(new Path(leaf.getPath()), new Path(u.getPath()))){
              needToRemove = leaf;
              break;
            }
          }
          
          if(needToRemove!=null){
            leafDataset.remove(needToRemove);
          }
          leafDataset.add(u);
        }
      }
      
      List<Config> result =new ArrayList<Config>();
      for(URI u: leafDataset){
        result.add(configClient.getConfig(u));
      }
      
      return result;
    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      log.error("Caught error " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  private boolean generateWorkunitsPerReplicationDataset( SourceState originState,
      final ConcurrentBoundedWorkUnitList workUnitList, final long minWorkUnitWeight, ReplicationConfiguration rc) throws IOException{
    SourceState stateCp = new SourceState();
    stateCp.addAll(originState);
    ReplicationMetaData md = rc.getMetaData();
    log.info("metadata : " + md);

    ReplicationSource source = rc.getSource();
    log.info("source : " + source);

    List<ReplicationReplica> replicas = rc.getReplicas();
    for(ReplicationReplica r: replicas){
      log.info("replica: " + r);
    }

    DataFlowTopology topology = rc.getTopology();
    for(DataFlowTopology.CopyRoute route: topology.getRoutes()){
      log.info("route: " + route);
    }

    DataFlowTopology.CopyRoute route = this.getCopyRoute(topology);
    log.info("route is " + route);

    // TODO, for now only pick the first copyFrom
    if(source.getReplicationLocation().getType() == ReplicationType.HDFS){
      HdfsReplicationLocation hdfsCopyTo = (HdfsReplicationLocation)(route.getCopyDestination().getReplicationLocation());
      HdfsReplicationLocation hdfsCopyFrom = (HdfsReplicationLocation)(route.getCopyFrom().get(0).getReplicationLocation());

      stateCp.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, hdfsCopyFrom.getClustername()); 
      stateCp.setProp(ConfigurableGlobDatasetFinder.DATASET_FINDER_PATTERN_KEY, hdfsCopyFrom.getPath());
      log.info("source dir is " + hdfsCopyFrom.getPath());


      stateCp.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, hdfsCopyTo.getClustername());
      stateCp.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, hdfsCopyTo.getPath());

      log.info("target dir is " + hdfsCopyTo.getPath());

      FileSystem sourceFs = super.getSourceFileSystem(stateCp);
      FileSystem targetFs = super.getTargetFileSystem(stateCp);

      DatasetsFinder<CopyableDatasetBase> datasetFinder =
          DatasetUtils.instantiateDatasetFinder(stateCp.getProperties(), sourceFs, DEFAULT_DATASET_PROFILE_CLASS_KEY,
              new EventSubmitter.Builder(this.metricContext, CopyConfiguration.COPY_PREFIX).build(), stateCp);

      return generateWorkunitsPerSourceTargetFs(stateCp, sourceFs, targetFs, datasetFinder, workUnitList,
          minWorkUnitWeight);
    }

    return false;
  }

}
