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

package org.apache.gobblin.data.management.copy.replication;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gobblin.dataset.Dataset;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import lombok.extern.slf4j.Slf4j;


/**
 * Based on single dataset configuration in {@link Config} format, in Pull mode replication, there could be multiple
 * {@link ConfigBasedDataset} generated. For example, if two replicas exists on the same copyTo cluster,
 * say replica1 and replica2, then there will be 2 {@link ConfigBasedDataset} generated, one for replication data from
 * copyFrom {@link EndPoint} to replica1, the other from copyFrom {@link EndPoint} to replica2
 *
 * This class will be responsible to generate those {@link ConfigBasedDataset}s
 *
 * @author mitu
 */

@Slf4j
public class ConfigBasedMultiDatasets {

  private final Properties props;
  private final List<Dataset> datasets = new ArrayList<>();
  private Optional<List<Pattern>> blacklist = Optional.of(new ArrayList<>());


  /**
   * if push mode is set in property, only replicate data when
   * 1. Push mode is set in Config store
   * 2. CopyTo cluster in sync with property with 'writer.fs.uri'
   */
  public static final String REPLICATION_PUSH_MODE = CopyConfiguration.COPY_PREFIX + ".replicationPushMode";

  // Dummy constructor, return empty datasets.
  public ConfigBasedMultiDatasets(){
    this.props = new Properties();
  }

  public ConfigBasedMultiDatasets (Config c, Properties props,
      Optional<List<String>> blacklistPatterns){
    this.props = props;
    blacklist = patternListInitHelper(blacklistPatterns);

    try {
      FileSystem executionCluster = FileSystem.get(new Configuration());
      URI executionClusterURI = executionCluster.getUri();

      ReplicationConfiguration rc = ReplicationConfiguration.buildFromConfig(c);

      // push mode
      if(this.props.containsKey(REPLICATION_PUSH_MODE) && Boolean.parseBoolean(this.props.getProperty(REPLICATION_PUSH_MODE))){
        generateDatasetInPushMode(rc, executionClusterURI);
      }
      // default pull mode
      else{
        generateDatasetInPullMode(rc, executionClusterURI);
      }
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      log.error("Can not create Replication Configuration from raw config "
          + c.root().render(ConfigRenderOptions.defaults().setComments(false).setOriginComments(false)), e);
    } catch (IOException ioe) {
      log.error("Can not decide current execution cluster ", ioe);

    }
  }

  private Optional<List<Pattern>> patternListInitHelper(Optional<List<String>> patterns){
    if (patterns.isPresent() && patterns.get().size() >= 1) {
      List<Pattern> tmpPatterns = new ArrayList<>();
      for (String pattern : patterns.get()){
        tmpPatterns.add(Pattern.compile(pattern));
      }
      return Optional.of(tmpPatterns);
    }
    else{
      return Optional.absent();
    }
  }

  private void generateDatasetInPushMode(ReplicationConfiguration rc, URI executionClusterURI){
    if(rc.getCopyMode()== ReplicationCopyMode.PULL){
      log.info("Skip process pull mode dataset with meta data{} as job level property specify push mode ", rc.getMetaData());
      return;
    }

    if (!this.props.containsKey(ConfigurationKeys.WRITER_FILE_SYSTEM_URI)){
      return;
    }
    String pushModeTargetCluster = this.props.getProperty(ConfigurationKeys.WRITER_FILE_SYSTEM_URI);

    // PUSH mode
    CopyRouteGenerator cpGen = rc.getCopyRouteGenerator();
    List<EndPoint> replicas = rc.getReplicas();
    List<EndPoint> pushCandidates = new ArrayList<EndPoint>(replicas);
    pushCandidates.add(rc.getSource());

    for(EndPoint pushFrom: pushCandidates){
      if(needGenerateCopyEntity(pushFrom, executionClusterURI)){
        Optional<List<CopyRoute>> copyRoutes = cpGen.getPushRoutes(rc, pushFrom);
        if(!copyRoutes.isPresent()) {
          log.warn("In Push mode, did not found any copyRoute for dataset with meta data {}", rc.getMetaData());
          continue;
        }

        /**
         * For-Loop responsibility:
         * For each of the {@link CopyRoute}, generate a {@link ConfigBasedDataset}.
         */
        for(CopyRoute cr: copyRoutes.get()){
          if(cr.getCopyTo() instanceof HadoopFsEndPoint){

            HadoopFsEndPoint ep = (HadoopFsEndPoint)cr.getCopyTo();
            if(ep.getFsURI().toString().equals(pushModeTargetCluster)){

              // For a candidate dataset, iterate thru. all available blacklist patterns.
              ConfigBasedDataset configBasedDataset = new ConfigBasedDataset(rc, this.props, cr);
              if (blacklistFilteringHelper(configBasedDataset, this.blacklist)){
                this.datasets.add(configBasedDataset);
              }
              else{
                log.info("Dataset" + configBasedDataset.datasetURN() + " has been filtered out because of blacklist pattern:"
                    + this.blacklist.get().toString());
              }
            }
          }
        }// inner for loops ends
      }
    }// outer for loop ends
  }

  private void generateDatasetInPullMode(ReplicationConfiguration rc, URI executionClusterURI){
    if(rc.getCopyMode()== ReplicationCopyMode.PUSH){
      log.info("Skip process push mode dataset with meta data{} as job level property specify pull mode ", rc.getMetaData());
      return;
    }

    // PULL mode
    CopyRouteGenerator cpGen = rc.getCopyRouteGenerator();
    /**
     * Replicas comes from the 'List' which will normally be set in gobblin.replicas
     * Basically this is all possible destination for this replication job.
     */
    List<EndPoint> replicas = rc.getReplicas();
    for(EndPoint replica: replicas){
      // Only pull the data from current execution cluster
      if(needGenerateCopyEntity(replica, executionClusterURI)){
        /*
        * CopyRoute represent a coypable Dataset to execute, e.g. if you specify source:[war, holdem],
        * there could be two {@link #ConfigBasedDataset} generated.
        */
        Optional<CopyRoute> copyRoute = cpGen.getPullRoute(rc, replica);
        if(copyRoute.isPresent()){
          ConfigBasedDataset configBasedDataset = new ConfigBasedDataset(rc, this.props, copyRoute.get());
          if (blacklistFilteringHelper(configBasedDataset, this.blacklist)){
            this.datasets.add(configBasedDataset);
          }
          else{
            log.info("Dataset" + configBasedDataset.datasetURN() + " has been filtered out because of blacklist pattern:"
                + this.blacklist.get().toString());
          }
        }
      }
    }
  }

  @VisibleForTesting
  /**
   * Return false if the target configBasedDataset should be kept in the blacklist.
   */
  public boolean blacklistFilteringHelper(ConfigBasedDataset configBasedDataset, Optional<List<Pattern>> patternList){
    String datasetURN = configBasedDataset.datasetURN();
    if (patternList.isPresent()) {
      for(Pattern pattern: patternList.get()) {
        if (pattern.matcher(datasetURN).find()){
          return false;
        }
      }
      // If the dataset get thru. all blacklist check, accept it.
      return true;
    }
    // If blacklist not specified, automatically accept the dataset.
    else {
      return true;
    }
  }

  public List<Dataset> getConfigBasedDatasetList(){
    return this.datasets;
  }

  private boolean needGenerateCopyEntity(EndPoint e, URI executionClusterURI){
    if(!(e instanceof HadoopFsEndPoint)){
      return false;
    }

    HadoopFsEndPoint ep = (HadoopFsEndPoint)e;
    return ep.getFsURI().equals(executionClusterURI);
  }
}
