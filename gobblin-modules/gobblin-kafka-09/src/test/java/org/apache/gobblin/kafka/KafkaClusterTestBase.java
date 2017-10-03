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

package org.apache.gobblin.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

import org.apache.gobblin.test.TestUtils;

public class KafkaClusterTestBase extends KafkaTestBase {

	int clusterCount; 
	EmbeddedZookeeper _zkServer;
	String _zkConnectString;
	ZkClient _zkClient;
	List<KafkaServer> kafkaBrokerList = new ArrayList<KafkaServer>();
	List<Integer> kafkaBrokerPortList = new ArrayList<Integer>();
	
	public KafkaClusterTestBase(int clusterCount) throws InterruptedException, RuntimeException {
		super();
		this.clusterCount = clusterCount;
	}

	public void startCluster() {
		// Start Zookeeper.
		_zkServer = new EmbeddedZookeeper();
		_zkConnectString = "127.0.0.1:"+_zkServer.port();
		_zkClient = new ZkClient(_zkConnectString, 30000, 30000, ZKStringSerializer$.MODULE$);
		// Start Kafka Cluster.
	    for(int i=0;i<clusterCount;i++) {
	    	KafkaServer _kafkaServer = createKafkaServer(i,_zkConnectString);
	    	kafkaBrokerList.add(_kafkaServer);
	    }
	}

	public void stopCluster() {
		Iterator<KafkaServer> iter = kafkaBrokerList.iterator();
		while(iter.hasNext()){
			KafkaServer server = iter.next();
			try {
				server.shutdown(); 
			} catch (RuntimeException e) {
				// Simply Ignore.
			}
		}
	}

	public int getZookeeperPort() {
		return _zkServer.port();
	}

	public List<KafkaServer> getBrokerList() {
		return kafkaBrokerList;
	}

	public List<Integer> getKafkaBrokerPortList() {
		return kafkaBrokerPortList;
	}

	
	public int getClusterCount() {
		return kafkaBrokerList.size();
	}

	private KafkaServer createKafkaServer(int brokerId,String _zkConnectString){
		
		int _brokerId = brokerId;
		int _kafkaServerPort = TestUtils.findFreePort();	
		Properties props = kafka.utils.TestUtils.createBrokerConfig(
          _brokerId,
          _zkConnectString,
          kafka.utils.TestUtils.createBrokerConfig$default$3(),
          kafka.utils.TestUtils.createBrokerConfig$default$4(),
          _kafkaServerPort,
          kafka.utils.TestUtils.createBrokerConfig$default$6(),
          kafka.utils.TestUtils.createBrokerConfig$default$7(),
          kafka.utils.TestUtils.createBrokerConfig$default$8(),
          kafka.utils.TestUtils.createBrokerConfig$default$9(),
          kafka.utils.TestUtils.createBrokerConfig$default$10(),
          kafka.utils.TestUtils.createBrokerConfig$default$11(),
          kafka.utils.TestUtils.createBrokerConfig$default$12(),
          kafka.utils.TestUtils.createBrokerConfig$default$13(),
          kafka.utils.TestUtils.createBrokerConfig$default$14()
          );
      KafkaConfig config = new KafkaConfig(props);
      Time mock = new MockTime();
      KafkaServer _kafkaServer = kafka.utils.TestUtils.createServer(config, mock);
      kafkaBrokerPortList.add(_kafkaServerPort);
      return _kafkaServer;
	}

	public String getBootServersList() {
		String bootServerString = "";
		Iterator<Integer> ports =  kafkaBrokerPortList.iterator();
		while(ports.hasNext()){
			Integer port = ports.next();
			bootServerString = bootServerString+"localhost:"+port+",";
		}
		bootServerString = bootServerString.substring(0,bootServerString.length()-1);
		return bootServerString;
	}
}
