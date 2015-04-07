package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;

import org.apache.avro.generic.GenericRecord;

public class Test {
  public static final String REGISTRY_URL = "http://eat1-schema-registry-vip-1.corp.linkedin.com:12250/schemaRegistry/schemas";
  public static void main(String[] args) throws KafkaOffsetRetrievalFailureException, SchemaNotFoundException, DataRecordException, IOException {

    WorkUnitState state = new WorkUnitState();
    int partitionId = 38;
    String topicName = "PageViewEvent";
    state.setProp("kafka.brokers", "eat1-kafka-vip-5.corp.linkedin.com:10251");
    state.setProp("partition.id", partitionId);
    state.setProp("topic.name", topicName);
    KafkaWrapper wrapper = KafkaWrapper.create(state);
    Set<String> whitelist = new HashSet<String>();
    whitelist.add(topicName);
    List<KafkaTopic> topics = wrapper.getFilteredTopics(new HashSet<String>(), whitelist);
    for (KafkaTopic topic : topics) {
      System.out.println(topic.getName());
      for (KafkaPartition partition : topic.getPartitions()) {
        //System.out.println(partition.getId());
        if (partition.getId() == partitionId) {
          System.out.println("getting offsets for " + partition.getId());
          long earliest = wrapper.getEarliestOffset(partition);
          long latest = wrapper.getLatestOffset(partition);
          long start = (earliest + latest) / 2;
          System.out.println("earliest offset: " + wrapper.getEarliestOffset(partition));
          System.out.println("latest offset: " + wrapper.getLatestOffset(partition));
          state.setProp("leader.id", partition.getLeader().getId());
          state.setProp("leader.host", partition.getLeader().getHost());
          state.setProp("leader.port", partition.getLeader().getPort());
          System.out.println(String.format("leader: id = %d, host = %s, port = %d", partition.getLeader().getId(), partition.getLeader().getHost(), partition.getLeader().getPort()));
          state.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, start);
          state.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, start + 10);
          state.setProp(KafkaExtractor.KAFKA_SCHEMA_REGISTRY_URL, REGISTRY_URL);
          KafkaAvroExtractor extractor = new KafkaAvroExtractor(state);
          System.out.println("schema: " + extractor.getSchema());
          GenericRecord record = null;
          while (true) {
            record = extractor.readRecord(null);
            if (record == null) break;
            System.out.println("record: " + record);
          }
        }
      }

    }
  }
}
