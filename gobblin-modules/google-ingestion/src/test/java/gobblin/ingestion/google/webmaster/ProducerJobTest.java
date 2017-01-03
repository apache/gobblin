package gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class ProducerJobTest {
  @Test
  public void testSerializationWithJobsList() {
    ProducerJob job1 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-22");
    ProducerJob job2 = new SimpleProducerJob("p2", "2016-11-23", "2016-11-23");
    ArrayList<ProducerJob> jobs = new ArrayList<>();
    jobs.add(job1);
    jobs.add(job2);
    String json = ProducerJob.serialize(jobs);
    //System.out.println(json);
    List<ProducerJob> deserialized = SimpleProducerJob.deserialize(json);
    Assert.assertEquals(job1, deserialized.get(0));
    Assert.assertEquals(job2, deserialized.get(1));
  }

  @Test
  public void testSerializationWithEmptyList() {
    ArrayList<ProducerJob> jobs = new ArrayList<>();
    String json = ProducerJob.serialize(jobs);
    List<ProducerJob> deserialized = SimpleProducerJob.deserialize(json);
    Assert.assertTrue(deserialized.isEmpty());
  }

  @Test
  public void testSerializationWithEmptyString() {
    List<ProducerJob> deserialized = SimpleProducerJob.deserialize("");
    Assert.assertTrue(deserialized.isEmpty());
  }
}
