package gobblin.source.extractor.extract.google.webmaster;

import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class ProducerJobTest {
  @Test
  public void testJobsList() {
    ProducerJob job1 = new ProducerJob("p1", "start-date-1", "end-date-1", GoogleWebmasterFilter.FilterOperator.EQUALS);
    ProducerJob job2 = new ProducerJob("p2", "start-date-2", "end-date-2", GoogleWebmasterFilter.FilterOperator.EQUALS);
    ArrayList<ProducerJob> jobs = new ArrayList<>();
    jobs.add(job1);
    jobs.add(job2);
    String json = ProducerJob.serialize(jobs);
    //System.out.println(json);
    List<ProducerJob> deserialized = ProducerJob.deserialize(json);
    Assert.assertEquals(job1, deserialized.get(0));
    Assert.assertEquals(job2, deserialized.get(1));
  }

  @Test
  public void testEmptyList() {
    ArrayList<ProducerJob> jobs = new ArrayList<>();
    String json = ProducerJob.serialize(jobs);
    List<ProducerJob> deserialized = ProducerJob.deserialize(json);
    Assert.assertTrue(deserialized.isEmpty());
  }

  @Test
  public void testEmptyString() {
    List<ProducerJob> deserialized = ProducerJob.deserialize("");
    Assert.assertTrue(deserialized.isEmpty());
  }
}
