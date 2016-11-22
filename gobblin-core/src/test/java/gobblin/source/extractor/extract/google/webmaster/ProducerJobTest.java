package gobblin.source.extractor.extract.google.webmaster;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class ProducerJobTest {
  @Test
  public void testSerializationWithJobsList() {
    ProducerJob job1 = new ProducerJob("p1", "2016-11-22", "2016-11-22", GoogleWebmasterFilter.FilterOperator.EQUALS);
    ProducerJob job2 = new ProducerJob("p2", "2016-11-23", "2016-11-23", GoogleWebmasterFilter.FilterOperator.EQUALS);
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
  public void testSerializationWithEmptyList() {
    ArrayList<ProducerJob> jobs = new ArrayList<>();
    String json = ProducerJob.serialize(jobs);
    List<ProducerJob> deserialized = ProducerJob.deserialize(json);
    Assert.assertTrue(deserialized.isEmpty());
  }

  @Test
  public void testSerializationWithEmptyString() {
    List<ProducerJob> deserialized = ProducerJob.deserialize("");
    Assert.assertTrue(deserialized.isEmpty());
  }

  @Test
  public void testNotDivisibleJobs() {
    ProducerJob job1 = new ProducerJob("p1", "2016-11-22", "2016-11-22", GoogleWebmasterFilter.FilterOperator.EQUALS);
    Assert.assertNull(job1.divideJob());

    ProducerJob job2 = new ProducerJob("p1", "2016-11-23", "2016-11-22", GoogleWebmasterFilter.FilterOperator.EQUALS);
    Assert.assertNull(job2.divideJob());
  }

  @Test
  public void testDivisibleJobs1() {
    ProducerJob job3 = new ProducerJob("p1", "2016-11-22", "2016-11-23", GoogleWebmasterFilter.FilterOperator.EQUALS);
    Pair<ProducerJob, ProducerJob> divides = job3.divideJob();
    Assert.assertEquals(new ProducerJob("p1", "2016-11-22", "2016-11-22", GoogleWebmasterFilter.FilterOperator.EQUALS),
        divides.getLeft());
    Assert.assertEquals(new ProducerJob("p1", "2016-11-23", "2016-11-23", GoogleWebmasterFilter.FilterOperator.EQUALS),
        divides.getRight());
  }

  @Test
  public void testDivisibleJobs2() {
    ProducerJob job3 = new ProducerJob("p1", "2016-11-22", "2016-11-24", GoogleWebmasterFilter.FilterOperator.EQUALS);
    Pair<ProducerJob, ProducerJob> divides = job3.divideJob();
    Assert.assertEquals(new ProducerJob("p1", "2016-11-22", "2016-11-23", GoogleWebmasterFilter.FilterOperator.EQUALS),
        divides.getLeft());
    Assert.assertEquals(new ProducerJob("p1", "2016-11-24", "2016-11-24", GoogleWebmasterFilter.FilterOperator.EQUALS),
        divides.getRight());
  }
}
