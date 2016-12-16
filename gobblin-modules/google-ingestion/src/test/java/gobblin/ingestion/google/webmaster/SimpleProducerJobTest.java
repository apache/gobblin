package gobblin.ingestion.google.webmaster;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class SimpleProducerJobTest {
  @Test
  public void testNotDivisibleJobs() {
    ProducerJob job1 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-22");
    Assert.assertTrue(job1.partitionJobs().isEmpty());

    ProducerJob job2 = new SimpleProducerJob("p1", "2016-11-23", "2016-11-22");
    Assert.assertTrue(job2.partitionJobs().isEmpty());
  }

  @Test
  public void testDivisibleJobs1() {
    ProducerJob job3 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-23");
    List<? extends ProducerJob> divides = job3.partitionJobs();
    Assert.assertEquals(divides.size(), 2);
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-22", "2016-11-22"), divides.get(0));
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-23", "2016-11-23"), divides.get(1));
  }

  @Test
  public void testDivisibleJobs2() {
    ProducerJob job3 = new SimpleProducerJob("p1", "2016-11-22", "2016-11-24");
    List<? extends ProducerJob> divides = job3.partitionJobs();
    Assert.assertEquals(divides.size(), 2);
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-22", "2016-11-23"), divides.get(0));
    Assert.assertEquals(new SimpleProducerJob("p1", "2016-11-24", "2016-11-24"), divides.get(1));
  }
}
