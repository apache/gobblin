package gobblin.runtime.std;

import java.net.URI;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;


/** Unit tests for {@link InMemoryJobCatalog} */
public class TestInMemoryJobCatalog {

  @Test
  public void testCallbacks()
      throws Exception {
    InMemoryJobCatalog cat = new InMemoryJobCatalog();

    JobCatalogListener l = Mockito.mock(JobCatalogListener.class);

    JobSpec js1_1 = JobSpec.builer("test:job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builer("test:job1").withVersion("2").build();
    JobSpec js1_3 = JobSpec.builer("test:job1").withVersion("3").build();
    JobSpec js2 = JobSpec.builer("test:job2").withVersion("1").build();

    cat.put(js1_1);
    cat.addListener(l);
    cat.put(js1_2);
    cat.put(js2);
    cat.put(js1_3);
    cat.remove(js2.getUri());
    cat.remove(new URI("test:dummy_job"));
    cat.removeListener(l);
    cat.remove(js1_3.getUri());

    Mockito.verify(l).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l).onAddJob(Mockito.eq(js2));
    Mockito.verify(l).onUpdateJob(Mockito.eq(js1_2), Mockito.eq(js1_3));
    Mockito.verify(l).onDeleteJob(Mockito.eq(js2));

    Mockito.verifyNoMoreInteractions(l);
  }
}
