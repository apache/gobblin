package gobblin.runtime.std;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import gobblin.runtime.api.JobCatalogListener;
import gobblin.runtime.api.JobSpec;

/** Unit tests for {@link JobCatalogListenersList} */
public class TestJobCatalogListenersList {

  @Test
  public void testCalls() {
    JobCatalogListenersList ll = new JobCatalogListenersList();

    JobSpec js1_1 = JobSpec.builer("test:job1").build();
    JobSpec js1_2 = JobSpec.builer("test:job1").withVersion("2").build();
    JobSpec js2 = JobSpec.builer("test:job2").build();

    JobCatalogListener l1 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l1 failure")).when(l1).onDeleteJob(Mockito.eq(js2));

    JobCatalogListener l2 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l2 failure")).when(l2).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));

    JobCatalogListener l3 = Mockito.mock(JobCatalogListener.class);
    Mockito.doThrow(new RuntimeException("injected l3 failure")).when(l3).onAddJob(Mockito.eq(js2));

    ll.addListener(l1);
    ll.addListener(l2);
    ll.addListener(l3);

    ll.onAddJob(js1_1);
    ll.onAddJob(js2);
    ll.onUpdateJob(js1_1, js1_2);
    ll.onDeleteJob(js2);

    Mockito.verify(l1).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l1).onAddJob(Mockito.eq(js2));
    Mockito.verify(l1).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l1).onDeleteJob(Mockito.eq(js2));

    Mockito.verify(l2).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l2).onAddJob(Mockito.eq(js2));
    Mockito.verify(l2).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l2).onDeleteJob(Mockito.eq(js2));

    Mockito.verify(l3).onAddJob(Mockito.eq(js1_1));
    Mockito.verify(l3).onAddJob(Mockito.eq(js2));
    Mockito.verify(l3).onUpdateJob(Mockito.eq(js1_1), Mockito.eq(js1_2));
    Mockito.verify(l3).onDeleteJob(Mockito.eq(js2));


    Mockito.verifyNoMoreInteractions(l1, l2, l3);
  }

}
