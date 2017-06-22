package gobblin.async;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.util.ExponentialBackoff;


@Test
public class AsyncDataDispatcherTest {
  /**
   * Test successful data dispatch with 2 writers. No exception
   */
  public void testSuccessfulDataDispatch()
      throws ExecutionException, InterruptedException {
    final TestAsyncDataDispatcher dispatcher = new TestAsyncDataDispatcher();
    // This should work when there is nothing to process
    dispatcher.waitForBufferEmpty();

    ExecutorService service = Executors.newFixedThreadPool(2);
    Writer writer1 = new Writer(dispatcher);
    Writer writer2 = new Writer(dispatcher);
    Future<?> future1 = service.submit(writer1);
    Future<?> future2 = service.submit(writer2);

    // Process 10 records
    ExponentialBackoff.awaitCondition().callable(new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return dispatcher.count == 10;
      }
    }).maxWait(1000L).await();

    writer1.shouldWaitForABufferEmpty = true;
    writer2.shouldWaitForABufferEmpty = true;
    dispatcher.count = 0;

    // Process another 10 records
    ExponentialBackoff.awaitCondition().callable(new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return dispatcher.count == 10;
      }
    }).maxWait(1000L).await();

    writer1.shouldExit = true;
    writer2.shouldExit = true;

    try {
      future1.get();
      future2.get();
      service.shutdown();
      dispatcher.terminate();
    } catch (Exception e) {
      Assert.fail("Could not complete successful data dispatch");
    }

    Assert.assertTrue(dispatcher.isDispatchCalled);
    Assert.assertTrue(writer1.aBufferEmptyWaited);
    Assert.assertTrue(writer2.aBufferEmptyWaited);
  }

  /**
   * Test successful data dispatch with 2 writers. Normal exception
   */
  public void testSuccessfulDataDispatchWithNormalException()
      throws ExecutionException, InterruptedException {
    final TestAsyncDataDispatcher dispatcher = new TestAsyncDataDispatcher();
    ExecutorService service = Executors.newFixedThreadPool(2);
    Writer writer1 = new Writer(dispatcher);
    Writer writer2 = new Writer(dispatcher);
    Future<?> future1 = service.submit(writer1);
    Future<?> future2 = service.submit(writer2);

    dispatcher.status = DispatchStatus.ERROR;

    // Process 10 records
    ExponentialBackoff.awaitCondition().callable(new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return dispatcher.count == 10;
      }
    }).maxWait(1000L).await();

    writer1.shouldExit = true;
    writer2.shouldExit = true;

    // Everything should be fine
    try {
      future1.get();
      future2.get();
      service.shutdown();
      dispatcher.terminate();
    } catch (Exception e) {
      Assert.fail("Could not complete successful data dispatch");
    }

    Assert.assertTrue(dispatcher.isDispatchCalled);
  }

  /**
   * Test data dispatch with 2 writers. Fatal exception
   */
  public void testSuccessfulDataDispatchWithFatalException()
      throws ExecutionException, InterruptedException {
    final TestAsyncDataDispatcher dispatcher = new TestAsyncDataDispatcher();
    ExecutorService service = Executors.newFixedThreadPool(2);
    Writer writer1 = new Writer(dispatcher);
    Writer writer2 = new Writer(dispatcher);
    Future<?> future1 = service.submit(writer1);
    Future<?> future2 = service.submit(writer2);

    dispatcher.status = DispatchStatus.FATAL;

    // Process 10 records
    ExponentialBackoff.awaitCondition().callable(new Callable<Boolean>() {
      @Override
      public Boolean call()
          throws Exception {
        return dispatcher.count == 10;
      }
    }).maxWait(1000L).await();

    boolean hasAnException = false;
    try {
      dispatcher.put(new Object());
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException, "put should get an exception");

    hasAnException = false;
    try {
      dispatcher.waitForBufferEmpty();
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException, "waitForBufferEmpty should get an exception");

    hasAnException = false;
    try {
      // Everything should be fine
      future1.get();
    } catch (ExecutionException e) {
      hasAnException = true;
    } catch (InterruptedException e) {
      // Do nothing
    }
    Assert.assertTrue(hasAnException, "future1 should get an exception");

    hasAnException = false;
    try {
      // Everything should be fine
      future2.get();
    } catch (ExecutionException e) {
      hasAnException = true;
    } catch (InterruptedException e) {
      // Do nothing
    }
    Assert.assertTrue(hasAnException, "future2 should get an exception");

    hasAnException = false;
    try {
      service.shutdown();
      dispatcher.terminate();
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException, "terminating should get an exception");
    Assert.assertTrue(dispatcher.isDispatchCalled);
  }

  enum DispatchStatus {
    // Dispatch success
    OK,
    // Dispatch exception
    ERROR,
    // Fatal dispatch exception
    FATAL
  }

  class TestAsyncDataDispatcher extends AsyncDataDispatcher<Object> {
    volatile DispatchStatus status;
    volatile int count;
    boolean isDispatchCalled;

    TestAsyncDataDispatcher() {
      super(2);
      status = DispatchStatus.OK;
      isDispatchCalled = false;
      count = 0;
    }

    @Override
    protected void dispatch(Queue<Object> buffer)
        throws DispatchException {
      // Assert buffer must not be empty anytime dispatch is called
      Assert.assertTrue(buffer.size() > 0);
      isDispatchCalled = true;
      // Consume a record
      buffer.poll();
      count++;

      switch (status) {
        case OK:
          return;
        case ERROR:
          throw new DispatchException("error", false);
        case FATAL:
          throw new DispatchException("fatal");
      }
    }
  }

  class Writer implements Runnable {

    TestAsyncDataDispatcher dispather;
    boolean aBufferEmptyWaited;
    volatile boolean shouldWaitForABufferEmpty;
    volatile boolean shouldExit;

    Writer(TestAsyncDataDispatcher dispather) {
      this.dispather = dispather;
      shouldWaitForABufferEmpty = false;
      shouldExit = false;
      aBufferEmptyWaited = false;
    }

    @Override
    public void run() {
      while (!shouldExit) {
        dispather.put(new Object());
        if (shouldWaitForABufferEmpty) {
          dispather.waitForBufferEmpty();
          aBufferEmptyWaited = true;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // Do nothing
        }
      }
    }
  }
}
