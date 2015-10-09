package gobblin.tunnel;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A convenient, trackable, easy-to-cleanup wrapper around threads.
 *
 * @author kkandekar@linkedin.com
 */
abstract class EasyThread extends Thread {
  static Set<EasyThread> ALL_THREADS = Collections.synchronizedSet(new HashSet<EasyThread>());

  EasyThread startThread() {
    setDaemon(true);
    start();
    ALL_THREADS.add(this);
    return this;
  }

  @Override
  public void run() {
    try {
      runQuietly();
    } catch (Exception ignored) {
    }
  }

  abstract void runQuietly() throws Exception;
}
