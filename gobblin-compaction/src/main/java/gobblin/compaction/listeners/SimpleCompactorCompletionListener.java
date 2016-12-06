package gobblin.compaction.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.annotation.Alias;
import gobblin.compaction.conditions.RecompactionCombineCondition;
import gobblin.compaction.mapreduce.MRCompactor;


public class SimpleCompactorCompletionListener implements CompactorCompletionListener {
  private static final Logger logger = LoggerFactory.getLogger (RecompactionCombineCondition.class);

  public void onCompactionCompletion (MRCompactor compactor) {
    logger.info(String.format("Compaction (started on : %s) is finished", compactor.getInitilizeTime()));
  }

  @Alias("SimpleCompactorCompletionHook")
  public static class Factory implements CompactorCompletionListenerFactory {
    @Override public CompactorCompletionListener createCompactorCompactionListener () {
      return new SimpleCompactorCompletionListener ();
    }
  }
}
