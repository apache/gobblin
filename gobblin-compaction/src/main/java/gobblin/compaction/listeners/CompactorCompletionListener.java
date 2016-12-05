package gobblin.compaction.listeners;

import gobblin.compaction.mapreduce.MRCompactor;


public interface CompactorCompletionListener {
  public void onCompactionCompletion(MRCompactor compactor);
}
