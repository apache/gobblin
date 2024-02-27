package org.apache.gobblin.service.modules.orchestration;

import java.util.Iterator;

import org.apache.gobblin.service.modules.orchestration.task.DagTask;


public interface DagTaskStream extends Iterator<DagTask> {
  boolean hasNext();
  DagTask next();
}
