package org.apache.gobblin.service.modules.orchestration;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

// TODO: move to the right package & decide whether this wrapper is needed
public class DagActionReminderScheduler {

    public static void main(String[] args) throws SchedulerException {

      // Create a new scheduler
      Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

      // Start the scheduler
      scheduler.start();

    }

  }
}
