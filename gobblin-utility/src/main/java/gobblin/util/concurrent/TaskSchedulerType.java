package gobblin.util.concurrent;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Enums;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * The types of supported {@link TaskScheduler}s.
 *
 * @author joelbaranick
 */
@AllArgsConstructor
public enum TaskSchedulerType {
  /**
   * A {@link TaskScheduler} based on a {@link java.util.concurrent.ScheduledExecutorService}. This
   * is the default {@link TaskScheduler}.
   */
  SCHEDULEDEXECUTORSERVICE(ScheduledExecutorServiceTaskScheduler.class),

  /**
   * A {@link TaskScheduler} based on a {@link org.jboss.netty.util.HashedWheelTimer}.
   */
  HASHEDWHEELTIMER(HashedWheelTimerTaskScheduler.class);

  @Getter
  private final Class<? extends TaskScheduler> taskSchedulerClass;

  /**
   * Return the {@link TaskSchedulerType} with the specified name. If the specified name
   * does not map to a {@link TaskSchedulerType}, then {@link #SCHEDULEDEXECUTORSERVICE}
   * will be returned.
   *
   * @param name the name of the {@link TaskSchedulerType}
   * @return the specified {@link TaskSchedulerType} or {@link #SCHEDULEDEXECUTORSERVICE}
   */
  public static TaskSchedulerType parse(String name) {
    if (StringUtils.isEmpty(name)) {
      return SCHEDULEDEXECUTORSERVICE;
    }
    return Enums.getIfPresent(TaskSchedulerType.class, name.toUpperCase()).or(SCHEDULEDEXECUTORSERVICE);
  }
}
