package gobblin.writer.initializer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.JdbcWriterBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

public class WriterInitializerFactory {
  private static final NoopWriterInitializer NOOP = new NoopWriterInitializer();

  /**
   * Provides WriterInitializer based on the writer. Mostly writer is decided by the Writer builder (and destination) that user passes.
   * If there's more than one branch, it will instantiate same number of WriterInitializer instance as number of branches and combine it into MultiWriterInitializer.
   *
   * @param state
   * @return WriterInitializer
   */
  public static WriterInitializer newInstace(State state, Collection<WorkUnit> workUnits) {
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    if (branches == 1) {
      return newSingleInstance(state, workUnits, branches, 0);
    }

    List<WriterInitializer> wis = Lists.newArrayList();
    for (int branchId = 0; branchId < branches; branchId++) {
      wis.add(newSingleInstance(state, workUnits, branches, branchId));
    }
    return NOOP;
  }

  private static WriterInitializer newSingleInstance(State state, Collection<WorkUnit> workUnits, int branches, int branchId) {
    Objects.requireNonNull(state);

    String writerBuilderKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUILDER_CLASS, branches, branchId);
    String writerBuilderClass = state.getProp(writerBuilderKey, ConfigurationKeys.DEFAULT_WRITER_BUILDER_CLASS);

    if(JdbcWriterBuilder.class.getName().equals(writerBuilderClass)) {
      return new JdbcWriterInitializer(state, workUnits, branches, branchId);
    }
    return NOOP;
  }

  public static void main(String[] args) {
    System.out.println(JdbcWriterInitializer.class.getName());
  }
}
