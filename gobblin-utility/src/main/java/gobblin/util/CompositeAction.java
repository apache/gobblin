package gobblin.util;

public class CompositeAction implements Action {
  private final Iterable<Action> actions;

  public CompositeAction(Iterable<Action> actions) {
    this.actions = actions;
  }

  @Override
  public void apply() throws Exception {
    for (Action action : actions) {
      action.apply();
    }
  }
}
