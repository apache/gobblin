package org.apache.gobblin.runtime.troubleshooter;

import java.util.List;

public class ErrorCategories {
  private final List<String> names;

  public ErrorCategories(List<String> names) {
    this.names = names;
  }

  public List<String> getCategories() {
    return this.names;
  }

  public int priority(String s) {
    return names.indexOf(s);
  }
}

