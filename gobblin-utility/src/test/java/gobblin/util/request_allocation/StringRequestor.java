package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class StringRequestor implements PushDownRequestor<StringRequest> {

  public StringRequestor(String name, String... strings) {
    this(name, Lists.newArrayList(strings));
  }

  @Getter
  private final String name;
  private final List<String> strings;

  @Override
  public Iterator<StringRequest> iterator() {
    return Iterators.transform(this.strings.iterator(), new Function<String, StringRequest>() {
      @Override
      public StringRequest apply(String input) {
        return new StringRequest(StringRequestor.this, input);
      }
    });
  }

  @Override
  public Iterator<StringRequest> getRequests(Comparator<StringRequest> prioritizer)
      throws IOException {
    List<StringRequest> requests = Lists.newArrayList(Iterators.transform(this.strings.iterator(), new Function<String, StringRequest>() {
      @Override
      public StringRequest apply(String input) {
        return new StringRequest(StringRequestor.this, input);
      }
    }));
    Collections.sort(requests, prioritizer);
    return requests.iterator();
  }
}
