package gobblin.source.extractor.extract.google.webmaster;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


public class ProducerJob {
  private static final GsonBuilder gsonBuilder = new GsonBuilder();
  private final String _page;
  private final String _startDate;
  private final String _endDate;
  private final GoogleWebmasterFilter.FilterOperator _operator;

  ProducerJob(String page, String startDate, String endDate, GoogleWebmasterFilter.FilterOperator operator) {
    _page = page;
    _startDate = startDate;
    _endDate = endDate;
    _operator = operator;
  }

  public static String serialize(Collection<ProducerJob> jobs) {
    Gson gson = gsonBuilder.create();
    return gson.toJson(jobs);
  }

  public static List<ProducerJob> deserialize(String jobs) {
    if (jobs == null || jobs.trim().isEmpty()) {
      jobs = "[]";
    }
    JsonArray jobsJson = new JsonParser().parse(jobs).getAsJsonArray();
    return new Gson().fromJson(jobsJson, new TypeToken<ArrayList<ProducerJob>>() {
    }.getType());
  }

  public String getPage() {
    return _page;
  }

  public String getStartDate() {
    return _startDate;
  }

  public String getEndDate() {
    return _endDate;
  }

  public GoogleWebmasterFilter.FilterOperator getOperator() {
    return _operator;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_page, _startDate, _endDate, _operator);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!ProducerJob.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    ProducerJob other = (ProducerJob) obj;
    return Objects.equals(_page, other._page) && Objects.equals(_startDate, other._startDate) && Objects.equals(
        _endDate, other._endDate) && Objects.equals(_operator, other._operator);
  }

  @Override
  public String toString() {
    return String.format("ProducerJob{_page='%s', _startDate='%s', _endDate='%s', _operator=%s}", _page, _startDate,
        _endDate, _operator);
  }
}
