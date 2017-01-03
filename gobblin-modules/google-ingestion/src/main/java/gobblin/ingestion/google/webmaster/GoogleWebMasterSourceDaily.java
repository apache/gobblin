package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


public class GoogleWebMasterSourceDaily extends GoogleWebMasterSource {

  @Override
  GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException {

    long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    return new GoogleWebmasterExtractor(state, lowWatermark, lowWatermark, columnPositionMap, requestedDimensions,
        requestedMetrics);
  }
}
