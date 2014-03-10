package com.linkedin.uif.source.extractor.watermark;

import java.util.HashMap;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public interface Watermark {
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator);
	public HashMap<Long, Long> getIntervals(long lowWatermarkValue, long highWatermarkValue, int partitionInterval, int maxIntervals);
	public int getDeltaNumForNextWatermark();
}
