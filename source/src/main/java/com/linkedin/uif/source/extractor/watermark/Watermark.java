package com.linkedin.uif.source.extractor.watermark;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public interface Watermark {
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator);
}
