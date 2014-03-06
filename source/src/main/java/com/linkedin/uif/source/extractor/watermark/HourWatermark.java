package com.linkedin.uif.source.extractor.watermark;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public class HourWatermark implements Watermark {
    private String watermarkColumn;
    private String watermarkFormat;
    
	public HourWatermark(String watermarkColumn, String watermarkFormat) {
		this.watermarkColumn = watermarkColumn;
		this.watermarkFormat = watermarkFormat;
	}

	@Override
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator) {
		return extractor.getHourPredicateCondition(this.watermarkColumn, watermarkValue, this.watermarkFormat, operator);
	}

}
