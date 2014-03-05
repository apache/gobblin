package com.linkedin.uif.source.extractor.watermark;

import com.linkedin.uif.source.extractor.extract.BaseExtractor;

public class SimpleWatermark implements Watermark {
    private String watermarkColumn;
    private String watermarkFormat;
    
	public SimpleWatermark(String watermarkColumn, String watermarkFormat) {
		this.watermarkColumn = watermarkColumn;
		this.watermarkFormat = watermarkFormat;
	}

	@Override
	public String getWatermarkCondition(BaseExtractor extractor, long watermarkValue, String operator) {
		return this.watermarkColumn + operator +  watermarkValue;
	}
}
