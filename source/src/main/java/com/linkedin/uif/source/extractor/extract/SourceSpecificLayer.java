package com.linkedin.uif.source.extractor.extract;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.linkedin.uif.source.extractor.config.Predicate;
import com.linkedin.uif.source.extractor.exception.DataRecordException;
import com.linkedin.uif.source.extractor.exception.HighWatermarkException;
import com.linkedin.uif.source.extractor.exception.RecordCountException;
import com.linkedin.uif.source.extractor.exception.SchemaException;
import com.linkedin.uif.source.extractor.resultset.RecordSet;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An interface for source extractors
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public interface SourceSpecificLayer<D,S> {
    /**
     * Metadata to extract raw schema(like url, query)
     *
     * @param source schema name
     * @param source entity name
     * @return metadata for schema
     * @throws SchemaException if there is anything wrong in building metadata for schema extraction
     */
	public String getSchemaMetadata(String schema, String entity) throws SchemaException;
	
    /**
     * Raw schema from the response
     *
     * @return JsonArray of schema
     * @throws SchemaException if there is anything wrong in getting raw schema
     */
	public JsonArray getSchema(String response) throws SchemaException, IOException;
	
    /**
     * Metadata for high watermark(like url, query)
     *
     * @param source schema name
     * @param source entity name
     * @param water mark column
     * @param lis of all predicates that needs to be applied
     * @return metadata for high watermark
     * @throws HighWatermarkException if there is anything wrong in building metadata to get high watermark
     */
	public String getHighWatermarkMetadata(String schema, String entity, String watermarkColumn, List<Predicate> predicateList) throws HighWatermarkException;
	
    /**
     * High watermark from the response
     *
     * @param source schema name
     * @param source entity name
     * @param water mark column
     * @param lis of all predicates that needs to be applied
     * @return high water mark from source
     * @throws HighWatermarkException if there is anything wrong in building metadata to get high watermark
     */
	public long getHighWatermark(String response, String watermarkColumn, String predicateColumnFormat) throws HighWatermarkException;
	
    /**
     * Metadata for record count(like url, query)
     *
     * @param source schema name
     * @param source entity name
     * @param work unit: properties
     * @param lis of all predicates that needs to be applied
     * @return metadata for record count
     * @throws RecordCountException if there is anything wrong in building metadata for record counts
     */
	public String getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws RecordCountException;
	
    /**
     * Record count from the response
     *
     * @return record count
     * @throws RecordCountException if there is anything wrong in getting record count
     */
	public long getCount(String response) throws RecordCountException;
	
    /**
     * Metadata for data records(like url, query)
     *
     * @param source schema name
     * @param source entity name
     * @param work unit: properties
     * @param lis of all predicates that needs to be applied
     * @return metadata for data records
     * @throws DataRecordException if there is anything wrong in building metadata for data records
     */
	public String getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) throws DataRecordException;
	
    /**
     * Set of data records from the response
     *
     * @return RecordSet of type D
     * @throws DataRecordException if there is anything wrong in getting data records
     */
	public RecordSet<D> getData(String response) throws DataRecordException, IOException;
	
    /**
     * Data type of source
     *
     * @return Map of source and target data types
     */
	public Map<String, String> getDataTypeMap();
}
