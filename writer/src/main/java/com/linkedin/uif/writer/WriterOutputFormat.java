package com.linkedin.uif.writer;

/**
 * An enumeration of writer output formats.
 *
 * @author ynli
 */
public enum WriterOutputFormat {
    AVRO ("avro"),
    PARQUET ("parquet"),
    CSV ("csv");
    
    /**
     * Extension specifies the file name extension
     */
    private final String extension;
    
    WriterOutputFormat(String extension) {
        this.extension = extension;
    }
    
    /**
     * Returns the file name extension for the enum type
     * @return a string representation of the file name extension
     */
    public String getExtension() {
        return this.extension;
    }
}
