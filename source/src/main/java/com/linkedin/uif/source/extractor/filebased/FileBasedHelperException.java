package com.linkedin.uif.source.extractor.filebased;

public class FileBasedHelperException extends Exception
{
    private static final long serialVersionUID = 1L;

    public FileBasedHelperException(String message) {
        super(message);
    }
    
    public FileBasedHelperException(String message, Exception e) {
        super(message, e);
    }
}
