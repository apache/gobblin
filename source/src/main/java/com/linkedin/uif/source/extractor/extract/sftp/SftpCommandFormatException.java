package com.linkedin.uif.source.extractor.extract.sftp;

public class SftpCommandFormatException extends Exception
{
    private static final long serialVersionUID = 1L;

    public SftpCommandFormatException(String message) {
        super(message);
    }
    
    public SftpCommandFormatException(String message, Exception e) {
        super(message, e);
    }
}