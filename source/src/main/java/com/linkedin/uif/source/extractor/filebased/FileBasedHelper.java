package com.linkedin.uif.source.extractor.filebased;

import java.io.InputStream;
import java.util.List;

public interface FileBasedHelper
{
    public void connect() throws FileBasedHelperException;
    public void close() throws FileBasedHelperException;
    public List<String> ls(String path) throws FileBasedHelperException;
    public InputStream getFileStream(String path) throws FileBasedHelperException;
}
