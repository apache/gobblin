package com.linkedin.uif.source.extractor.responsys;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.extract.sftp.SftpExtractor;
import com.linkedin.uif.source.extractor.filebased.FileBasedHelperException;

/**
 * Extractor to pull Responsys data using the SFTP protocol
 * @author stakiar
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public class ResponsysExtractor extends SftpExtractor<String, String>
{
    private static final Logger log = LoggerFactory.getLogger(ResponsysExtractor.class);
    private static final String RESPONSYS_DECRYPT_KEY = "responsys.decrypt.key.location";
    
    public ResponsysExtractor(WorkUnitState workUnitState)
    {
        super(workUnitState);
    }
    
    @Override
    public Iterator<String> downloadFile(String file) throws IOException
    {
        log.info("Beginning to download file: " + file);

        try {
            InputStream inputStream = GPGFileDecrypter.decryptGPGFile(this.fsHelper.getFileStream(file), this.workUnit.getProp(RESPONSYS_DECRYPT_KEY));
            Iterator<String> fileItr = IOUtils.lineIterator(inputStream, "UTF-8");
            fileHandles.put(file, inputStream);
            if (workUnitState.getPropAsBoolean(ConfigurationKeys.SOURCE_SKIP_FIRST_RECORD, false) && fileItr.hasNext()) {
                fileItr.next();
            }
            return fileItr;
        } catch (FileBasedHelperException e) {
            throw new IOException("Exception while downloading file " + file + " with message " + e.getMessage(), e);
        }
    }
}
