package com.linkedin.uif.source.extractor.filebased;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;

public class FileByteIterator implements Iterator<Byte> {

    private BufferedInputStream bufferedInputStream;
    
    public FileByteIterator(InputStream inputStream) throws IOException {
        this.bufferedInputStream = new BufferedInputStream(inputStream);
    }
    
    @Override
    public boolean hasNext() {
        try {
            return bufferedInputStream.available() > 0;
        } catch (IOException e) {
            Throwables.propagate(e);
            return false;
        }
    }

    @Override
    public Byte next()
    {
        try {
            if (this.hasNext()) {
                return new Byte((byte) bufferedInputStream.read());
            } else {
                throw new NoSuchElementException("No more data left in the file");
            }
        } catch (IOException e) {
            Throwables.propagate(e);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}