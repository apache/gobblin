package com.linkedin.uif.converter.filter;

import java.util.HashSet;

public class DataPurgerAvroFilterConverter extends AvroFilterConverter
{
    @Override
    public HashSet<Object> getFilterIds()
    {
        // TODO - where will the member IDs come from?
        return new HashSet<Object>();
    }
}
