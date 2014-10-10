package com.linkedin.uif.source.extractor.schema;

/**
 * Attributes of column in projection list
 * 
 * @author nveeramr
 */
public class ColumnAttributes {
    String columnName;
    String AliasName;
    String sourceTableName;
    String sourceColumnName;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAliasName() {
        return AliasName;
    }

    public void setAliasName(String aliasName) {
        AliasName = aliasName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }
}
