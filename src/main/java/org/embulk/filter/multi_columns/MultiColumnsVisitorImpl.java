package org.embulk.filter.multi_columns;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class MultiColumnsVisitorImpl
        implements ColumnVisitor

{
    private MultiColumnsFilterPlugin.PluginTask task;
    private Schema inputSchema;
    private Schema outputSchema;
    private PageReader pageReader;
    private PageBuilder pageBuilder;
    private MultiColumnsConfiguration multiColumnsConfig;
    private Map<String, String[]> separateMap;
    private static final Logger logger = Exec.getLogger(MultiColumnsFilterPlugin.class);

    MultiColumnsVisitorImpl(MultiColumnsFilterPlugin.PluginTask task, Schema inputSchema, Schema outputSchema,
            PageReader pageReader, PageBuilder pageBuilder, MultiColumnsConfiguration multiColumnsConfig)
    {
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        this.multiColumnsConfig = multiColumnsConfig;
        this.separateMap = new HashMap();
        this.task = task;
    }

    public void updatePage()
    {
        separateMap.clear();
        for (MultiColumnsFilterPlugin.MultiColumnsRulesTask rulesTask : task.getRules()) {
            String separator = rulesTask.getSeparator();
            String src = rulesTask.getSrc();
            Column column = inputSchema.lookupColumn(src);
            if (pageReader.isNull(column)) {
                separateMap.put(src, null);
            }
            else {
                String value = pageReader.getString(column);
                logger.debug(value);
                String[] values = value.split(separator);
                separateMap.put(src, values);
            }
        }
    }

    @Override
    public void booleanColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
        }
    }

    @Override
    public void longColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
        }
    }

    @Override
    public void doubleColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
        }
    }

    @Override
    public void stringColumn(Column outputColumn)
    {
        MultiColumnsInputSource inputSource = multiColumnsConfig.getMultiColumnsInputSource(outputColumn.getName());
        if (inputSource != null) {
            String[] row = separateMap.get(inputSource.getSrc());
            Integer offset = inputSource.getOffset();
            if (row.length < offset + 1) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                String v = row[offset];
                pageBuilder.setString(outputColumn, v);
            }
        }
        else {
            Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
            }
        }
    }

    @Override
    public void jsonColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setJson(outputColumn, pageReader.getJson(inputColumn));
        }
    }

    @Override
    public void timestampColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setTimestamp(outputColumn, pageReader.getTimestamp(inputColumn));
        }
    }
}
