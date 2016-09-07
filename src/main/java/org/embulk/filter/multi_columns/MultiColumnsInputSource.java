package org.embulk.filter.multi_columns;

import org.embulk.spi.ColumnConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;

public class MultiColumnsInputSource
{
    private String src;
    private Integer offset;
    private ColumnConfig config;
    private TimestampParser parser;

    public MultiColumnsInputSource(TimestampParser.Task task, ColumnConfig config, String src, Integer offset)
    {
        this.src = src;
        this.offset = offset;
        this.config = config;
        if (Types.TIMESTAMP.equals(config.getType())) {
            TimestampParser.TimestampColumnOption option = config.getOption().loadConfig(TimestampParser.TimestampColumnOption.class);
            this.parser = new TimestampParser(task, option);
        }
        else {
            this.parser = null;
        }
    }

    public Integer getOffset()
    {
        return offset;
    }

    public String getSrc()
    {
        return src;
    }

    public String getName()
    {
        return config.getName();
    }

    public TimestampParser getTimestampParser() { return parser; }
}
