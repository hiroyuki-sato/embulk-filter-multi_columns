package org.embulk.filter.multi_columns;

import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.config.Task;
import org.embulk.spi.type.Types;
public class MultiColumnsInputSource
{
    private interface MultiColumnsTimestampOptions
            extends Task,TimestampParser.TimestampColumnOption
    {}

    private String inputColumnName;
    private Integer offset;
    private ColumnConfig config;
    private TimestampParser parser;
    public MultiColumnsInputSource(TimestampParser.Task task, ColumnConfig config,String inputColumnName, Integer offset)
    {
        this.inputColumnName = inputColumnName;
        this.offset = offset;
        this.config = config;
        if (Types.TIMESTAMP.equals(config.getType())) {
            TimestampParser.TimestampColumnOption option = config.getOption().loadConfig(MultiColumnsInputSource.MultiColumnsTimestampOptions.class);

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

    public String getInputColumnName()
    {
        return inputColumnName;
    }

    public String getName()
    {
        return config.getName();
    }

    public TimestampParser getTimestampParser() { return parser; }
}
