package org.embulk.filter.multi_columns;

public class MultiColumnsInputSource
{
    private String src;
    private Integer offset;
    private String name;

    public MultiColumnsInputSource(String name, String src, Integer offset)
    {
        this.src = src;
        this.offset = offset;
        this.name = name;
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
        return name;
    }
}
