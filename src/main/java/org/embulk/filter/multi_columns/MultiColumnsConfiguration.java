package org.embulk.filter.multi_columns;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MultiColumnsConfiguration
{
    private MultiColumnsFilterPlugin.PluginTask task;
    private Schema inputSchema;
    private final Map<String, MultiColumnsFilterPlugin.MultiColumnsRulesTask> inputRules = new HashMap();
    private final Map<String, MultiColumnsInputSource> ruleMap = new HashMap();
    private ArrayList<ColumnConfig> multiColumns = new ArrayList();
    private static final Logger logger = Exec.getLogger(MultiColumnsFilterPlugin.class);

    public MultiColumnsConfiguration(MultiColumnsFilterPlugin.PluginTask task, Schema inputSchema)
    {
        this.inputSchema = inputSchema;
        this.task = task;
        buildColumnMapping();
    }

    private void buildColumnMapping()
    {
        ArrayList<MultiColumnsFilterPlugin.MultiColumnsRulesTask> rules = task.getRules();
        for (MultiColumnsFilterPlugin.MultiColumnsRulesTask ruleTask : rules) {
            String src = ruleTask.getSrc();
            inputSchema.lookupColumn(src); // throw Exception.
            inputRules.put(src, ruleTask);

            buildRuleMap(src, ruleTask.getSchemaConfig());
        }
    }

    private void buildRuleMap(String src, SchemaConfig schemaConfig)
    {
        int offset = 0;
        for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
            ruleMap.put(columnConfig.getName(), new MultiColumnsInputSource(task, columnConfig, src, offset++));
            multiColumns.add(columnConfig);
        }
    }

    public MultiColumnsInputSource getMultiColumnsInputSource(String name)
    {
        return ruleMap.get(name);
    }

    public MultiColumnsFilterPlugin.MultiColumnsRulesTask getInputRule(String name)
    {
        return inputRules.get(name);
    }

    public ArrayList<ColumnConfig> getMultiColumns()
    {
        return multiColumns;
    }

    public Integer getColumnsSize()
    {
        return inputSchema.getColumnCount() + multiColumns.size();
    }

    public Schema buildOutputSchema()
    {
        Schema.Builder builder = Schema.builder();
        for (Column column : inputSchema.getColumns()) {
            MultiColumnsFilterPlugin.MultiColumnsRulesTask input = inputRules.get(column.getName());
            if (input == null || input.getRemain() == true) {
                builder.add(column.getName(), column.getType());
            }
        }

        for (ColumnConfig config : multiColumns) {
            logger.debug(config.getName());
            builder.add(config.getName(), config.getType());
        }
        return builder.build();
    }
}
