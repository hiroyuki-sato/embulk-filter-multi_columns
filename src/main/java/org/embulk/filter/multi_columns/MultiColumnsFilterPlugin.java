package org.embulk.filter.multi_columns;

import org.embulk.config.Config;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

import java.util.ArrayList;

public class MultiColumnsFilterPlugin
        implements FilterPlugin
{
    public interface MultiColumnsRulesTask
            extends Task
    {
        @Config("separator")
        public String getSeparator();

        @Config("columns")
        SchemaConfig getSchemaConfig();

        @Config("src")
        String getSrc();

        @Config("remain")
        Boolean getRemain();
    }

    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("rules")
        ArrayList<MultiColumnsFilterPlugin.MultiColumnsRulesTask> getRules();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        MultiColumnsConfiguration multiColumnsConfig = new MultiColumnsConfiguration(task, inputSchema);
        Schema outputSchema = multiColumnsConfig.buildOutputSchema();

        control.run(task.dump(), outputSchema);
    }

    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput()
        {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private MultiColumnsConfiguration multiColumnsConfig = new MultiColumnsConfiguration(task, inputSchema);
            private MultiColumnsVisitorImpl visitor = new MultiColumnsVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder, multiColumnsConfig);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    visitor.updatePage();
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
