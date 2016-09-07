package org.embulk.filter.multi_columns;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;
import org.embulk.filter.multi_columns.MultiColumnsFilterPlugin.PluginTask;


import java.util.List;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

public class TestMultiColumnsVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResource()
    {
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        return config.loadConfig(PluginTask.class);
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object ... objects)
    {
        TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();
        MultiColumnsConfiguration multiColumnsConfig = new MultiColumnsConfiguration(task, inputSchema);
        Schema outputSchema = multiColumnsConfig.buildOutputSchema();

        PageBuilder pageBuilder = new PageBuilder(runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = new PageReader(inputSchema);
        MultiColumnsVisitorImpl visitor = new MultiColumnsVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder, multiColumnsConfig);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, objects);
        for (Page page : pages) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                // MultiColumns Specific method.
                visitor.updatePage();

                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
        pageBuilder.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void visit_multiColumns_NoSplit()
    {
        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: []");
        Schema inputSchema = Schema.builder()
                .add("timestamp",TIMESTAMP)
                .add("string",STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double",DOUBLE)
                .add("json",JSON)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"),
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600),record[0]);
            assertEquals("string",record[1]);
            assertEquals(new Boolean(true),record[2]);
            assertEquals(new Long(0),record[3]);
            assertEquals(new Double(0.5),record[4]);
            assertEquals(ValueFactory.newString("json"),record[5]);
        }
    }

    @Test
    public void visit_multiColumns_SplitString()
    {
        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: ",
                "  - src: full_name",
                "    columns:",
                "      - { name: \"first\", type: string }",
                "      - { name: \"last\", type: string }",
                "      - { name: \"null\", type: string }");
        Schema inputSchema = Schema.builder()
                .add("full_name",STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "Shane Bogisich",
                // row2
                "Marlin Hahn");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(3, record.length);
            assertEquals("Shane",record[0]);
            assertEquals("Bogisich",record[1]);
            assertEquals(null,record[2]);
        }

    }


    @Test
    public void visit_multiColumns_SplitMultiTypes()
    {
//                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"),

        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: ",
                "  - src: input",
                "    columns:",
                "      - { name: timestamp, type: timestamp, format: \"%Y-%m-%d\" }",
                "      - { name: string,    type: string }",
                "      - { name: boolean,   type: boolean }",
                "      - { name: long,      type: long }",
                "      - { name: double,    type: double }");
        Schema inputSchema = Schema.builder()
                .add("input",STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "2016-01-01 test true 12345 98.7",
                // row2
                "2016-01-01 test true 12345 98.7");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(5, record.length);
            assertEquals(Timestamp.ofEpochSecond(1451606400),record[0]);
            assertEquals("test",record[1]);
            assertEquals(new Boolean(true),record[2]);
            assertEquals(new Long(12345),record[3]);
            assertEquals(new Double(98.7),record[4]);
        }

    }


    @Test
    public void visit_multiColumns_RemainColumn()
    {
        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: ",
                "  - src: full_name",
                "    remain: true",
                "    columns:",
                "      - { name: \"first\", type: string }",
                "      - { name: \"last\", type: string }");
        Schema inputSchema = Schema.builder()
                .add("full_name",STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "Shane Bogisich",
                // row2
                "Marlin Hahn");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(3, record.length);
            assertEquals("Shane Bogisich",record[0]);
            assertEquals("Shane",record[1]);
            assertEquals("Bogisich",record[2]);
        }

    }

    @Test
    public void visit_multiColumns_SplitTab()
    {
        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: ",
                "  - src: full_name",
                "    separator: \"\\\\t\"",
                "    columns:",
                "      - { name: \"first\", type: string }",
                "      - { name: \"last\", type: string }");
        Schema inputSchema = Schema.builder()
                .add("full_name",STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "Shane\tBogisich",
                // row2
                "Marlin\tHahn");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(2, record.length);
            assertEquals("Shane",record[0]);
            assertEquals("Bogisich",record[1]);
        }

    }

    @Test
    public void visit_multiColumns_SplitMultiColumn()
    {
        PluginTask task = taskFromYamlString(
                "type: multi_columns",
                "rules: ",
                "  - src: full_name",
                "    separator: \"\\\\t\"",
                "    columns:",
                "      - { name: \"first\", type: string }",
                "      - { name: \"last\", type: string }",
        "  - src: full_name2",
                "    separator: \"!\"",
                "    columns:",
                "      - { name: \"first2\", type: string }",
                "      - { name: \"last2\", type: string }");
        Schema inputSchema = Schema.builder()
                .add("full_name",STRING)
                .add("full_name2",STRING)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "Shane\tBogisich","Josiah!Bartoletti",
                // row2
                "Marlin\tHahn","Lonny!Watsica");

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(4, record.length);
            assertEquals("Shane",record[0]);
            assertEquals("Bogisich",record[1]);
            assertEquals("Josiah",record[2]);
            assertEquals("Bartoletti",record[3]);
        }

    }

}
