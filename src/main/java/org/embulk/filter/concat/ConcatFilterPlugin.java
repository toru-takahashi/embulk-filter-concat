package org.embulk.filter.concat;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.ArrayList;
import java.util.List;

public class ConcatFilterPlugin implements FilterPlugin
{
    interface ColumnConfig extends Task
    {
        @Config("name")
        String getName();
    }

    interface PluginTask extends Task
    {
        @Config("name")
        String getName();

        @Config("columns")
        @ConfigDefault("[]")
        List<ColumnConfig> getColumns();

        @Config("delimiter")
        @ConfigDefault("\" \"")
        String getDelimiter();
    }

    private void configure(PluginTask task, Schema inputSchema) {
        List<ColumnConfig> columns = task.getColumns();

        if (columns.size() < 2) {
            throw new ConfigException("\"columns\" should be specified 2~ columns.");
        }

        // throw if column type is not supported
        for (ColumnConfig columnConfig : columns) {
            String name = columnConfig.getName();
            Type type = inputSchema.lookupColumn(name).getType();

            if (type instanceof JsonType) {
                throw new ConfigException(String.format("casting to json is not available: \"%s\"", name));
            }
            if (type instanceof TimestampType) {
                throw new ConfigException(String.format("casting to timestamp is not available: \"%s\"", name));
            }
        }
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        configure(task, inputSchema);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        Column outputColumn;

        for (Column inputColumn: inputSchema.getColumns()) {
            outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }

        outputColumn = new Column(i, task.getName(), Types.STRING);
        builder.add(outputColumn);
        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column outputColumn = outputSchema.lookupColumn(task.getName());
        final List<ColumnConfig> columnConfigs = task.getColumns();

        return new PageOutput() {
            private final PageReader reader = new PageReader(inputSchema);
            private final PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    ArrayList<String> buf = new ArrayList<String>();
                    for (ColumnConfig target: columnConfigs) {
                        Column column = outputSchema.lookupColumn(target.getName());
                        if (reader.isNull(column)) {
                            buf.add("");
                            continue;
                        }
                        if (Types.STRING.equals(column.getType())) {
                            buf.add(reader.getString(column));
                        } else if (Types.BOOLEAN.equals(column.getType())) {
                            buf.add(String.valueOf(reader.getBoolean(column)));
                        } else if (Types.DOUBLE.equals(column.getType())) {
                            buf.add(String.valueOf(reader.getDouble(column)));
                        } else if (Types.LONG.equals(column.getType())) {
                            buf.add(String.valueOf(reader.getLong(column)));
                        }
                    }

                    String output = Joiner.on(task.getDelimiter()).join(buf);

                    for (Column column: outputSchema.getColumns()) {
                        if (column.getName().equals(outputColumn.getName())) {
                            builder.setString(outputColumn, output);
                            continue;
                        }
                        if (reader.isNull(column)) {
                            builder.setNull(column);
                            continue;
                        }
                        if (Types.STRING.equals(column.getType())) {
                            builder.setString(column, reader.getString(column));
                        } else if (Types.BOOLEAN.equals(column.getType())) {
                            builder.setBoolean(column, reader.getBoolean(column));
                        } else if (Types.DOUBLE.equals(column.getType())) {
                            builder.setDouble(column, reader.getDouble(column));
                        } else if (Types.LONG.equals(column.getType())) {
                            builder.setLong(column, reader.getLong(column));
                        } else if (Types.TIMESTAMP.equals(column.getType())) {
                            builder.setTimestamp(column, reader.getTimestamp(column));
                        } else if (Types.JSON.equals(column.getType())) {
                            builder.setJson(column, reader.getJson(column));
                        }
                    }
                    builder.addRecord();
                }
            }
        };
    }
}