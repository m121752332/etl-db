package tw.com.gamestudio.etl.agent.transform;

import tw.com.gamestudio.etl.agent.common.Column;
import tw.com.gamestudio.etl.agent.common.Row;
import tw.com.gamestudio.etl.agent.common.Table;
import tw.com.gamestudio.etl.agent.api.Transform;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Report;

import java.text.DecimalFormat;

/**
 * @author Game Studio
 */
public class DecimalFormatTransform implements Transform {

    private static final String ID = DecimalFormatTransform.class.getName();

    private Report report = new Report(ID);

    private DecimalFormat format;

    private String tablePattern;

    private String columnPattern;

    @Override
    public String getId() {
        return (ID);
    }

    @Override
    public void init(Configuration configuration) throws Exception {

        report.start();

        // required
        format = new DecimalFormat(configuration.get("format", null));
        if (format == null) {
            throw new Exception("missing required parameter 'format'");
        }

        // required
        tablePattern = configuration.get("table_pattern", null);
        if (tablePattern == null) {
            throw new Exception("missing required parameter 'table_pattern'");
        }

        // required
        columnPattern = configuration.get("column_pattern", null);
        if (columnPattern == null) {
            throw new Exception("missing required parameter 'column_pattern'");
        }

    }

    @Override
    public Table transform(Table table) {
        if (table.getName().matches(tablePattern)) {
            report.table();
        }
        for (Column column : table.getColumns()) {
            if (column.getName().matches(columnPattern)) {
                report.column();
            }
        }
        return (table);
    }

    @Override
    public Row transform(Table table, Row row) {
        if (!table.getName().matches(tablePattern)) {
            return (row);
        }
        for (int i = 0; i < row.getValues().size(); i++) {
            Column column = table.getColumns().get(i);
            if (!column.getName().matches(columnPattern)) {
                continue;
            }
            Object value = row.getValues().get(i);
            if (value instanceof Float) {
                row.getValues().set(i, format.format((float) value));
            }
            if (value instanceof Double) {
                row.getValues().set(i, format.format((double) value));
            }
            report.column();
        }
        report.row();
        return (row);
    }

    @Override
    public void clean() {
        report.end();
    }

    @Override
    public Report report() {
        return (report);
    }

}
