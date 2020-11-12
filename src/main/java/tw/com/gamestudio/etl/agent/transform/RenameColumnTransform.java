package tw.com.gamestudio.etl.agent.transform;

import tw.com.gamestudio.etl.agent.common.Column;
import tw.com.gamestudio.etl.agent.common.Row;
import tw.com.gamestudio.etl.agent.common.Table;
import tw.com.gamestudio.etl.agent.api.Transform;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Report;

/**
 * @author Game Studio
 */
public class RenameColumnTransform implements Transform {

    private static final String ID = RenameColumnTransform.class.getName();

    private Report report = new Report(ID);

    private String tablePattern;

    private String columnPattern;

    private String newName;

    @Override
    public String getId() {
        return (ID);
    }

    @Override
    public void init(Configuration configuration) throws Exception {

        report.start();

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

        // required
        newName = configuration.get("new_name", null);
        if (newName == null) {
            throw new Exception("missing required parameter 'new_name'");
        }

    }

    @Override
    public Table transform(Table table) {
        if (!table.getName().matches(tablePattern)) {
            return (table);
        }
        int i = 0;
        for (Column column : table.getColumns()) {
            if (column.getName().matches(columnPattern)) {
                table.getColumns().set(i, new Column(newName, column.getTypeName(), column.getClassName()));
                report.column();
            }
            i++;
        }
        report.table();
        return (table);
    }

    @Override
    public Row transform(Table table, Row row) {
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
