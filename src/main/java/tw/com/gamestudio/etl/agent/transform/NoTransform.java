package tw.com.gamestudio.etl.agent.transform;

import tw.com.gamestudio.etl.agent.api.Transform;
import tw.com.gamestudio.etl.agent.common.*;
import tw.com.gamestudio.etl.agent.common.*;

/**
 * @author Game Studio
 */
public class NoTransform implements Transform {

    private static final String ID = NoTransform.class.getName();

    private Report report = new Report(ID);

    @Override
    public String getId() {
        return (ID);
    }

    @Override
    public void init(Configuration configuration) throws Exception {
        report.start();
    }

    @Override
    public Table transform(Table table) {
        report.table();
        for (Column column : table.getColumns()) {
            report.column();
        }
        return (table);
    }

    @Override
    public Row transform(Table table, Row row) {
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
