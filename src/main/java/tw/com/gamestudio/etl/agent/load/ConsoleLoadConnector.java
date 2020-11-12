package tw.com.gamestudio.etl.agent.load;

import tw.com.gamestudio.etl.agent.api.*;
import tw.com.gamestudio.etl.agent.api.Format;
import tw.com.gamestudio.etl.agent.api.LoadConnector;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Report;
import tw.com.gamestudio.etl.agent.common.Row;
import tw.com.gamestudio.etl.agent.common.Table;
import tw.com.gamestudio.etl.agent.utils.ExceptionUtils;

/**
 * @author Game Studio
 */
public class ConsoleLoadConnector implements LoadConnector {

    private static final String ID = ConsoleLoadConnector.class.getName();

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
    public void header(Format format) {
        if (format.getHeader() != null) {
            try {
                System.out.write(format.getHeader());
            } catch (Exception e) {
                ExceptionUtils.handleException(e);
                report.error(e.getMessage());
            }
        }
    }

    @Override
    public void headerSeparator(Table table, Format format) {
        if (table == null) {
            return;
        }
        if (format.getHeaderSeparator() != null) {
            try {
                System.out.write(format.getHeaderSeparator());
            } catch (Exception e) {
                ExceptionUtils.handleException(e);
                report.error(e.getMessage());
            }
        }
    }

    @Override
    public void rowSeparator(Table table, Format format) {
        if (table == null) {
            return;
        }
        if (format.getRowSeparator() !=  null) {
            try {
                System.out.write(format.getRowSeparator());
            } catch (Exception e) {
                ExceptionUtils.handleException(e);
                report.error(e.getMessage());
            }
        }
    }

    @Override
    public void footer(Format format) {
        if (format.getFooter() != null) {
            try {
                System.out.write(format.getFooter());
            } catch (Exception e) {
                ExceptionUtils.handleException(e);
                report.error(e.getMessage());
            }
        }
    }

    @Override
    public void load(Table table, Format format) {
        if (table == null) {
            return;
        }
        try {
            byte[] b = format.format(table);
            if (b != null) {
                System.out.write(b);
            }
            report.column(table.getColumns().size());
            report.table();
        } catch (Exception e) {
            ExceptionUtils.handleException(e);
            report.error(e.getMessage());
        }
    }

    @Override
    public void load(Table table, Row row, Format format) {
        if (table == null || row == null) {
            return;
        }
        try {
            byte[] b = format.format(table, row);
            if (b != null) {
                System.out.write(b);
            }
            report.row();
        } catch (Exception e) {
            ExceptionUtils.handleException(e);
            report.error(e.getMessage());
        }
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
