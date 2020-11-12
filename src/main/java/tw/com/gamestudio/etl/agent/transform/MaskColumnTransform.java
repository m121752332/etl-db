package tw.com.gamestudio.etl.agent.transform;

import tw.com.gamestudio.etl.agent.common.Column;
import tw.com.gamestudio.etl.agent.common.Row;
import tw.com.gamestudio.etl.agent.common.Table;
import tw.com.gamestudio.etl.agent.api.Transform;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Report;
import tw.com.gamestudio.etl.agent.utils.ExceptionUtils;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * @author Game Studio
 */
public class MaskColumnTransform implements Transform {

    private static final String ID = MaskColumnTransform.class.getName();

    private Report report = new Report(ID);

    private static final String MASK_POLICY_EMPTY = "empty";

    private static final String MASK_POLICY_HIDE = "hide";

    private static final String MASK_POLICY_MD5 = "md5";

    private String tablePattern;

    private String columnPattern;

    private String maskPolicy;

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
        maskPolicy = configuration.get("mask_policy", null);
        if (maskPolicy == null) {
            throw new Exception("missing required parameter 'mask_policy'");
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
        int i = 0;
        for (Column column : table.getColumns()) {
            if (column.getName().matches(columnPattern)) {
                switch (maskPolicy) {
                    case MASK_POLICY_EMPTY : row.getValues().set(i, empty(row.getValues().get(i))); break;
                    case MASK_POLICY_HIDE : row.getValues().set(i, hide(row.getValues().get(i))); break;
                    case MASK_POLICY_MD5 : row.getValues().set(i, md5(row.getValues().get(i))); break;
                    default: report.warning(String.format("Unknown mask policy '%s'", maskPolicy)); break;
                }
            }
            i++;
        }
        report.row();
        return (row);
    }

    private String empty(Object o) {
        return ("");
    }

    private String hide(Object o) {
        if (o instanceof String) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ((String) o).length(); i++) {
                sb.append("*");
            }
            return (sb.toString());
        } else {
            return ("****");
        }
    }

    private String md5(Object o) {
        try {
            byte[] b = o.toString().getBytes();
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(b);
            return (new BigInteger(1, messageDigest.digest()).toString(16));
        } catch (Exception e) {
            ExceptionUtils.handleException(e);
            return ("");
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
