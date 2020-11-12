package tw.com.gamestudio.etl.redash.demo;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

/**
 * Demo table to table copy with JDBC BulkCopy
 * lib from mssql-jdbc-8.4.1.jar
 */
public class BulkCopySingle {
    public static void main(String[] args) throws NoSuchAlgorithmException {
        String connectionUrl = "jdbc:sqlserver://IP:1433;databaseName=demo;" +
                "user=sa;password=sa;" +
                //"sslProtocol=TLSv1.2;" +
                "useUnicode=true;CharacterSet=UTF-8;";
        String destinationTable = "dbo.bi_china_sales_test";
        int countBefore, countAfter;
        ResultSet rsSourceData;
        SQLServerBulkCopyOptions copyOptions;

        try (Connection sourceConnection = DriverManager.getConnection(connectionUrl);
             Connection destinationConnection = DriverManager.getConnection(connectionUrl);
             Statement stmt = sourceConnection.createStatement();
             SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(destinationConnection)) {

            copyOptions = new SQLServerBulkCopyOptions();
            copyOptions.setKeepIdentity(true);
            copyOptions.setBatchSize(10000);  //越小越久
            copyOptions.setBulkCopyTimeout(100);
            //copyOptions.setUseInternalTransaction(true);

            // Empty the destination table.
            stmt.executeUpdate("DELETE FROM " + destinationTable);

            // Perform an initial count on the destination table.
            countBefore = getRowCount(stmt, destinationTable);

            // Get data from the source table as a ResultSet.
            rsSourceData = stmt.executeQuery(
                    "select * from dbo.bi_china_sales");

            // In real world applications you would
            // not use SQLServerBulkCopy to move data from one table to the other
            // in the same database. This is for demonstration purposes only.

            // Set up the bulk copy object inside the transaction.
            destinationConnection.setAutoCommit(false);

            // Set up the bulk copy object.
            // Note that the column positions in the source
            // table match the column positions in
            // the destination table so there is no need to
            // map columns.
            bulkCopy.setBulkCopyOptions(copyOptions);
            bulkCopy.setDestinationTableName(destinationTable);

            // Write from the source to the destination.
            // This should fail with a duplicate key error
            // after some of the batches have been copied.
            try {
                bulkCopy.writeToServer(rsSourceData);
                destinationConnection.commit();
            }
            catch (SQLException e) {
                e.printStackTrace();
                destinationConnection.rollback();
            }
            bulkCopy.close();

            // Perform a final count on the destination
            // table to see how many rows were added.
            countAfter = getRowCount(stmt, destinationTable);
            System.out.println((countAfter - countBefore) + " rows were added.");
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static int getRowCount(Statement stmt,
                                   String tableName) throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
        int count = rs.getInt(1);
        rs.close();
        return count;
    }
}