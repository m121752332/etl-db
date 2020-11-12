package tw.com.gamestudio.etl.redash.demo;
import joinery.DataFrame;

import java.sql.*;

public class ConnMssql {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static final String DB_URL = "jdbc:sqlserver://IP:1433;databaseName=demo;useUnicode=true;CharacterSet=UTF-8;";
    //+ "database=report_demo;"
    //+ "encrypt=false;"
    //+ "trustServerCertificate=false;"
    //+ "sslProtocol=TLSv1.2;"
    //+ "useUnicode=true;"
    //+ "CharacterSet=UTF-8;"
    //+ "CharacterSet=ISO-8859-1;"
    //+ "loginTimeout=30;";

    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "sa";

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);


            // Delete all data in employees
            String sql = "DELETE FROM employees " +
                    " WHERE id IN ('400','401')";
            stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();

            // Create SQL statement
            String SQL = "INSERT INTO employees(id,first,last,age,remark) " +
                         "VALUES(?, ?, ?, ?, ?)";

            // Create preparedStatemen
            System.out.println("Creating statement...");
            stmt = conn.prepareStatement(SQL);

            // Set auto-commit to false
            conn.setAutoCommit(false);

            // First, let us select all the records and display them.
            printRows();

            // Set the variables
            stmt.setInt( 1, 400 );
            stmt.setString( 2, "Pappu" );
            stmt.setString( 3, "Singh" );
            stmt.setInt( 4, 33 );
            stmt.setString( 5, "測試人員" );
            // Add it to the batch
            stmt.addBatch();

            // Set the variables
            stmt.setInt( 1, 401 );
            stmt.setString( 2, "Pawan" );
            stmt.setString( 3, "Singh" );
            stmt.setInt( 4, 31 );
            stmt.setString( 5, "開發人員" );
            // Add it to the batch
            stmt.addBatch();

            // Create an int[] to hold returned values
            int[] count = stmt.executeBatch();

            //Explicitly commit statements to apply changes
            conn.commit();

            // Again, let us select all the records and display them.
            printRows();

            // Clean-up environment
            stmt.close();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }//end main

    public static void printRows() throws SQLException{
        System.out.println("Displaying available rows...");
        // Let us select all the records and display them.
        Connection conn = DriverManager.getConnection(DB_URL,USER,PASS);
        String sql = "SELECT id, first, last, age, remark FROM employees";
        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();


        DataFrame df = new DataFrame();

        while(rs.next()){
            //Retrieve by column name
            int id  = rs.getInt("id");
            int age = rs.getInt("age");
            String first = rs.getString("first");
            String last = rs.getString("last");
            String remark = rs.getString("remark");

            //Display values
            System.out.print("ID: " + id);
            System.out.print(", Age: " + age);
            System.out.print(", First: " + first);
            System.out.print(", Last: " + last);
            System.out.println(", Remark: " + last);
        }
        System.out.println();
        rs.close();
    }//end printRows()
}//end JDBCExample