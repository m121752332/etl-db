package tw.com.gamestudio.etl.redash.demo;
import java.sql.*;

public class ConnMysql {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://IP:3306/report_demo?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "root";

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            // Create SQL statement
            String SQL = "INSERT INTO employees(id,first,last,age,remark) " +
                         "VALUES(?, ?, ?, ?, ?)";

            // Create preparedStatemen
            System.out.println("Creating statement...");
            stmt = conn.prepareStatement(SQL);

            // Set auto-commit to false
            conn.setAutoCommit(false);

            // Delete all data in employees
            String sql = "DELETE FROM employees " +
                         " WHERE id IN ('400','401','L1317')";
            stmt.executeUpdate(sql);

            // First, let us select all the records and display them.
            printRows( stmt );

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

            // Set the variables
            stmt.setString( 1, "L1317" );
            stmt.setString( 2, "Game" );
            stmt.setString( 3, "Studio" );
            stmt.setInt( 4, 31 );
            stmt.setString( 5, "工程師" );
            // Add it to the batch
            stmt.addBatch();

            // Create an int[] to hold returned values
            int[] count = stmt.executeBatch();

            //Explicitly commit statements to apply changes
            conn.commit();

            // Again, let us select all the records and display them.
            printRows( stmt );

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

    public static void printRows(Statement stmt) throws SQLException{
        System.out.println("Displaying available rows...");
        // Let us select all the records and display them.
        String sql = "SELECT id, first, last, age, remark FROM employees";
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()){
            //Retrieve by column name
            String id  = rs.getString("id");
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