package tw.com.gamestudio.etl.redash.demo;
import java.sql.*;

public class ConnOracle {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String DB_URL = "jdbc:oracle:thin:@IP:1521:toptest";

    //  Database credentials
    static final String USER = "ds";
    static final String PASS = "ds";

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            // Again, let us select all the records and display them.
            printRows();

            // Clean-up environment
            //stmt.close();
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
        //System.out.println("Finished Job!");
    }//end main

    public static void printRows() throws SQLException{
        System.out.println("Displaying available rows...");
        // Let us select all the records and display them.
        Connection conn = DriverManager.getConnection(DB_URL,USER,PASS);
        String sql = "select * from cl.gen_file";
        PreparedStatement stmt = conn.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery();


        int no_of_rows = 0;
        while(rs.next()){
            //Retrieve by column name
            String gen01  = rs.getString("gen01");
            String gen02 = rs.getString("gen02");
            String gen03 = rs.getString("gen03");
            String genacti = rs.getString("genacti");
            String mail = rs.getString("gen06");

            //Display values
            System.out.print("ID: " + gen01);
            System.out.print(", Name: " + gen02);
            System.out.print(", Dept: " + gen03);
            System.out.print(", avtive: " + genacti);
            System.out.println(", mail: " + mail);
            no_of_rows++;
        }
        System.out.println("There are "+ no_of_rows
                         + " record in the table");
        System.out.println();
        rs.close();
    }//end printRows()
}//end JDBCExample