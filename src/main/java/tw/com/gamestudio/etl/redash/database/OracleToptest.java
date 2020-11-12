package tw.com.gamestudio.etl.redash.database;

public class OracleToptest {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String DB_URL = "jdbc:oracle:thin:@IP:1521:toptest";
    static final String CHAR_SET = "UTF-8";

    //  Database credentials
    private String USER = "ds";
    private String PASS = "ds";

    public OracleToptest(String user,String password){
        USER = user;
        PASS = password;
    }

    public String getDriver(){
        return JDBC_DRIVER;
    }

    public String getDBUrl(){
        return DB_URL;
    }

    public void setUser(String user){
        USER = user;
    }

    public void setPass(String password){
        PASS = password;
    }
}
