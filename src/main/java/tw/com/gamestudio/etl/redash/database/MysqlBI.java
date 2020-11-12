package tw.com.gamestudio.etl.redash.database;

/**
 * DB_URL :
 */
public class MysqlBI {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://IP:3306/report_demo?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
    static final String CHAR_SET = "UTF-8";

    //  Database credentials
    private String USER = "root";
    private String PASS = "root";

    public MysqlBI(){}

    public MysqlBI(String user,String password){
        USER = user;
        PASS = password;
    }

    public String getDriver(){
        return JDBC_DRIVER;
    }

    public String getDBUrl(){
        return DB_URL;
    }
    public String getCharSet(){
        return CHAR_SET;
    }

    public void setUser(String user){
        USER = user;
    }

    public void setPass(String password){
        PASS = password;
    }
}
