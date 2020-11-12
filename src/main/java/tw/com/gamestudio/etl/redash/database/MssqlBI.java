package tw.com.gamestudio.etl.redash.database;

/**
 * DB_URL :
 * @loginTimeout 驅動程式應等待失敗連接逾時的秒數。 值為零表示此逾時為預設系統逾時，預設指定為 15 秒。非零值為驅動程式應等待失敗連接之逾時的秒數。
 * 如果您在 Server 連線屬性中指定虛擬網路名稱，您應該指定三分鐘或更長的逾時值，好讓容錯移轉連線有充足的時間可以順利完成。
 * 請參閱 JDBC 驅動程式對於高可用性、災害復原的支援，以取得詳細資訊。
 */
public class MssqlBI {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static final String DB_URL = "jdbc:sqlserver://IP:1433;databaseName=demo;useUnicode=true;CharacterSet=UTF-8;";
    static final String CHAR_SET = "UTF-8";
    static final String NEW_CHAR_SET = "UTF-8";

    //  Database credentials
    private String USER = "sa";
    private String PASS = "sa";

    public MssqlBI(){}

    public MssqlBI(String user,String password){
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
    public String getUser(){
        return USER;
    }
    public String getPass(){
        return PASS;
    }

    public void setUser(String user){
        USER = user;
    }

    public void setPass(String password){
        PASS = password;
    }
}
