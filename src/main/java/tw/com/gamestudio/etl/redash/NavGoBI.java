package tw.com.gamestudio.etl.redash;
import org.apache.commons.lang3.StringUtils;
import tw.com.gamestudio.etl.redash.database.MssqlNav2009;
import tw.com.gamestudio.etl.redash.database.MssqlBI;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.math.BigDecimal;

public class NavGoBI {
    //處理顯示參數每100筆執行結果
    static final private int PROCESS_MOD = 100;

    static class r0025_china {
        String company="";
        String document_type="";
        String posting_date="";
        String invoice_no_="";
        String approved_no_="";
        String external_no_="";
        String si_group="";
        String si_group_not="";
        String si_desc="";
        String reason_code="";
        String reason_desc="";
        String cate_code="";
        String sell_customer_no_="";
        String sell_customer_name="";
        String ship_customer_name="";
        String bill_customer_no_="";
        String bill_customer_name="";
        String gui_no_="";
        String dim_1="";
        String dim_2="";
        String dim_2_name="";
        int    line_no_;
        String order_no_="";
        String order_date="";
        String posting_group="";
        String cat_code="";
        String cat_code_not="";
        String vendor_no_="";
        String vendor_no_q="";
        String service_item_no_="";
        String ibs_description="";
        String service_item_serial_no_="";
        String item_no_="";
        String item_desc="";
        int    line_type;
        int    line_type_desc;
        String location_code="";
        String currency_code="";
        BigDecimal currency_factor;
        BigDecimal unit_cost;
        BigDecimal quantity;
        BigDecimal unit_price;
        BigDecimal item_unit_price;
        BigDecimal cost;
        BigDecimal amount;
        BigDecimal amount_lcy;
        BigDecimal amount_vat;
        String vendor_group="";
        String vendor_group_kpi="";
        String posting_group_not="";
        BigDecimal sales_price;
        BigDecimal sales_price_vat;
        String cust_dim1="";
        String cust_price_group="";

        public r0025_china() {
        }// end of constructor()

        public String toString() {
            //return r0025_china(this.company,this.invoice_no_,this.si_desc,this.reason_code,this.reason_desc);
            return "company: '" + this.company + "', invoice_no: '" + this.invoice_no_ + "', si_desc: '" + this.si_desc + "'";
        }
    }


    public static void main(String[] args) {
        Connection nav_conn = null;
        Connection bi_conn  = null;
        PreparedStatement nav_stmt = null;
        PreparedStatement bi_stmt  = null;
        MssqlNav2009 nav2009 = new MssqlNav2009();
        MssqlBI      bi      = new MssqlBI();
        try{
            // Register JDBC driver
            Class.forName(nav2009.getDriver());
            Class.forName(bi.getDriver());

            // Open a connection
            System.out.println("Connecting to database...");
            nav_conn = DriverManager.getConnection(nav2009.getDBUrl(),nav2009.getUser(),nav2009.getPass());
            bi_conn = DriverManager.getConnection(bi.getDBUrl(),bi.getUser(),bi.getPass());


            // Delete all data in mssqlbi.bi_china_sales
            String sql = "DELETE FROM bi_china_sales " +
                         " WHERE 1=1";
            bi_stmt = bi_conn.prepareStatement(sql);
            bi_stmt.executeUpdate();


            // Create preparedStatemen
            System.out.println("Creating statement...");

            // Set auto-commit to false
            bi_conn.setAutoCommit(false);

            // First, let us select all the records and display them.
            catchAndInsertRows(nav2009,nav_conn,bi,bi_conn);

            //Explicitly commit statements to apply changes
            bi_conn.commit();

            // Clean-up environment
            //nav_stmt.close();
            nav_conn.close();
            bi_stmt.close();
            bi_conn.close();

        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
            System.err.println("------SQLException------");
            System.err.println("Error message:  " + se.getMessage());
            System.err.println("SQLState:  " + se.getSQLState());

        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(nav_stmt!=null)
                    nav_stmt.close();
                if(bi_stmt!=null)
                    bi_stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(nav_conn!=null)
                    nav_conn.close();
                if(bi_conn!=null)
                    bi_conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
    }//end main

    public static void catchAndInsertRows(MssqlNav2009 nav2009,Connection nav_conn,MssqlBI bi,Connection bi_conn)
            throws SQLException, UnsupportedEncodingException {
        System.out.println("Displaying available rows...");


        // Create SQL statement
        String SQL = "INSERT INTO bi_china_sales \n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,\n" +
                "       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,\n" +
                "       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement bi_stmt = bi_conn.prepareStatement(SQL);

        // Let us select all the records and display them.
        // 54 columns
        String sql = "SELECT company,\n" +
                "       document_type,\n" +
                "       posting_date,\n" +
                "       invoice_no_,\n" +
                "       approved_no_,\n" +
                "       external_no_,\n" +
                "       si_group,\n" +
                "       si_group_not,\n" +
                "       si_desc,\n" +
                "       reason_code,\n" +
                "       reason_desc,\n" +
                "       cate_code, \n" +
                "       sell_customer_no_,\n" +
                "       sell_customer_name,\n" +
                "       ship_customer_name,\n" +
                "       bill_customer_no_,\n" +
                "       bill_customer_name,\n" +
                "       gui_no_,\n" +
                "       dim_1,\n" +
                "       dim_2,\n" +
                "       dim_2_name,\n" +
                "       line_no_,\n" +
                "       order_no_,\n" +
                "       order_date,\n" +
                "       posting_group,\n" +
                "       cat_code,\n" +
                "       cat_code_not,\n" +
                "       vendor_no_,\n" +
                "       vendor_no_q,\n" +
                "       [Service Item No_],\n" +
                "       ibs_description,\n" +
                "       [Service Item Serial No_],\n" +
                "       item_no_,\n" +
                "       item_desc,\n" +
                "       line_type,\n" +
                "       line_type_desc,\n" +
                "       location_code,\n" +
                "       currency_code,\n" +
                "       currency_factor,\n" +
                "       '0' as unit_cost,\n" +
                "       quantity,\n" +
                "       unit_price,\n" +
                "       item_unit_price,\n" +
                "       '0' as cost,\n" +
                "       amount,\n" +
                "       amount_lcy,\n" +
                "       amount_vat,\n" +
                "       vendor_group,\n" +
                "       vendor_group_KPI,\n" +
                "       posting_group_not,\n" +
                "       sales_price,\n" +
                "       sales_price_vat,\n" +
                "       cust_dim1,\n" +
                "       cust_price_group\n" +
                "  FROM dbo.r0025_china\n" +
                " WHERE 1=1\n" +
                " ORDER BY document_type,posting_date,invoice_no_";
        PreparedStatement stmt = nav_conn.prepareStatement(sql);
        System.out.println(sql);
        ResultSet rs = stmt.executeQuery();

        int no_of_rows = 0;
        int[] count ;
        while(rs.next()){
            //Retrieve by column name
            //String invoice_no = rs.getString("invoice_no_");
            //String rsname = rs.getString("si_desc");
            //String si_desc  = new String(rsname.getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            //String si_desc  = new String(rs.getString("si_desc").getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());

            r0025_china table = new r0025_china();
            table = fillTable(rs,table);

            bi_stmt.setString(1,table.company);
            bi_stmt.setString(2,table.document_type);
            bi_stmt.setString(3,table.posting_date);
            bi_stmt.setString(4,table.invoice_no_);
            bi_stmt.setString(5,table.approved_no_);
            bi_stmt.setString(6,table.external_no_);
            bi_stmt.setString(7,table.si_group);
            bi_stmt.setString(8,table.si_group_not);
            bi_stmt.setString(9,table.si_desc);
            bi_stmt.setString(10,table.reason_code);
            bi_stmt.setString(11,table.reason_desc);
            bi_stmt.setString(12,table.cate_code);
            bi_stmt.setString(13,table.sell_customer_no_);
            bi_stmt.setString(14,table.sell_customer_name);
            bi_stmt.setString(15,table.ship_customer_name);
            bi_stmt.setString(16,table.bill_customer_no_);
            bi_stmt.setString(17,table.bill_customer_name);
            bi_stmt.setString(18,table.gui_no_);
            bi_stmt.setString(19,table.dim_1);
            bi_stmt.setString(20,table.dim_2);
            bi_stmt.setString(21,table.dim_2_name);
            bi_stmt.setInt(22,table.line_no_);
            bi_stmt.setString(23,table.order_no_);
            bi_stmt.setString(24,table.order_date);
            bi_stmt.setString(25,table.posting_group);
            bi_stmt.setString(26,table.cat_code);
            bi_stmt.setString(27,table.cat_code_not);
            bi_stmt.setString(28,table.vendor_no_);
            bi_stmt.setString(29,table.vendor_no_q);
            bi_stmt.setString(30,table.service_item_no_);
            bi_stmt.setString(31,table.ibs_description);
            bi_stmt.setString(32,table.service_item_serial_no_);
            bi_stmt.setString(33,table.item_no_);
            bi_stmt.setString(34,table.item_desc);
            bi_stmt.setInt(35,table.line_type);
            bi_stmt.setInt(36,table.line_type_desc);
            bi_stmt.setString(37,table.location_code);
            bi_stmt.setString(38,table.currency_code);
            bi_stmt.setBigDecimal(39,table.currency_factor);
            bi_stmt.setBigDecimal(40,table.unit_cost);
            bi_stmt.setBigDecimal(41,table.quantity);
            bi_stmt.setBigDecimal(42,table.unit_price);
            bi_stmt.setBigDecimal(43,table.item_unit_price);
            bi_stmt.setBigDecimal(44,table.cost);
            bi_stmt.setBigDecimal(45,table.amount);
            bi_stmt.setBigDecimal(46,table.amount_lcy);
            bi_stmt.setBigDecimal(47,table.amount_vat);
            bi_stmt.setString(48,table.vendor_group);
            bi_stmt.setString(49,table.vendor_group_kpi);
            bi_stmt.setString(50,table.posting_group_not);
            bi_stmt.setBigDecimal(51,table.sales_price);
            bi_stmt.setBigDecimal(52,table.sales_price_vat);
            bi_stmt.setString(53,table.cust_dim1);
            bi_stmt.setString(54,table.cust_price_group);

            bi_stmt.addBatch();
            //Display values
            //DataFrame df = new DataFrame();
            //df.writeSql(stmt);
            //System.out.println("DF size:"+df.size());
            //System.out.println();

            //Count record
            no_of_rows++;
            if (no_of_rows % PROCESS_MOD ==0){
                System.out.println("Process no:"+ no_of_rows +" to Content:"+table.toString());
            }
        }
        System.out.println("There are "+ no_of_rows
                + " record in the table. Ready commit...");
        System.out.println();
        // Create an int[] to hold returned values
        count = bi_stmt.executeBatch();
        System.out.println("There are "+ count.length
                + " record in the addBatch. Mission completed.");

        rs.close();
    }//end printRows()


    public static r0025_china fillTable(ResultSet rs,r0025_china table)  {

        MssqlNav2009 nav2009 = new MssqlNav2009();
        try {
            table.company = rs.getString(1);
            table.document_type = rs.getString(2);
            table.posting_date = rs.getString(3);
            table.invoice_no_=rs.getString(4);
            table.approved_no_=rs.getString(5);
            table.external_no_=new String(rs.getString(6).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.si_group=rs.getString(7);
            table.si_group_not=rs.getString(8);
            table.si_desc=new String(rs.getString(9).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.reason_code=new String(StringUtils.defaultString(rs.getString(10)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.reason_desc=new String(StringUtils.defaultString(rs.getString(11)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.cate_code=rs.getString(12);
            table.sell_customer_no_=rs.getString(13);
            table.sell_customer_name=new String(StringUtils.defaultString(rs.getString(14)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.ship_customer_name=new String(StringUtils.defaultString(rs.getString(15)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.bill_customer_no_=rs.getString(16);
            table.bill_customer_name=new String(StringUtils.defaultString(rs.getString(17)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.gui_no_=new String(StringUtils.defaultString(rs.getString(18)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.dim_1=rs.getString(19);
            table.dim_2=rs.getString(20);
            table.dim_2_name=new String(StringUtils.defaultString(rs.getString(21)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.line_no_=rs.getInt(22);
            table.order_no_=new String(StringUtils.defaultString(rs.getString(23)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.order_date=rs.getString(24);
            table.posting_group=rs.getString(25);
            table.cat_code=rs.getString(26);
            table.cat_code_not=rs.getString(27);
            table.vendor_no_=rs.getString(28);
            table.vendor_no_q=rs.getString(29);
            table.service_item_no_=new String(StringUtils.defaultString(rs.getString(30)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.ibs_description=new String(StringUtils.defaultString(rs.getString(31)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.service_item_serial_no_=new String(StringUtils.defaultString(rs.getString(32)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.item_no_  =new String(StringUtils.defaultString(rs.getString(33)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.item_desc =new String(StringUtils.defaultString(rs.getString(34)).getBytes(nav2009.getCharSet()), nav2009.getNewCharSet());
            table.line_type =rs.getInt(35);
            table.line_type_desc =rs.getInt(36);
            table.location_code= rs.getString(37);
            table.currency_code= rs.getString(38);
            table.currency_factor = rs.getBigDecimal(39);
            table.unit_cost = rs.getBigDecimal(40);
            table.quantity = rs.getBigDecimal(41);
            table.unit_price = rs.getBigDecimal(42);
            table.item_unit_price = rs.getBigDecimal(43);
            table.cost = rs.getBigDecimal(44);
            table.amount = rs.getBigDecimal(45);
            table.amount_lcy = rs.getBigDecimal(46);
            table.amount_vat = rs.getBigDecimal(47);
            table.vendor_group = rs.getString(48);
            table.vendor_group_kpi = rs.getString(49);
            table.posting_group_not = rs.getString(50);
            table.sales_price = rs.getBigDecimal(51);
            table.sales_price_vat = rs.getBigDecimal(52);
            table.cust_dim1=rs.getString(53);
            table.cust_price_group=rs.getString(54);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return table;
    }
}//end JDBCMssql