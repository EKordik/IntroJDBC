package booksample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author emmakordik
 */
public class MySqlDb {
    private Connection conn;
    
    public void openConnection(String driverClass, String url, String userName, 
            String password) throws ClassNotFoundException, SQLException{
        Class.forName(driverClass);
        conn = DriverManager.getConnection(url, userName, password);
    }
    
    public void closeConnection() throws SQLException{
        conn.close();
    }
    
    public List<Map<String, Object>> getAllRecords(String tableName) throws SQLException{
        List<Map<String,Object>> records = new ArrayList<>();
        
        String sql = "SELECT * From " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()){
            Map<String,Object> record = new HashMap<>();
            for(int i = 1; i<=columnCount; i++){
                record.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(record);
        }
        
        
        return records;
    }
    
//    public void deleteByID(String tableName, String primaryKeyColumnName, 
//            Object primaryKeyValue) throws SQLException{
//        String sql = "DELETE FROM " + tableName + " WHERE " + 
//                primaryKeyColumnName + "=";
//        
//        if(primaryKeyValue instanceof String){
//             sql += "'" + primaryKeyValue + "'";
//        }else{
//            sql += primaryKeyValue;
//        }
//        
//        Statement stmt = conn.createStatement();
//        int columnsDeleted = stmt.executeUpdate(sql);
//    }
    
    public void deleteByID(String tableName, String primaryKeyColName, 
            Object primaryKeyValue) throws SQLException{
        PreparedStatement deleteStmt = null;
        
        String sql = "DELETE FROM " + tableName + " WHERE " + primaryKeyColName + "=?";
        
        deleteStmt = conn.prepareStatement(sql);
        
        if(primaryKeyValue instanceof String){
            deleteStmt.setString(1, (String)primaryKeyValue);
        }else{
            deleteStmt.setInt(1, (Integer)primaryKeyValue);
        }
        
        int columnsDeleted = deleteStmt.executeUpdate();
        deleteStmt.close();
        

    }
    
    public void insertRecord(String tableName, List<String> colNames, 
            List<Object> colValues)throws SQLException{
        String sql = "INSERT INTO " + tableName + " (";
        
        for(int i = 0; i< colNames.size(); i++){
            sql+= colNames.get(i) + ", ";
        }
        sql = sql.substring(0, sql.length()-2);
        sql += ") VALUES(";
        
        for(int i = 0; i<colValues.size(); i++){
            if(colValues.get(i) instanceof String){
                sql += "'" + colValues.get(i) + "', ";
            }else{
                sql += colValues.get(i) + ", ";
            }
        }
        
        sql = sql.substring(0, sql.length()-2);
        sql += ")";
        
                System.out.println(sql);

        Statement stmt = conn.createStatement();
        int insertedColumns = stmt.executeUpdate(sql);
        

    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        MySqlDb db = new MySqlDb();
        String driverClass = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/book";
        String userName = "root";
        String password = "admin";
        List<Map<String,Object>>records = new ArrayList<>();
        
        db.openConnection(driverClass, url, userName, password);
        
        List<String> colNames = new ArrayList<>();
        colNames.add("author_name");
        colNames.add("date_created");
        
        List<Object> colValues = new ArrayList<>();
        colValues.add("Mark Twain");
        Date created = new Date();
        SimpleDateFormat stf = new SimpleDateFormat("yyyy-MM-dd");

        colValues.add(stf.format(created));

        try{
            //db.deleteByID("author", "author_id", 6);
            db.insertRecord("author", colNames, colValues);
            records = db.getAllRecords("author");
        }finally{
            db.closeConnection();
        }
        
        for(Map record: records){
            System.out.println(record);
        }
    }
}
