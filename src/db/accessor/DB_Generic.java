package db.accessor;

/*
 * @(#)DB_Generic.java	1.52 5/3/2006
 * Author: Jim Lombardo, WCTC Lead Java Instructor
 *
 * Copyright (c) 2006-2010 Waukesha County Technical College.
 * All Rights Reserved.
 *
 * THE ACCOMPANYING PROGRAM IS PROVIDED UNDER THE TERMS OF
 * THE ACADEMIC FREE LICENSE V2.0 ("AGREEMENT"). ANY USE, REPRODUCTION
 * OR DISTRIBUTION OF THE PROGRAM CONSTITUTES RECIPIENT'S ACCEPTANCE
 * OF THIS AGREEMENT. A COPY OF THE AGREEMENT MUST BE ATTACHED TO ANY
 * AND ALL ORIGINALS OR DERIVITIVES.
 */

import java.util.*;
import java.sql.*;

/**
 * <code>DB_Generic</code> is a general purpose class providing services 
 * for querying, inserting, updating and deleting records in most ANSI-compliant
 * databases. It has been successfully tested on MS-SQL Server 7 and 2000.
 * <p>
 * Use this class when you need to use JDBC version 2 and typical,
 * ANSI-standard SQL statements and do not need to call stored procedures.
 * If your sql statements are standard, this class could also be used with
 * other databases. However, when database specific sql is needed, such as with 
 * date criteria, developers should create db-specific classes that implement
 * the DBAccessor interface.
 * <p>
 * @author 	Jim Lombardo
 * @version 	1.6 5/3/2010
 */
public class DB_Generic implements DBAccessor {
	private Connection conn;

	/**
	 * Default constructor should be used when you do not want to
	 * immediately open a connection to the database. When the
	 * connection is needed, call the openConnection method.
	 */
	public DB_Generic() {}
	
	
	/**
	 * A utility method to expicitly open a db connection. Note that the
	 * connection will remain open until explicitly closed by member methods.
	 * 
	 * @param driverClassName - the fully qualified name of the driver class.
	 * @param url - the connection URL, driver dependent.
	 * @param username for database access permission, if required. Null and "" values
	 * are allowed.
	 * @param password for database access persmission, if required. Null and "" values
	 * are allowed
	 * @throws IllegalArgumentException if url is null or zero length
	 * @throws ClassNotFoundException if driver class cannot be found
	 * @throws SQLException if database access error occurs. For example, an invalid
	 * url could cause this; or, a database that is no longer available due to network
	 * or access permission problems.
	 */
	public void openConnection(String driverClassName, String url, String username, String password) 
	throws IllegalArgumentException, ClassNotFoundException, SQLException
	{
		String msg = "Error: url is null or zero length!";
		if( url == null || url.length() == 0 ) throw new IllegalArgumentException(msg);
		username = (username == null) ? "" : username;
		password = (password == null) ? "" : password;
		Class.forName (driverClassName);
		conn = DriverManager.getConnection(url, username, password);
	}
		
	/**
	 * A utility method to expicitly close a db connection. Pooled connections
	 * should never be closed, but rather returned to the pool.
	 * <p>
	 * As an alternative to using this method, other member methods is this
	 * class offer a boolean switch to close the connection automatically.
	 * 
	 * @throws SQLException if connection cannot be closed due to a db access error.
	 */
	public void closeConnection() throws SQLException {
		conn.close();
	}

	/**
	 * Can be used to perform general purpose sql queries. This is especially 
	 * useful when other methods in the public interface don't provide the 
	 * needed features (such as complex, multiple criteria statements).
	 * 
	 * @param sqlString - the sql statement (check your database for compatibility)
	 * @param closeConnection - true if connection should be closed automatically; if
	 * false, connection must be explicitly closed using the closeConnection method.
	 * @return The found records as a List of Maps. Each Map is one record, with
	 * column name as the key, and the column value as the value. The List will be
	 * null if the query fails to return any records.
	 * @throws SQLException if database access error or illegal sql
	 * @throws Exception for all other problems
	 */
	public List findRecords(String sqlString, boolean closeConnection) 
	throws SQLException, Exception
	{
		Statement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData metaData = null;
		final List list = new ArrayList();
		Map record = null;

		// do this in an excpetion handler so that we can depend on the
		// finally clause to close the connection
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sqlString);
			metaData = rs.getMetaData();
			final int fields = metaData.getColumnCount();

			while( rs.next() ) {
				record = new HashMap();
				for( int i=1; i <= fields; i++ ) {
					try {
						record.put( metaData.getColumnName(i), rs.getObject(i) );
					} catch(NullPointerException npe) { 
						// no need to do anything... if it fails, just ignore it and continue
					}
				} // end for
				list.add(record);
			} // end while

		} catch (SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				stmt.close();
				if(closeConnection) conn.close();
			} catch(SQLException e) {
				throw e;
			} // end try
		} // end finally

		return list; // will  be null if none found
	}

	/**
	 * Retrieves a record based on the primary key of a table.
	 * 
	 * @param table - a <code>String</code> representing the table name.
	 * @param primaryKeyField - a <code>String</code> representing the primary key field
	 * name used as the search criteria.
	 * @param keyValue - an <code>Object</code> representing the primary key field value
	 * used for the search criteria. Typically this is a String or numeric typewrapper class.
	 * @param closeConnection - true if connection should be closed automatically; if
	 * false, connection must be explicitly closed using the closeConnection method.
	 * @return a <code>Map</code> if the record is found; <code>null</code> otherwise. The key
	 * is the columnName, the value is the field data.
	 * @throws SQLException if database access error or illegal sql
	 * @throws Exception for all other problems
	 */
	public Map getRecordByID(String table, String primaryKeyField, Object keyValue, boolean closeConnection)
	throws SQLException, Exception
	{
		Statement stmt = null;
		ResultSet rs = null;
		ResultSetMetaData metaData = null;
		final Map record=new HashMap();

		// do this in an excpetion handler so that we can depend on the
		// finally clause to close the connection
		try {
			stmt = conn.createStatement();
			String sql2;

			if(keyValue instanceof String){
				sql2 = "= '" + keyValue + "'";}
			else {
				sql2 = "=" + keyValue;}

			final String sql="SELECT * FROM " + table + " WHERE " + primaryKeyField + sql2;
			rs = stmt.executeQuery(sql);
			metaData = rs.getMetaData();
			metaData.getColumnCount();
			final int fields=metaData.getColumnCount();

			// Retrieve the raw data from the ResultSet and copy the values into a Map
			// with the keys being the column names of the table.
			if(rs.next() ) {
				for( int i=1; i <= fields; i++ ) {
					record.put( metaData.getColumnName(i), rs.getObject(i) );
				}
			}

		} catch (SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				stmt.close();
				if(closeConnection) conn.close();
			} catch(SQLException e) {
				throw e;
			} // end try
		} // end finally

		return record;
	}

	/**
	 * Inserts a record into a table based on a <code>List</code> of column descriptors
	 * and a one-to-one mapping of an associated <code>List</code> of column values.
	 * 
	 * @param tableName - a <code>String</code> representing the table name
	 * @param colDescriptors - <code>List</code> containing the column descriptors
	 * @param colValues - <code>List</code> containing the column values. The order of
	 * these values must match the order of the column descriptors.
	 * @param closeConnection - true if connection should be closed automatically; if
	 * false, connection must be explicitly closed using the closeConnection method.
	 * @return <code>true</code> if successfull; <code>false</code> otherwise
	 * @throws SQLException if database access error or illegal sql
	 * @throws Exception for all other problems
	 */
	public boolean insertRecord(String tableName, List colDescriptors, List colValues, boolean closeConnection)
	throws SQLException, Exception
	{
		PreparedStatement pstmt = null;
		int recsUpdated = 0;

		// do this in an excpetion handler so that we can depend on the
		// finally clause to close the connection
		try {
			pstmt = buildInsertStatement(conn,tableName,colDescriptors);

			final Iterator i=colValues.iterator();
			int index = 1;
			while( i.hasNext() ) {
				final Object obj=i.next();
				if(obj instanceof String){
					pstmt.setString( index++,(String)obj );
				} else if(obj instanceof Integer ){
					pstmt.setInt( index++,((Integer)obj).intValue() );
				} else if(obj instanceof Long ){
					pstmt.setLong( index++,((Long)obj).longValue() );
				} else if(obj instanceof Double ){
					pstmt.setDouble( index++,((Double)obj).doubleValue() );
				} else if(obj instanceof java.sql.Date ){
					pstmt.setDate(index++, (java.sql.Date)obj );
				} else if(obj instanceof Boolean ){
					pstmt.setBoolean(index++, ((Boolean)obj).booleanValue() );
				} else {
					if(obj != null) pstmt.setObject(index++, obj);
				}
			}
			recsUpdated = pstmt.executeUpdate();

		} catch (SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				pstmt.close();
				if(closeConnection) conn.close();
			} catch(SQLException e) {
				throw e;
			} // end try
		} // end finally

		if(recsUpdated == 1){
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Updates one or more records in a table based on a single, matching field value.
	 * 
	 * @param tableName - a <code>String</code> representing the table name
	 * @param colDescriptors - a <code>List</code> containing the column descriptors for
	 * the fields that can be updated.
	 * @param colValues - a <code>List</code> containing the values for the fields that
	 * can be updated.
	 * @param whereField - a <code>String</code> representing the field name for the
	 * search criteria.
	 * @param whereValue - an <code>Object</code> containing the value for the search criteria.
	 * @param closeConnection - true if connection should be closed automatically; if
	 * false, connection must be explicitly closed using the closeConnection method.
	 * @return an <code>int</code> containing the number of records updated.
	 * @throws SQLException if database access error or illegal sql
	 * @throws Exception for all other problems
	 */
	public int updateRecords(String tableName, List colDescriptors, List colValues,
							 String whereField, Object whereValue, boolean closeConnection)
							 throws SQLException, Exception
	{
		PreparedStatement pstmt = null;
		int recsUpdated = 0;

		// do this in an excpetion handler so that we can depend on the
		// finally clause to close the connection
		try {
			pstmt = buildUpdateStatement(conn,tableName,colDescriptors,whereField);

			final Iterator i=colValues.iterator();
			int index = 1;
			boolean doWhereValueFlag = false;
			Object obj = null;

			while( i.hasNext() || doWhereValueFlag) {
				if(!doWhereValueFlag){ obj = i.next();}

				if(obj instanceof String){
					pstmt.setString( index++,(String)obj );
				} else if(obj instanceof Integer ){
					pstmt.setInt( index++,((Integer)obj).intValue() );
				} else if(obj instanceof Long ){
					pstmt.setLong( index++,((Long)obj).longValue() );
				} else if(obj instanceof Double ){
					pstmt.setDouble( index++,((Double)obj).doubleValue() );
				} else if(obj instanceof java.sql.Timestamp ){
					pstmt.setTimestamp(index++, (java.sql.Timestamp)obj );
				} else if(obj instanceof java.sql.Date ){
					pstmt.setDate(index++, (java.sql.Date)obj );
				} else if(obj instanceof Boolean ){
					pstmt.setBoolean(index++, ((Boolean)obj).booleanValue() );
				} else {
					if(obj != null) pstmt.setObject(index++, obj);
				}

				if(doWhereValueFlag){ break;} // only allow loop to continue one time
				if(!i.hasNext() ) {          // continue loop for whereValue
					doWhereValueFlag = true;
					obj = whereValue;
				}
			}

			recsUpdated = pstmt.executeUpdate();

		} catch (SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				pstmt.close();
				if(closeConnection) conn.close();
			} catch(SQLException e) {
				throw e;
			} // end try
		} // end finally

		return recsUpdated;
	}

	/**
	 * Deletes one or more records in a table based on a single, matching field value.
	 * 
	 * @param tableName - a <code>String</code> representing the table name
	 * @param whereField - a <code>String</code> representing the field name for the
	 * search criteria.
	 * @param whereValue - an <code>Object</code> containing the value for the search criteria.
	 * @param closeConnection - true if connection should be closed automatically; if
	 * false, connection must be explicitly closed using the closeConnection method.
	 * @return an <code>int</code> containing the number of records updated.
	 * @throws SQLException if database access error or illegal sql
	 * @throws Exception for all other problems
	 */
	public int deleteRecords(String tableName, String whereField, Object whereValue, boolean closeConnection)
	throws SQLException, Exception
	{
		PreparedStatement pstmt = null;
		int recsDeleted = 0;

		// do this in an excpetion handler so that we can depend on the
		// finally clause to close the connection
		try {
			pstmt = buildDeleteStatement(conn,tableName,whereField);

			// delete all records if whereField is null
			if(whereField != null ) {
				if(whereValue instanceof String){
					pstmt.setString( 1,(String)whereValue );
				} else if(whereValue instanceof Integer ){
					pstmt.setInt( 1,((Integer)whereValue).intValue() );
				} else if(whereValue instanceof Long ){
					pstmt.setLong( 1,((Long)whereValue).longValue() );
				} else if(whereValue instanceof Double ){
					pstmt.setDouble( 1,((Double)whereValue).doubleValue() );
				} else if(whereValue instanceof java.sql.Date ){
					pstmt.setDate(1, (java.sql.Date)whereValue );
				} else if(whereValue instanceof Boolean ){
					pstmt.setBoolean(1, ((Boolean)whereValue).booleanValue() );
				} else {
					if(whereValue != null) pstmt.setObject(1, whereValue);
				}
			}

			recsDeleted = pstmt.executeUpdate();

		} catch (SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				pstmt.close();
				if(closeConnection) conn.close();
			} catch(SQLException e) {
				throw e;
			} // end try
		} // end finally

		return recsDeleted;
	}

	/*
	 * Builds a java.sql.PreparedStatement for an sql insert
	 * @param conn - a valid connection
	 * @param tableName - a <code>String</code> representing the table name
	 * @param colDescriptors - a <code>List</code> containing the column descriptors for
	 * the fields that can be inserted.
	 * @return java.sql.PreparedStatement
	 * @throws SQLException
	 */
	private PreparedStatement buildInsertStatement(Connection conn_loc, String tableName, List colDescriptors)
	throws SQLException {
		StringBuffer sql = new StringBuffer("INSERT INTO ");
		(sql.append(tableName)).append(" (");
		final Iterator i=colDescriptors.iterator();
		while( i.hasNext() ) {
			(sql.append( (String)i.next() )).append(", ");
		}
		sql = new StringBuffer( (sql.toString()).substring( 0,(sql.toString()).lastIndexOf(", ") ) + ") VALUES (" );
		for( int j = 0; j < colDescriptors.size(); j++ ) {
			sql.append("?, ");
		}
		final String finalSQL=(sql.toString()).substring(0,(sql.toString()).lastIndexOf(", ")) + ")";
		//System.out.println(finalSQL);
		return conn_loc.prepareStatement(finalSQL);
	}

	/*
	 * Builds a java.sql.PreparedStatement for an sql update using only one where clause test
	 * @param conn - a JDBC <code>Connection</code> object
	 * @param tableName - a <code>String</code> representing the table name
	 * @param colDescriptors - a <code>List</code> containing the column descriptors for
	 * the fields that can be updated.
	 * @param whereField - a <code>String</code> representing the field name for the
	 * search criteria.
	 * @return java.sql.PreparedStatement
	 * @throws SQLException
	 */
	private PreparedStatement buildUpdateStatement(Connection conn_loc, String tableName,
												   List colDescriptors, String whereField)
	throws SQLException {
		StringBuffer sql = new StringBuffer("UPDATE ");
		(sql.append(tableName)).append(" SET ");
		final Iterator i=colDescriptors.iterator();
		while( i.hasNext() ) {
			(sql.append( (String)i.next() )).append(" = ?, ");
		}
		sql = new StringBuffer( (sql.toString()).substring( 0,(sql.toString()).lastIndexOf(", ") ) );
		((sql.append(" WHERE ")).append(whereField)).append(" = ?");
		final String finalSQL=sql.toString();
		return conn_loc.prepareStatement(finalSQL);
	}

	/*
	 * Builds a java.sql.PreparedStatement for an sql delete using only one where clause test
	 * @param conn - a JDBC <code>Connection</code> object
	 * @param tableName - a <code>String</code> representing the table name
	 * @param whereField - a <code>String</code> representing the field name for the
	 * search criteria.
	 * @return java.sql.PreparedStatement
	 * @throws SQLException
	 */
	private PreparedStatement buildDeleteStatement(Connection conn_loc, String tableName, String whereField)
	throws SQLException {
		final StringBuffer sql=new StringBuffer("DELETE FROM ");
		sql.append(tableName);

		// delete all records if whereField is null
		if(whereField != null ) {
			sql.append(" WHERE ");
			(sql.append( whereField )).append(" = ?");
		}

		final String finalSQL=sql.toString();
//		System.out.println(finalSQL);
		return conn_loc.prepareStatement(finalSQL);
	}
        
        public static void main(String[] args) throws Exception {
            DB_Generic db = new DB_Generic();
            db.openConnection("com.microsoft.sqlserver.jdbc.SQLServerDriver", 
                    "jdbc:sqlserver://bitsql.wctc.edu:1433;databaseName=JGL-ParkingGarage", 
                    "advjava", "advjava");
            
            List records = db.findRecords("select total_hours, total_fees from RunTotals", true);
            System.out.println(records);
        }


} // end class