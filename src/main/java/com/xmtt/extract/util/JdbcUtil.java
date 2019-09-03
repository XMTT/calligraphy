package com.xmtt.extract.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class JdbcUtil {
	// private static final ThreadLocal<Connection> CONN_HOLDER = new
	// ThreadLocal<Connection>();

	private JdbcUtil() {
	}

	public  static void executeAutoCommit(Connection con, JdbcTask task)throws SQLException {
		boolean old = con.getAutoCommit();
		try{
			con.setAutoCommit(false);
			task.exec();
			con.commit();
		}catch(SQLException e){
			rollback(con);
			throw e;
		}finally{
			con.setAutoCommit(old);
		}
	}
	public  static void executeAutoClose(Connection con, JdbcTask task, boolean commit)throws SQLException {
		boolean old = con.getAutoCommit();
		try{
		if(commit) 	con.setAutoCommit(false);
			task.exec();
		if(commit)	con.commit();
		}catch(SQLException e){
			rollback(con);
			throw e;
		}finally{
			if(commit)con.setAutoCommit(old);
			close(con);
		}
	}


	public static void close(Connection con) {
		try {
			con.close();
		} catch (SQLException e) {
		}
	}

	public static void close(Statement st) {
		try {
			st.close();
		} catch (SQLException e) {
		}
	}

	public static void close(ResultSet rs) {
		try {
			rs.close();
		} catch (SQLException e) {
		}
	}

	public static void rollback(Connection con) {
		try {
			con.rollback();
		} catch (SQLException e) {
		}
	}

	public static int execute(Connection con, String sql) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			return ps.executeUpdate();
		} finally {
			close(ps);
		}
	}

	public static int execute(Connection con, String sql, PreparedStatementConfig config) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			return ps.executeUpdate();
		} finally {
			close(ps);
		}
	}

	public static int[] batchExecute(Connection con, String sql, BatchPreparedStatementConfig config)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			while (config.hasNext()) {
				config.config(ps);
				ps.addBatch();
			}
			return ps.executeBatch();
		} finally {
			close(ps);
		}
	}

	public static <T> T query(Connection con, String sql, ResultSetExtractor<T> rse) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				return rse.extractData(rs);
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static <T> T query(Connection con, String sql, ResultSetExtractor<T> rse, PreparedStatementConfig config)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				return rse.extractData(rs);
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static <T> List<T> queryList(Connection con, String sql, ResultSetExtractor<T> rse) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			List<T> result = new ArrayList<T>();
			try {
				while (rs.next()) {
					result.add(rse.extractData(rs));
				}
				return result;
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static <T> List<T> queryList(Connection con, String sql, ResultSetExtractor<T> rse,
                                        PreparedStatementConfig config) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			List<T> result = new ArrayList<T>();
			try {
				while (rs.next()) {
					result.add(rse.extractData(rs));
				}
				return result;
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static int queryInt(Connection con, String sql, int defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					int ret = rs.getInt(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static int queryInt(Connection con, String sql, int valWithNoFound, int valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					int ret = rs.getInt(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static byte queryByte(Connection con, String sql, byte defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					byte ret = rs.getByte(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static byte queryByte(Connection con, String sql, byte valWithNoFound, byte valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					byte ret = rs.getByte(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static short queryShort(Connection con, String sql, short defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					short ret = rs.getShort(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static short queryShort(Connection con, String sql, short valWithNoFound, short valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					short ret = rs.getShort(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static float queryFloat(Connection con, String sql, float defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					float ret = rs.getFloat(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static float queryFloat(Connection con, String sql, float valWithNoFound, float valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					float ret = rs.getFloat(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static double queryDouble(Connection con, String sql, double defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					double ret = rs.getDouble(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static double queryDouble(Connection con, String sql, double valWithNoFound, double valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					double ret = rs.getDouble(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static String queryString(Connection con, String sql, String defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					String ret = rs.getString(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static String queryString(Connection con, String sql, String valWithNoFound, String valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					String ret = rs.getString(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static int queryInt(Connection con, String sql, PreparedStatementConfig config, int defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					int ret = rs.getInt(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static int queryInt(Connection con, String sql, PreparedStatementConfig config, int valWithNoFound,
                               int valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					int ret = rs.getInt(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static byte queryByte(Connection con, String sql, PreparedStatementConfig config, byte defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					byte ret = rs.getByte(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static byte queryByte(Connection con, String sql, PreparedStatementConfig config, byte valWithNoFound,
                                 byte valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					byte ret = rs.getByte(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static long queryLong(Connection con, String sql, PreparedStatementConfig config, long defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					long ret = rs.getLong(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static long queryLong(Connection con, String sql, PreparedStatementConfig config, long valWithNoFound,
                                 long valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					long ret = rs.getLong(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static long queryLong(Connection con, String sql, long defaultValue) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					long ret = rs.getLong(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static long queryLong(Connection con, String sql, long valWithNoFound, long valWithNull)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					long ret = rs.getLong(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static short queryShort(Connection con, String sql, PreparedStatementConfig config, short defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					short ret = rs.getShort(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static short queryShort(Connection con, String sql, PreparedStatementConfig config, short valWithNoFound,
                                   short valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					short ret = rs.getShort(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static float queryFloat(Connection con, String sql, PreparedStatementConfig config, float defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					float ret = rs.getFloat(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static float queryfloat(Connection con, String sql, PreparedStatementConfig config, float valWithNoFound,
                                   float valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					float ret = rs.getFloat(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static double queryDouble(Connection con, String sql, PreparedStatementConfig config, double defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					double ret = rs.getDouble(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static double queryDouble(Connection con, String sql, PreparedStatementConfig config, double valWithNoFound,
                                     double valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					double ret = rs.getDouble(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static String queryString(Connection con, String sql, PreparedStatementConfig config, String defaultValue)
			throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					String ret = rs.getString(1);
					if (rs.wasNull())
						return defaultValue;
					return ret;
				} else {
					return defaultValue;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static String queryString(Connection con, String sql, PreparedStatementConfig config, String valWithNoFound,
                                     String valWithNull) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					String ret = rs.getString(1);
					if (rs.wasNull())
						return valWithNull;
					return ret;
				} else {
					return valWithNoFound;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static Map<String, Object> queryMap(Connection con, String sql) throws SQLException {
		PreparedStatement ps = con.prepareStatement(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {

					ResultSetMetaData rsmd = rs.getMetaData();

					int columnCount = rsmd.getColumnCount();
					Map<String, Object> ret = new LinkedHashMap<String, Object>(columnCount);
					for (int i = 1; i <= columnCount; i++) {
						ret.put(lookupColumnName(rsmd, i), getResultSetValue(rs, i));
					}
					return ret;
				} else {
					return null;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}

	public static Map<String, Object> queryMap(Connection con, String sql, PreparedStatementConfig config)
			throws SQLException {
		PreparedStatement ps = con.prepareStatement(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();
					Map<String, Object> ret = new LinkedHashMap<String, Object>(columnCount);
					for (int i = 1; i <= columnCount; i++) {
						ret.put(lookupColumnName(rsmd, i), getResultSetValue(rs, i));
					}
					return ret;
				} else {
					return null;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}
	}

	public static List<Map<String, Object>> queryMaps(Connection con, String sql) throws SQLException {
		PreparedStatement ps = con.prepareStatement(sql);
		try {
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();

					List<Map<String, Object>> rets = new LinkedList<Map<String, Object>>();
					++columnCount;
					String[] keys = new String[columnCount];
					for (int i = 1; i < columnCount; ++i) {
						keys[i] = lookupColumnName(rsmd, i);
					}
					do {
						Map<String, Object> ret = new LinkedHashMap<String, Object>(columnCount);
						for (int i = 1; i < columnCount; i++) {
							ret.put(keys[i], getResultSetValue(rs, i));
						}
						rets.add(ret);
					} while (rs.next());
					return rets;
				} else {
					return Collections.EMPTY_LIST;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}

	public static List<Map<String, Object>> queryMaps(Connection con, String sql, PreparedStatementConfig config)
			throws SQLException {
		PreparedStatement ps = con.prepareStatement(sql);
		try {
			config.config(ps);
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();

					List<Map<String, Object>> rets = new LinkedList<Map<String, Object>>();
					++columnCount;
					String[] keys = new String[columnCount];
					for (int i = 1; i < columnCount; ++i) {
						keys[i] = lookupColumnName(rsmd, i);
					}
					do {
						Map<String, Object> ret = new LinkedHashMap<String, Object>(columnCount);
						for (int i = 1; i < columnCount; i++) {
							ret.put(keys[i], getResultSetValue(rs, i));
						}
						rets.add(ret);
					} while (rs.next());
					return rets;
				} else {
					return Collections.EMPTY_LIST;
				}
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}



	public static <T> T queryObject(Connection con, String sql, Class<T> clazz) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			T ret = null;
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					try {
						ResultSetMetaData rsmd = rs.getMetaData();
						Method[] methods = new Method[rsmd.getColumnCount() + 1];
						Method[] dms = clazz.getMethods();
						for (int i = 1; i <= methods.length; ++i) {
							String caption = lookupColumnName(rsmd, i);
							methods[i] = match(caption, dms);
						}
						ret = (T) clazz.newInstance();
						for(int i = 1 ;i < methods.length ; ++i){
							if(null!=methods[i]) methods[i].invoke(ret,rs.getObject(i));
						}
						return ret;
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return null;

			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}
	public static <T> List<T> queryObjects(Connection con, String sql, Class<T> clazz) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			List<T> rets = new ArrayList<T>();
			T ret = null;
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next()) {
					try {
						ResultSetMetaData rsmd = rs.getMetaData();
						Method[] methods = new Method[rsmd.getColumnCount() + 1];
						Method[] dms = clazz.getMethods();
						for (int i = 1; i <= methods.length; ++i) {
							String caption = lookupColumnName(rsmd, i);
							methods[i] = match(caption, dms);
						}
						ret = (T) clazz.newInstance();
						for(int i = 1 ;i < methods.length ; ++i){
							if(null!=methods[i]) methods[i].invoke(ret,rs.getObject(i));
						}
						rets.add(ret);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return rets;
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}
	public static <T> T queryObject(Connection con, String sql, PreparedStatementConfig config, Class<T> clazz) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			T ret = null;
			ResultSet rs = ps.executeQuery();
			try {
				if (rs.next()) {
					try {
						ResultSetMetaData rsmd = rs.getMetaData();
						Method[] methods = new Method[rsmd.getColumnCount() + 1];
						Method[] dms = clazz.getMethods();
						for (int i = 1; i <= methods.length; ++i) {
							String caption = lookupColumnName(rsmd, i);
							methods[i] = match(caption, dms);
						}
						ret = (T) clazz.newInstance();
						for(int i = 1 ;i < methods.length ; ++i){
							if(null!=methods[i]) methods[i].invoke(ret,rs.getObject(i));
						}
						return ret;
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return null;

			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}
	public static <T> List<T> queryObjects(Connection con, String sql, PreparedStatementConfig config, Class<T> clazz) throws SQLException {
		PreparedStatement ps = con.prepareCall(sql);
		try {
			config.config(ps);
			List<T> rets = new ArrayList<T>();
			T ret = null;
			ResultSet rs = ps.executeQuery();
			try {
				while (rs.next()) {
					try {
						ResultSetMetaData rsmd = rs.getMetaData();
						Method[] methods = new Method[rsmd.getColumnCount() + 1];
						Method[] dms = clazz.getMethods();
						for (int i = 1; i <= methods.length; ++i) {
							String caption = lookupColumnName(rsmd, i);
							methods[i] = match(caption, dms);
						}
						ret = (T) clazz.newInstance();
						for(int i = 1 ;i < methods.length ; ++i){
							if(null!=methods[i]) methods[i].invoke(ret,rs.getObject(i));
						}
						rets.add(ret);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return rets;
			} finally {
				close(rs);
			}
		} finally {
			close(ps);
		}

	}

	public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
		Object obj = rs.getObject(index);
		String className = null;
		if (obj != null) {
			className = obj.getClass().getName();
		}
		if (className.equals("[B")) {
			obj = rs.getBinaryStream(index);
		}else if (obj instanceof Blob) {
			obj = rs.getBytes(index);
		} else if (obj instanceof Clob) {
			obj = rs.getString(index);
		} else if ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ".equals(className)) {
			obj = rs.getTimestamp(index);
		} else if (className != null && className.startsWith("oracle.sql.DATE")) {
			String metaDataClassName = rs.getMetaData().getColumnClassName(index);
			if ("java.sql.Timestamp".equals(metaDataClassName) || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
				obj = rs.getTimestamp(index);
			} else {
				obj = rs.getDate(index);
			}
		} else if (obj != null && obj instanceof java.sql.Date) {
			if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(index))) {
				obj = rs.getTimestamp(index);
			}
		}
		return obj;
	}

	public static String lookupColumnName(ResultSetMetaData resultSetMetaData, int columnIndex) throws SQLException {
		String name = resultSetMetaData.getColumnLabel(columnIndex);
		if (name == null || name.length() < 1) {
			name = resultSetMetaData.getColumnName(columnIndex);
		}
		return name;
	}

	private static Method match(String mn, Method[] methods) {
		if (mn.length() == 1)
			mn = "set" + mn.toUpperCase(Locale.US);
		else
			mn = "set" + Character.toUpperCase(mn.charAt(0)) + mn.substring(1);
		for (Method method : methods) {
			if (method.getName().equals(mn) && method.getParameterTypes().length == 1
					&& !Modifier.isStatic(method.getModifiers())) {
				return method;
			}
		}
		return null;
	}

}
