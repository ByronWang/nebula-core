package nebula.data.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nebula.data.SmartList;
import nebula.lang.Type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;

/**
 * 单表持久化
 * 
 * @author wanglocal
 * 
 * @param <T>
 */
class DbDefaultPersister<T> implements DbPersister<T> {
	private final Log log;

	final protected DbConfiguration dbconfig;
	final private DbSqlHelper helper;

	final BO2DBSerializer<T> serializer;

	final private String SQL_GET;
	final private String SQL_INSERT;
	final private String SQL_UPDATE;
	final private String SQL_DELETE;
	final private String SQL_LIST;
	final private String SQL_MAX_ID;

	public DbDefaultPersister(DbConfiguration dbconfig, Type type, DbSqlHelper helper, BO2DBSerializer<T> serializer) {
		super();
		this.dbconfig = dbconfig;
		this.helper = helper;
		this.serializer = serializer;

		log = LogFactory.getLog(DbDefaultPersister.class + "_" + type.getName());

		SQL_GET = helper.builderGet();
		SQL_INSERT = helper.builderInsert();
		SQL_UPDATE = helper.builderUpdate();
		SQL_DELETE = helper.builderDelete();
		SQL_LIST = helper.builderList();
		SQL_MAX_ID = helper.builderMaxId();

		if (!type.getAttrs().containsKey(Type.LEGACY)) {
			this.ensureDBSchema();
		}
	}

	private void ensureDBSchema() {
		Statement statement = null;
		boolean exist = false;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = dbconfig.openConnection();

			final SmartList<String, DbColumn> mapColumns = new SmartList<String, DbColumn>(new Function<DbColumn, String>() {
				@Override
				public String apply(DbColumn data) {
					return data.columnName;
				}
			});

			DbColumn[] userColumns = helper.getUserColumns();
			for (int i = 0; i < userColumns.length; i++) {
				mapColumns.add(userColumns[i]);
			}

			statement = conn.createStatement();
			try {
				rs = statement.executeQuery(helper.builderGetMeta());
				exist = true;
			} catch (SQLSyntaxErrorException e) {
				// don't exist
			}

			if (exist) {

				boolean isKeyChanged = false;

				ResultSetMetaData metaData = rs.getMetaData();
				int columnsSize = metaData.getColumnCount();

				if (log.isTraceEnabled()) log.trace("\tcheck columns define");

				ArrayList<String> needDeletedColumns = new ArrayList<String>();
				Map<String, String> allColumns = new HashMap<String, String>();
				ArrayList<DbColumn> typeNotMatchColumns = new ArrayList<DbColumn>();
				ArrayList<DbColumn> typeSizeNotMatchColumns = new ArrayList<DbColumn>();

				String curKeys = "";

				for (int i = 1; i <= columnsSize; i++) {
					String columnName = metaData.getColumnName(i);

					DbColumn newColumn = mapColumns.get(columnName);
					if (newColumn == null) {
						if (log.isTraceEnabled())
							log.trace("\t\t" + metaData.getColumnName(i) + "\t column type not exist in type or is system column \t- "
									+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
						if (!columnName.endsWith("_")) {
							needDeletedColumns.add(columnName);
						}
					} else {
						int size = metaData.getColumnDisplaySize(i);
						int nullable = metaData.isNullable(i);
						int precision = metaData.getPrecision(i);
						int scale = metaData.getScale(i);

						int jdbcType = metaData.getColumnType(i);

						if (jdbcType != newColumn.jdbcType) {
							if (log.isTraceEnabled())
								log.trace("\t\t" + metaData.getColumnName(i) + "\t column type not match,need update \t- " + metaData.getColumnTypeName(i)
										+ "\t" + metaData.getColumnDisplaySize(i));
							typeNotMatchColumns.add(newColumn);

						} else {
							switch (jdbcType) {
							case Types.NUMERIC:
								if (newColumn.precision == precision && newColumn.scale == scale) {

								} else if (newColumn.precision > precision && newColumn.scale > scale) {

									if (log.isTraceEnabled())
										log.trace("\t" + metaData.getColumnName(i) + "\t\t column size not match,need update \t- "
												+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
									typeSizeNotMatchColumns.add(newColumn);
								} else if (newColumn.precision > precision || newColumn.scale > scale) {
									int newPrecision = newColumn.precision > precision ? newColumn.precision : precision;
									int newScale = newColumn.scale > scale ? newColumn.scale : scale;
									typeSizeNotMatchColumns.add(helper.makeColumn(newColumn.fieldName, newColumn.columnName, newColumn.key, newColumn.nullable,
											false, newColumn.rawType, newColumn.size, newPrecision, newScale));

									if (log.isTraceEnabled())
										log.trace("\t" + metaData.getColumnName(i) + "\t\tcolumn precision not match,need update \t- "
												+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
								}
								break;
							case Types.VARCHAR:
								if (newColumn.size > size) typeSizeNotMatchColumns.add(newColumn);
								break;
							}
						}

						/*
						 * if (nullable == ResultSetMetaData.columnNoNulls) { curKeys += columnName; if (!newColumn.key) { isKeyChanged = true; } }
						 */

						allColumns.put(columnName, columnName);
					}

				}
				rs.close();
				conn.commit();

				// 获得主键的信息
				rs = conn.getMetaData()
						.getPrimaryKeys(metaData.getCatalogName(1), metaData.getSchemaName(1), metaData.getTableName(1));
				while (rs.next()) {
					String primaryKey = rs.getString("COLUMN_NAME");
					curKeys += primaryKey;
					DbColumn newColumn = mapColumns.get(primaryKey);
					if (newColumn == null || !newColumn.key) {
						isKeyChanged = true;
					}
				}

				if (log.isTraceEnabled()) log.trace("\tcheck columns define finished");

				if (log.isTraceEnabled()) log.trace("\tcheck key define");
				StringBuffer newKeys = new StringBuffer();
				ArrayList<DbColumn> notExistColumns = new ArrayList<DbColumn>();
				for (DbColumn f : mapColumns) {
					if (!allColumns.containsKey(f.columnName)) {
						notExistColumns.add(f);
					}
					if (f.key) {
						if (curKeys.indexOf(f.columnName) < 0) {
							isKeyChanged = true;
						}
						if (newKeys.length() > 0) {
							newKeys.append(",");
						}
						newKeys.append(f.columnName);
					}
				}
				if (log.isTraceEnabled()) log.trace("\tcheck key define finished");

				// key changed
				if (isKeyChanged) {
					statement = conn.createStatement();
					if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] key changed,reconstruct ");
					statement.addBatch(helper.builderDrop());
					statement.addBatch(helper.builderCreate());
					statement.executeBatch();
					log.trace("\t[" + helper.tableName + "] reconstruct finished table  finished");

					if (log.isTraceEnabled()) {
						if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] table layout after add not exist column");
						rs = statement.executeQuery(helper.builderGetMeta());
						metaData = rs.getMetaData();
						columnsSize = metaData.getColumnCount();
						for (int i = 1; i <= columnsSize; i++) {
							log.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
						}
						rs.close();
					}
				} else {
					// add not exist column to DB
					if (notExistColumns.size() > 0) {
						if (log.isTraceEnabled()) log.trace("\tadd not exist column");
						statement = conn.createStatement();
						for (DbColumn c : notExistColumns) {
							log.trace("\t\t add column - " + helper.builderAddColumn(c));
							statement.addBatch(helper.builderAddColumn(c));
						}
						statement.executeBatch();

						if (log.isTraceEnabled()) log.trace("\tadd not exist column finished");
						if (log.isTraceEnabled()) {
							if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] table layout after add not exist column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								log.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

					// update changed column define to DB
					if (typeNotMatchColumns.size() > 0) {
						if (log.isTraceEnabled()) log.trace("\tupdate changed column define to DB");
						statement = conn.createStatement();
						for (DbColumn c : typeNotMatchColumns) {
							log.trace("!!!!!! \t\tupdate column - " + helper.builderModifyColumnDateType(c));
							statement.addBatch(helper.builderRemoveColumn(c.columnName));
							statement.addBatch(helper.builderAddColumn(c));
						}
						statement.executeBatch();
						if (log.isTraceEnabled()) log.trace("\tupdate don't match column finished");
						if (log.isTraceEnabled()) {
							if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] table layout after update don't match column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								log.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

					// update size don't match column to DB
					if (typeSizeNotMatchColumns.size() > 0) {
						if (log.isTraceEnabled()) log.trace("\tupdate size don't match column");
						statement = conn.createStatement();
						for (DbColumn c : typeSizeNotMatchColumns) {
							log.trace("\t\tmodify column - " + helper.builderModifyColumnDateType(c));
							statement.addBatch(helper.builderModifyColumnDateType(c));
						}
						statement.executeBatch();
						if (log.isTraceEnabled()) log.trace("\tupdate size don't match column finished");

						if (log.isTraceEnabled()) {
							if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] table layout after update size don't match column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								log.debug("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

					// remove unused column
					if (needDeletedColumns.size() > 0) {
						if (log.isTraceEnabled()) log.trace("\tremove unused column");
						statement = conn.createStatement();
						for (String key : needDeletedColumns) {
							log.trace("\t\tremove column - " + helper.builderRemoveColumn(key));
							statement.addBatch(helper.builderRemoveColumn(key));
						}
						statement.executeBatch();
						if (log.isTraceEnabled()) log.trace("\tremove unused column finished");

						if (log.isTraceEnabled()) {
							if (log.isTraceEnabled()) log.trace("\t[" + helper.tableName + "] table layout after  remove unused column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								log.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

				}

				conn.commit();

			} else {
				String sqlCreateTable = helper.builderCreate();
				if (log.isTraceEnabled()) {
					log.trace(sqlCreateTable);
				}
				statement.executeUpdate(sqlCreateTable);
//				helper.builderAddPrimaryKey( helper.getKeyColumns());

				conn.commit();
			}

		} catch (SQLException e) {
			log.trace(e);
			throw new RuntimeException(e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				dbconfig.closeConnection(conn);
			} catch (SQLException e) {
			}
		}
	}

	public T get(Object... keys) {
		if (log.isTraceEnabled()) log.trace("\tSQL_GET : " + Joiner.on(',').join(keys));

		List<T> list = executeQuery(SQL_GET, keys);
		if (list == null) {
			throw new RuntimeException("Can not find record key:" + keys);
		}

		if (list.size() != 1) {
			throw new RuntimeException("Can not find record key:" + keys);
		}
		return list.get(0);
	}

	@Override
	public void insert(T value) {
		if (log.isDebugEnabled()) {
			// log.debug("\t" + SQL_INSERT + ": " + value);
		}
		executeUpdate(SQL_INSERT, value);
	}

	@Override
	public void update(T value, Object... keys) {
		if (log.isDebugEnabled()) {
			// log.debug("\t" + SQL_UPDATE + ": " + value);
		}
		executeUpdate(SQL_UPDATE, value, keys);
	}

	@Override
	public void deleteAll() {
		Connection conn = null;
		try {
			conn = dbconfig.openConnection();
			conn.createStatement().execute(helper.builderDeleteAll());
		} catch (Exception e) {
			log.error(e.getClass().getName(), e);
			throw new RuntimeException(e);
		} finally {
			dbconfig.closeConnection(conn);
		}
	}

	@Override
	public List<T> getAll() {
		return executeQuery(SQL_LIST); // Only hot data
	}

	@Override
	public void delete(T value) {
		log.debug(SQL_DELETE + " : " + value);
		executeUpdate(SQL_DELETE, value);

	}

	@Override
	public void drop() {
		Connection conn = dbconfig.openConnection();
		try {
			conn.createStatement().execute(helper.builderDrop());
		} catch (Exception e) {
			log.error(e.getClass().getName(), e);
		} finally {
			dbconfig.closeConnection(conn);
		}
	}

	@Override
	public void close() {

	}

	@Override
	public long getCurrentMaxID() {
		Connection conn = dbconfig.openConnection();
		ResultSet res = null;
		try {
			res = conn.createStatement().executeQuery(SQL_MAX_ID);
			if (log.isTraceEnabled()) {
				log.trace("\texecuteQuery Open Recordset");
			}
			while (res.next()) {
				return res.getLong(1);
			}
			return 0;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (res != null) {
					res.close();
					if (log.isTraceEnabled()) {
						log.trace("\texecuteQuery Close Recordset");
					}
				}
				dbconfig.closeConnection(conn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private List<T> executeQuery(String sql, Object... keys) {
		Connection conn = dbconfig.openConnection();
		ResultSet res = null;
		try {
			PreparedStatement pstmt = conn.prepareStatement(sql);

			int pos = 0;
			for (int i = 0; i < keys.length; i++) {
				pstmt.setObject(pos + i + 1, keys[i]);
			}
			res = pstmt.executeQuery();
			if (log.isTraceEnabled()) {
				log.trace("\texecuteQuery Open Recordset");
			}
			List<T> list = new ArrayList<T>();

			while (res.next()) {
				T v = serializer.fromDb(res);
				list.add(v);
			}

			return list;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (res != null) {
					res.close();
					if (log.isTraceEnabled()) {
						log.trace("\texecuteQuery Close Recordset");
					}
				}
				dbconfig.closeConnection(conn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void executeUpdate(String sql, T v, Object... keys) {
		Connection conn = null;
		ResultSet res = null;
		try {
			conn = dbconfig.openConnection();
			PreparedStatement pstmt = conn.prepareStatement(sql);

			int pos = serializer.toDb(pstmt, v);

			for (int i = 0; i < keys.length; i++) {
				pstmt.setObject(pos + i, keys[i]);
			}

			pstmt.executeUpdate();
			conn.commit();
			pstmt.clearParameters();

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (res != null) res.close();
				dbconfig.closeConnection(conn);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public List<T> query(Object... values) {
		Object[] vs = new Object[values.length / 2];
		StringBuilder sb = new StringBuilder();
		sb.append(SQL_LIST);
		sb.append(" WHERE ");
		for (int i = 0; i < values.length; i += 2) {
			String s = helper.knownedColumns.get(values[i]).columnName;
			sb.append(s);
			sb.append("= ?");
			sb.append(" AND ");
			vs[i / 2] = values[i + 1];
		}

		sb.setLength(sb.length() - 5);

		return executeQuery(sb.toString(), vs);
	}
}
