package nebula.data.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Function;

import nebula.data.SmartList;
import nebula.data.schema.DbColumn;
import nebula.data.schema.DbSqlHelper;

public class DBSchemaMerger {
	static final Log logroot = LogFactory.getLog(DBSchemaMerger.class);

	@SuppressWarnings("resource")
	public static void ensureDBSchema(Connection conn, DbSqlHelper helper) {
		Statement statement = null;
		boolean exist = false;
		ResultSet rs = null;
		try {

			final SmartList<String, DbColumn> mapColumns = new SmartList<String, DbColumn>(
					new Function<DbColumn, String>() {
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

				if (logroot.isTraceEnabled()) logroot.trace("\tcheck columns define");

				ArrayList<String> needDeletedColumns = new ArrayList<String>();
				Map<String, String> allColumns = new HashMap<String, String>();
				ArrayList<DbColumn> typeNotMatchColumns = new ArrayList<DbColumn>();
				ArrayList<DbColumn> typeSizeNotMatchColumns = new ArrayList<DbColumn>();

				String curKeys = "";

				for (int i = 1; i <= columnsSize; i++) {
					String columnName = metaData.getColumnName(i);

					DbColumn newColumn = mapColumns.get(columnName);
					if (newColumn == null) {
						if (logroot.isTraceEnabled()) logroot.trace("\t\t" + metaData.getColumnName(i)
								+ "\t column type not exist in type or is system column \t- "
								+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
						if (!columnName.endsWith("_")) {
							needDeletedColumns.add(columnName);
						}
					} else {
						int size = metaData.getColumnDisplaySize(i);
						@SuppressWarnings("unused")
						int nullable = metaData.isNullable(i);
						int precision = metaData.getPrecision(i);
						int scale = metaData.getScale(i);

						int jdbcType = metaData.getColumnType(i);

						if (jdbcType != newColumn.jdbcType) {
							if (logroot.isTraceEnabled()) logroot
								.trace("\t\t" + metaData.getColumnName(i) + "\t column type not match,need update \t- "
										+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
							typeNotMatchColumns.add(newColumn);

						} else {
							switch (jdbcType) {
							case Types.NUMERIC:
								if (newColumn.precision == precision && newColumn.scale == scale) {

								} else if (newColumn.precision > precision && newColumn.scale > scale) {

									if (logroot.isTraceEnabled()) logroot.trace("\t" + metaData.getColumnName(i)
											+ "\t\t column size not match,need update \t- "
											+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
									typeSizeNotMatchColumns.add(newColumn);
								} else if (newColumn.precision > precision || newColumn.scale > scale) {
									int newPrecision = newColumn.precision > precision ? newColumn.precision
											: precision;
									int newScale = newColumn.scale > scale ? newColumn.scale : scale;
									typeSizeNotMatchColumns.add(helper.makeColumn(newColumn.fieldName,
											newColumn.columnName, newColumn.key, newColumn.nullable, false,
											newColumn.rawType, newColumn.size, newPrecision, newScale));

									if (logroot.isTraceEnabled()) logroot.trace("\t" + metaData.getColumnName(i)
											+ "\t\tcolumn precision not match,need update \t- "
											+ metaData.getColumnTypeName(i) + "\t" + metaData.getColumnDisplaySize(i));
								}
								break;
							case Types.VARCHAR:
								if (newColumn.size > size) typeSizeNotMatchColumns.add(newColumn);
								break;
							}
						}

						/*
						 * if (nullable == ResultSetMetaData.columnNoNulls) { curKeys += columnName; if
						 * (!newColumn.key) { isKeyChanged = true; } }
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

				if (logroot.isTraceEnabled()) logroot.trace("\tcheck columns define finished");

				if (logroot.isTraceEnabled()) logroot.trace("\tcheck key define");
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
				if (logroot.isTraceEnabled()) logroot.trace("\tcheck key define finished");

				// key changed
				if (isKeyChanged) {
					statement = conn.createStatement();
					if (logroot.isTraceEnabled())
						logroot.trace("\t[" + helper.tableName + "] key changed,reconstruct ");
					statement.addBatch(helper.builderDrop());
					statement.addBatch(helper.builderCreate());
					statement.executeBatch();
					logroot.trace("\t[" + helper.tableName + "] reconstruct finished table  finished");

					if (logroot.isTraceEnabled()) {
						if (logroot.isTraceEnabled())
							logroot.trace("\t[" + helper.tableName + "] table layout after add not exist column");
						rs = statement.executeQuery(helper.builderGetMeta());
						metaData = rs.getMetaData();
						columnsSize = metaData.getColumnCount();
						for (int i = 1; i <= columnsSize; i++) {
							logroot.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i)
									+ "\t" + metaData.getColumnDisplaySize(i));
						}
						rs.close();
					}
				} else {
					// add not exist column to DB
					if (notExistColumns.size() > 0) {
						if (logroot.isTraceEnabled()) logroot.trace("\tadd not exist column");
						statement = conn.createStatement();
						for (DbColumn c : notExistColumns) {
							logroot.trace("\t\t add column - " + helper.builderAddColumn(c));
							statement.addBatch(helper.builderAddColumn(c));
						}
						statement.executeBatch();

						if (logroot.isTraceEnabled()) logroot.trace("\tadd not exist column finished");
						if (logroot.isTraceEnabled()) {
							if (logroot.isTraceEnabled())
								logroot.trace("\t[" + helper.tableName + "] table layout after add not exist column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								logroot.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i)
										+ "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
						
					}

					// update changed column define to DB
					if (typeNotMatchColumns.size() > 0) {
						if (logroot.isTraceEnabled()) logroot.trace("\tupdate changed column define to DB");
						statement = conn.createStatement();
						for (DbColumn c : typeNotMatchColumns) {
							logroot.trace("!!!!!! \t\tupdate column - " + helper.builderModifyColumnDateType(c));
							statement.addBatch(helper.builderRemoveColumn(c.columnName));
							statement.addBatch(helper.builderAddColumn(c));
						}
						statement.executeBatch();
						if (logroot.isTraceEnabled()) logroot.trace("\tupdate don't match column finished");
						if (logroot.isTraceEnabled()) {
							if (logroot.isTraceEnabled()) logroot
								.trace("\t[" + helper.tableName + "] table layout after update don't match column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								logroot.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i)
										+ "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

					// update size don't match column to DB
					if (typeSizeNotMatchColumns.size() > 0) {
						if (logroot.isTraceEnabled()) logroot.trace("\tupdate size don't match column");
						statement = conn.createStatement();
						for (DbColumn c : typeSizeNotMatchColumns) {
							logroot.trace("\t\tmodify column - " + helper.builderModifyColumnDateType(c));
							statement.addBatch(helper.builderModifyColumnDateType(c));
						}
						statement.executeBatch();
						if (logroot.isTraceEnabled()) logroot.trace("\tupdate size don't match column finished");

						if (logroot.isTraceEnabled()) {
							if (logroot.isTraceEnabled()) logroot.trace(
									"\t[" + helper.tableName + "] table layout after update size don't match column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								logroot.debug("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i)
										+ "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

					// remove unused column
					if (needDeletedColumns.size() > 0) {
						if (logroot.isTraceEnabled()) logroot.trace("\tremove unused column");
						statement = conn.createStatement();
						for (String key : needDeletedColumns) {
							logroot.trace("\t\tremove column - " + helper.builderRemoveColumn(key));
							statement.addBatch(helper.builderRemoveColumn(key));
						}
						statement.executeBatch();
						if (logroot.isTraceEnabled()) logroot.trace("\tremove unused column finished");

						if (logroot.isTraceEnabled()) {
							if (logroot.isTraceEnabled())
								logroot.trace("\t[" + helper.tableName + "] table layout after  remove unused column");
							rs = statement.executeQuery(helper.builderGetMeta());
							metaData = rs.getMetaData();
							columnsSize = metaData.getColumnCount();
							for (int i = 1; i <= columnsSize; i++) {
								logroot.trace("\t\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i)
										+ "\t" + metaData.getColumnDisplaySize(i));
							}
							rs.close();
						}
					}

				}

				conn.commit();

			} else {
				String sqlCreateTable = helper.builderCreate();
				if (logroot.isTraceEnabled()) {
					logroot.trace(sqlCreateTable);
				}
				statement.executeUpdate(sqlCreateTable);
				// helper.builderAddPrimaryKey( helper.getKeyColumns());

				conn.commit();
			}

		} catch (SQLException e) {
			logroot.trace(e);
			throw new RuntimeException(e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
			}
		}
	}

}
