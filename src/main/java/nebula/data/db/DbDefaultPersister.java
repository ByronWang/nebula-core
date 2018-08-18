package nebula.data.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Joiner;

import nebula.data.schema.DbPersister;
import nebula.data.schema.DbSqlHelper;
import nebula.lang.Type;

/**
 * 单表持久化
 * 
 * @author wanglocal
 * 
 * @param <T>
 */
class DbDefaultPersister<T> implements DbPersister<T> {
	final Log log;
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
			DBSchemaMerger.ensureDBSchema(dbconfig, helper);
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
