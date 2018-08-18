package nebula.data.db.mysql;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import nebula.data.db.DbConfiguration;
import nebula.data.schema.DBSchema;
import nebula.data.schema.DbColumn;
import nebula.data.schema.DbSqlHelper;
import nebula.lang.RawTypes;
import nebula.lang.Type;

public class MysqlConfiguration extends DbConfiguration {
	private static final Log log = LogFactory.getLog(MysqlConfiguration.class);
	ComboPooledDataSource dataSource;

	public MysqlConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}

		registerColumnType(RawTypes.Boolean, "bit(1)");// .BIGINT
		registerColumnType(RawTypes.Long, "bigint");// .BIGINT
		registerColumnType(RawTypes.Decimal, "decimal($p,$s)");
		registerColumnType(RawTypes.String, "varchar($l)");
		registerColumnType(RawTypes.Text, "text");
		registerColumnType(RawTypes.Date, "date");
		registerColumnType(RawTypes.Time, "time");
		registerColumnType(RawTypes.Datetime, "datetime");
		registerColumnType(RawTypes.Timestamp, "timestamp");

		dbTypeMap.put(RawTypes.Boolean, Types.BIT);
		dbTypeMap.put(RawTypes.Long, Types.BIGINT);
		dbTypeMap.put(RawTypes.Decimal, Types.DECIMAL);
		dbTypeMap.put(RawTypes.String, Types.VARCHAR);
		dbTypeMap.put(RawTypes.Text, Types.LONGVARCHAR);
		dbTypeMap.put(RawTypes.Date, Types.DATE);
		dbTypeMap.put(RawTypes.Time, Types.TIME);
		dbTypeMap.put(RawTypes.Datetime, Types.TIMESTAMP);
		dbTypeMap.put(RawTypes.Timestamp, Types.TIMESTAMP);

	}

	@Override
	public void shutdown() {
		super.shutdown();

		try { // perform a clean shutdown
			String shutdownUrl = this.url.replaceAll(";create=true", ";shutdown=true");
			DriverManager.getConnection(shutdownUrl);
			log.info("== shut down database s- " + shutdownUrl);
		} catch (SQLException se) {
		}
	}

	@Override
	protected DBSchema makeDbSchema() {
		// TODO Auto-generated method stub
		return new DBSchema() {

			@Override
			public String builderModifyColumnDateType(String tableName, DbColumn column) {
				return "ALTER TABLE " + tableName + " CHANGE  COLUMN " + column.columnName + " " + column.columnName
						+ " " + makeTypeDefine(column);
			}

		};
	}

	@Override
	public DbSqlHelper builderSQLHelper(Type type) {
		return new DbSqlHelper(this.getSchema(), type);
	}

	@Override
	protected void finalize() throws Throwable {
		this.shutdown();
	}

	@Override
	public Connection openConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void init() {
		try {
			dataSource = new ComboPooledDataSource();
			dataSource.setDriverClass(this.driverClass);
			dataSource.setJdbcUrl(this.url);
			dataSource.setUser(this.userName);
			dataSource.setPassword(this.userPassword);
			dataSource.setInitialPoolSize(2);
			dataSource.setMinPoolSize(1);
			dataSource.setMaxPoolSize(50);
			dataSource.setMaxStatements(50);
			dataSource.setMaxIdleTime(500);
		} catch (PropertyVetoException e1) {
			throw new RuntimeException(e1);
		}
	}

	@Override
	public void closeConnection(Connection conn) {
		try {
			if (conn != null) conn.close();
			log.trace("Opened Connections : " + dataSource.getNumBusyConnections());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}
}
