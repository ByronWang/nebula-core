package nebula.data.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;

import nebula.data.Entity;
import nebula.data.db.derby.DerbyConfiguration;
import nebula.data.db.mysql.MysqlConfiguration;
import nebula.data.db.oracle.OracleConfiguration;
import nebula.data.db.postgres.PostgresConfiguration;
import nebula.data.schema.DBSchema;
import nebula.data.schema.DbColumn;
import nebula.data.schema.DbPersister;
import nebula.data.schema.DbSqlHelper;
import nebula.data.schema.RowMapper;
import nebula.data.schema.TypeNames;
import nebula.lang.RawTypes;
import nebula.lang.Type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class DbConfiguration {

	private static final Log log = LogFactory.getLog(DbConfiguration.class);
	protected final String driverClass;
	protected final String url;
	protected final String userName;
	protected final String userPassword;
	private Connection conn = null;

	TypeNames typeNames = new TypeNames();

	protected DbConfiguration(String driverClass, String url, String userName, String password) {
		this.driverClass = driverClass;
		this.url = url;
		this.userName = userName;
		this.userPassword = password;

		registerColumnType(RawTypes.Boolean, "smallint");// .BIGINT
		registerColumnType(RawTypes.Long, "bigint");// .BIGINT
		registerColumnType(RawTypes.Decimal, "numeric($p,$s)");
		registerColumnType(RawTypes.String, "varchar($l)");
		registerColumnType(RawTypes.Text, "varchar($l)");
		registerColumnType(RawTypes.Date, "date");
		registerColumnType(RawTypes.Time, "time");
		registerColumnType(RawTypes.Datetime, "timestamp");
		registerColumnType(RawTypes.Timestamp, "timestamp");

		dbTypeMap.put(RawTypes.Boolean, Types.SMALLINT);
		dbTypeMap.put(RawTypes.Long, Types.BIGINT);
		dbTypeMap.put(RawTypes.Decimal, Types.NUMERIC);
		dbTypeMap.put(RawTypes.String, Types.VARCHAR);
		dbTypeMap.put(RawTypes.Text, Types.VARCHAR);
		dbTypeMap.put(RawTypes.Date, Types.DATE);
		dbTypeMap.put(RawTypes.Time, Types.TIME);
		dbTypeMap.put(RawTypes.Datetime, Types.TIMESTAMP);
		dbTypeMap.put(RawTypes.Timestamp, Types.TIMESTAMP);

		this.schema = makeDbSchema();
	}

	protected DBSchema makeDbSchema() {
		return new DBSchema();
	}

	final protected EnumMap<RawTypes, Integer> dbTypeMap = new EnumMap<RawTypes, Integer>(RawTypes.class);

	protected int getJdbcType(RawTypes rawtype) {
		return dbTypeMap.get(rawtype);
	}

	public void init() {
		try {
			Class.forName(driverClass).newInstance();
			if (log.isTraceEnabled()) {
				log.trace("\tload driverClass - " + driverClass);
			}
			conn = DriverManager.getConnection(this.url, this.userName, this.userPassword);
			log.info("== open database - " + this.url);
			conn.setAutoCommit(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public DbColumn makeColumn(String fieldName, String columnName, boolean key, boolean nullable, boolean array,
			RawTypes rawType, long size, int precision, int scale) {
		long realSize;
		int jdbcType;

		if (array) {
			realSize = 4000;
			jdbcType = Types.VARCHAR;
		} else {
			realSize = size;
			jdbcType = this.getJdbcType(rawType);
		}
		return new DbColumn(fieldName, columnName, key, nullable, array, rawType, realSize, precision, scale, jdbcType);
	}

	public static DbConfiguration getEngine(String driverClass, String url, String userName, String password) {
		String dbms = url.split(":")[1].toUpperCase();
		DbConfiguration dbEngine = null;
		if ("DERBY".equals(dbms)) {
			dbEngine = new DerbyConfiguration(driverClass, url, userName, password);
		} else if ("ORACLE".equals(dbms)) {
			dbEngine = new OracleConfiguration(driverClass, url, userName, password);
		} else if ("POSTGRESQL".equals(dbms)) {
			dbEngine = new PostgresConfiguration(driverClass, url, userName, password);
		} else if ("MYSQL".equals(dbms)) {
			dbEngine = new MysqlConfiguration(driverClass, url, userName, password);
		} else {
			throw new UnsupportedOperationException();
		}

		dbEngine.init();
		return dbEngine;
	}

	protected void registerColumnType(RawTypes jdbcType, String columnTypeName) {
		typeNames.put(jdbcType, columnTypeName);
	}

	protected String makeTypeDefine(DbColumn column) {
		String typeDefine;

		if (column.array) {
			typeDefine = typeNames.get(RawTypes.Text).replaceFirst("\\$l", String.valueOf(column.size));
		} else {
			typeDefine = typeNames.get(column.rawType);
			switch (column.rawType) {
			case Decimal:
				typeDefine = typeDefine.replaceFirst("\\$p", String.valueOf(column.precision));
				typeDefine = typeDefine.replaceFirst("\\$s", String.valueOf(column.scale));
				break;
			case String:
				typeDefine = typeDefine.replaceFirst("\\$l", String.valueOf(column.size));
				break;
			case Text:
				typeDefine = typeDefine.replaceFirst("\\$l", String.valueOf(column.size));
				break;
			default:
				break;
			}
		}
		return typeDefine;
	}

	// public abstract <T extends HasID> Persistence<T> getPersister(Class<T> t,
	// Type type);

	@SuppressWarnings("unchecked")
	public <T> DbPersister<T> getPersister(Type type, Class<T> clz) {
		if (log.isTraceEnabled()) {
			log.trace("\tload persister [" + type.getName() + "] from connection - " + conn);
		}
		DbPersister<T> executor = null;

		DbSqlHelper helper = builderSQLHelper(type);
		RowMapper<T> serializer = null;
		if (clz == Entity.class) {
			RowMapper<Entity> entitySerializer = helper.getEntitySerializer();
			serializer = (RowMapper<T>) entitySerializer;
		}

		switch (type.getStandalone()) {
		case Transaction:
		case Relation:
			executor = new DbDefaultPersister<T>(this, type, helper, serializer);
			break;

		default:
			executor = new DbDefaultPersister<T>(this, type, helper, serializer);
			break;
		}

		return executor;
	}

	@SuppressWarnings("unchecked")
	public <T> DbPersister<T> getPersister(Type type, Type childType, Class<T> clz) {
		if (log.isTraceEnabled()) {
			log.trace("\tload persister [" + type.getName() + "] from connection - " + conn);
		}
		DbPersister<T> executor = null;

		DbSqlHelper helper = builderSQLHelper(type);
		RowMapper<T> serializer = null;
		if (clz == Entity.class) {
			RowMapper<Entity> entitySerializer = helper.getEntitySerializer();
			serializer = (RowMapper<T>) entitySerializer;
		}

		switch (type.getStandalone()) {
		case Transaction:
		case Relation:
			executor = new DbDefaultPersister<T>(this, type, helper, serializer);
			break;

		default:
			executor = new DbDefaultPersister<T>(this, type, helper, serializer);
			break;
		}

		return executor;
	}

	final DBSchema schema;

	public DBSchema getSchema() {
		return this.schema;
	}

	public DbSqlHelper builderSQLHelper(Type type) {
		return new DbSqlHelper(new DBSchema(), type);
	}

	public void shutdown() {
		if (this.openedConnections > 0) {
			throw new RuntimeException("this.openedConnections not zero");
		}
		try {
			if (conn != null) {
				conn.commit();
				conn.close();
			}
			log.debug("== database disconnect");
		} catch (SQLException e) {
			log.debug("Exception When disconnect db");
			log.debug(e);
		}
	}

//	
//	 public List<DbColumn> getDsTableColumnInfo(String tableName) {
//		 ResultSet resultSet;
//	       List<DbColumn> clientTableInfos = new ArrayList<DbColumn>();
//	        try {
//	            
//	            //获得列的信息
//	        	resultSet = this.conn.getMetaData().getColumns(null, null, tableName, null);
//	            while (resultSet.next()) {						
//	            	
//				int nullable =  resultSet.getInt("NULLABLE");
//				int precision=  resultSet.getInt("COLUMN_SIZE");
//				int scale = resultSet.getInt("COLUMN_SIZE");
//
//				int jdbcType = resultSet.getInt("DATA_TYPE");
//				
//				
//	                 //获得字段名称
//	                 String name = resultSet.getString("COLUMN_NAME");
//	                 //获得字段类型名称
//	                 String type = resultSet.getString("TYPE_NAME");
//	                 //获得字段大小
//	                 int size = resultSet.getInt("COLUMN_SIZE");
//	                 //获得字段备注
//	                 String remark = resultSet.getString("REMARKS");
//	                 
//	                 
//	                 DsClientColumnInfo info = new DsClientColumnInfo(null, null, null, name, remark, size, type, "false");
//	                 clientTableInfos.add(info);
//	            }
//	            
//	            //获得主键的信息
//	            resultSet = connection.getMetaData().getPrimaryKeys(null, null, tableName);
//	            while(resultSet.next()){
//	                 String  primaryKey = resultSet.getString("COLUMN_NAME");
//	                 //设置是否为主键
//	                 for (DsClientColumnInfo dsClientColumnInfo : clientTableInfos) {
//	                    if(primaryKey != null && primaryKey.equals(dsClientColumnInfo.getClientColumnCode()))
//	                        dsClientColumnInfo.setIsParmaryKey("true");
//	                    else 
//	                        dsClientColumnInfo.setIsParmaryKey("false");
//	                }
//	            }
//	            
//	            //获得外键信息
//	            resultSet = connection.getMetaData().getImportedKeys(null, null, tableName);
//	            while(resultSet.next()){
//	                String  exportedKey = resultSet.getString("FKCOLUMN_NAME");
//	                //设置是否是外键
//	                 for (DsClientColumnInfo dsClientColumnInfo : clientTableInfos) {
//	                        if(exportedKey != null && exportedKey.equals(dsClientColumnInfo.getClientColumnCode()))
//	                            dsClientColumnInfo.setIsImportedKey("true");
//	                        else 
//	                            dsClientColumnInfo.setIsImportedKey("false");
//	                }
//	            }
//	            
//	            
//	        } catch (Exception e) {
//	            e.printStackTrace();
//	            throw new RuntimeException("获取字段信息的时候失败，请将问题反映到维护人员。" + e.getMessage(), e);
//	        } finally{
//	            if(resultSet != null)
//	                try {
//	                    resultSet.close();
//	                } catch (SQLException e) {
//	                    e.printStackTrace();
//	                       throw new DataAccessFailureException("关闭结果集resultSet失败。",e);
//	                }finally{
//	                    if(connection != null)
//	                        try {
//	                            connection.close();
//	                        } catch (SQLException e) {
//	                            e.printStackTrace();
//	                               throw new DataAccessFailureException("关闭连接connection失败。",e);
//	                        }
//	                }
//	        }
//	        
//	        Set set = new HashSet();
//	        set.addAll(clientTableInfos);
//	        clientTableInfos.clear();
//	        clientTableInfos.addAll(set);
//	        return clientTableInfos;
//	    }
//
//
//}

	int openedConnections = 0;

	public Connection openConnection() {
		openedConnections++;
		log.trace("### open : " + openedConnections);
		if (openedConnections == 15) {
			System.out.println("15");
		}
		return this.conn;
	}

	public void closeConnection(Connection conn) {
		openedConnections--;
		log.trace("### close : " + openedConnections);
	}

	@Override
	protected void finalize() throws Throwable {
		this.shutdown();
	}

}
