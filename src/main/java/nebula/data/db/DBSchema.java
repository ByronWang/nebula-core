package nebula.data.db;

import java.sql.Types;
import java.util.EnumMap;

import nebula.lang.RawTypes;
import nebula.lang.Type;

public class DBSchema {

	TypeNames typeNames = new TypeNames();

	protected DBSchema() {

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
	}

	final protected EnumMap<RawTypes, Integer> dbTypeMap = new EnumMap<RawTypes, Integer>(RawTypes.class);

	protected int getJdbcType(RawTypes rawtype) {
		return dbTypeMap.get(rawtype);
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

	public DbSqlHelper builderSQLHelper(Type type) {
//		return new DbSqlHelper(this, type);//TODO 
		return null;
	}

	public String builderAddColumn(String tableName, DbColumn column) {
		if (column.key) {
			return "ALTER TABLE " + tableName + " ADD COLUMN " + column.columnName + " " + this.makeTypeDefine(column)
					+ " NOT NULL";
		} else {
			return "ALTER TABLE " + tableName + " ADD COLUMN " + column.columnName + " " + this.makeTypeDefine(column);
		}
	}

	public String builderAddPrimaryKey(String tableName, String keys) {
		return "ALTER TABLE " + tableName + " ADD PRIMARY KEY ( " + keys + ") ";
	}

	public String builderCount(String tableName) {
		return "SELECT count(1) FROM " + tableName + " ";
	}

	public String builderDelete(String tableName, String wherekeys) {
		return "DELETE FROM " + tableName + " WHERE " + wherekeys + "";
	}

	public String builderDeleteAll(String tableName) {
		return "DELETE FROM " + tableName + " ";
	}

	public String builderDrop(String tableName) {
		return "DROP TABLE " + tableName + " ";
	}

	public String builderDropPrimaryKey(String tableName) {
		return "ALTER TABLE " + tableName + " DROP PRIMARY KEY";
	}

	public String builderGet(String tableName, String fieldlist_comma, String wherekeys) {
		return "SELECT " + fieldlist_comma + ",TIMESTAMP_  FROM " + tableName + " WHERE " + wherekeys + "";
	}

	public String builderGetMeta(String tableName) {
		return "SELECT * FROM " + tableName + " WHERE 0=1";
	}

	public String builderInsert(String tableName, String fieldlist_comma, String fieldlist_questions) {
		return "INSERT INTO  " + tableName + "(" + fieldlist_comma + ",TIMESTAMP_) values(" + fieldlist_questions
				+ ",CURRENT_TIMESTAMP)";

	}

	public String builderList(String tableName, String fieldlist_comma) {
		return "SELECT " + fieldlist_comma + ",TIMESTAMP_ FROM " + tableName + " ";
	}

	public String builderMaxId(String tableName, String columnName) {
		// ID表肯定只有一个Key，所以可以直接使用keyColumn[0]
		return "SELECT max(" + columnName + ") FROM " + tableName + " ";
	}

	public String builderModifyColumnDateType(String tableName, DbColumn column) {
		return "ALTER TABLE " + tableName + " ALTER COLUMN " + column.columnName + " SET DATA TYPE "
				+ makeTypeDefine(column);
	}

	public String builderRemoveColumn(String tableName, String columnName) {
		return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName + " ";
	}

	public String builderUpdate(String tableName, String sets, String wherekeys) {
		String sb = "UPDATE " + tableName + " SET ";
		sb += sets;
		sb += "WHERE " + wherekeys + "";
		return sb.toString();
	}

	public String builderCreate(String tableName, DbColumn[] userColumns, DbColumn[] systemColumns) {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE " + tableName);
		sb.append("(");

		for (DbColumn column : userColumns) {
			if (column.key) {
				sb.append(column.columnName).append(' ').append(makeTypeDefine(column)).append(" NOT NULL").append(",");
			} else {
				sb.append(column.columnName).append(' ').append(makeTypeDefine(column)).append(",");
			}
		}

		sb.append("PRIMARY KEY ( ");
		for (DbColumn column : userColumns) {
			if (column.key) {
				sb.append(column.columnName).append(",");
			}
		}
		sb.setCharAt(sb.length() - 1, ')');
		sb.append(",");
		for (DbColumn column : systemColumns) {
			sb.append(column.columnName.toUpperCase()).append(' ').append(makeTypeDefine(column)).append(",");
		}

		sb.setCharAt(sb.length() - 1, ')');

		return sb.toString();
	}

}
