package nebula.data.db.serializer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.EnumMap;

import nebula.lang.RawTypes;

public abstract class DefaultFieldSerializer<T> implements FieldSerializer<T, ResultSet, PreparedStatement> {
//	public final int jdbcType;

	static EnumMap<RawTypes, Integer> dbTypeMap = new EnumMap<RawTypes, Integer>(RawTypes.class);
	static {
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

	public DefaultFieldSerializer(String fieldName, String columnName) {
		this.fieldName = fieldName;
		this.columnName = columnName;
	}

	// String fieldName, String columnName, boolean key, boolean nullable,
	// RawTypes rawType,long size,
	// int precision, int scale

	protected final String fieldName;
	protected final String columnName;

}
