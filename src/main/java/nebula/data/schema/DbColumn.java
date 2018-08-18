package nebula.data.schema;

import nebula.data.Timable;
import nebula.lang.RawTypes;

public class DbColumn implements Timable {
	public final int jdbcType;

	public DbColumn(String fieldName, String columnName, boolean key, boolean nullable, boolean array, RawTypes rawType, long size, int precision, int scale,
			int jdbcType) {
		this.fieldName = fieldName;
		this.columnName = columnName;
		this.key = key;
		this.nullable = nullable;
		this.array = array;
		this.rawType = rawType;
		this.precision = precision;
		this.scale = scale;
		this.size = size;
		this.jdbcType = jdbcType;
	}

	public final String fieldName;
	public final String columnName;
	public final boolean key;
	public final boolean nullable;
	public final boolean array;
	public final RawTypes rawType;
	public final long size;
	public final int precision;
	public final int scale;

	@Override
	public String toString() {
		return "DbColumn [fieldName=" + fieldName + ", columnName=" + columnName + ", rawType=" + rawType + ", size=" + size + ", precision=" + precision
				+ ", scale=" + scale + ", key=" + key + "]";
	}

	@Override
	public long getLastModified() {
		// TODO Not realized getLastModified
		return 0;
	}

}
