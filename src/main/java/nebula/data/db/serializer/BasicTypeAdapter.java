package nebula.data.db.serializer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.EnumMap;

import nebula.data.db.FieldConverter;
import nebula.lang.RawTypes;

import org.joda.time.DateTime;

abstract class BasicTypeAdapter<T extends Object> implements FieldConverter<T, ResultSet, PreparedStatement> {
	public T readFrom(ResultSet in, String name) throws Exception {
		throw new UnsupportedOperationException("readFrom(ResultSet in, String name)");
	}

	public void writeTo(String name, Object value, PreparedStatement gen) throws Exception {
		throw new UnsupportedOperationException("writeTo(int index,T value)");
	}

	private final static EnumMap<RawTypes, BasicTypeAdapter<?>> typeMaps;
	static {
		typeMaps = new EnumMap<RawTypes, BasicTypeAdapter<?>>(RawTypes.class);
		typeMaps.put(RawTypes.Boolean, new DbBooleanTypeAdapter());
		typeMaps.put(RawTypes.Long, new DbLong_BigInt_TypeAdapter());
		typeMaps.put(RawTypes.Decimal, new DbDecimalDealer());
		typeMaps.put(RawTypes.String, new DbString_Varchar_TypeAdapter());
		typeMaps.put(RawTypes.Text, new DbTextBlock_Varchar_TypeAdapter());
		typeMaps.put(RawTypes.Date, new DbDateTypeAdapter());
		typeMaps.put(RawTypes.Time, new DbTimeTypeAdapter());
		typeMaps.put(RawTypes.Datetime, new DbDatetimeTypeAdapter());
		typeMaps.put(RawTypes.Timestamp, new DbTimestampTypeAdapter());
	}

	public static BasicTypeAdapter<?> getAdapter(RawTypes rawType) {
		return typeMaps.get(rawType);
	}
}

class DbBooleanTypeAdapter extends BasicTypeAdapter<Boolean> {
	@Override
	public Boolean readFrom(ResultSet res, int index) throws Exception {
		Boolean value = res.getBoolean(index);
		return res.wasNull() ? null : value;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setBoolean(index, v != null ? (Boolean) v : false);
	}
}

class DbLong_BigInt_TypeAdapter extends BasicTypeAdapter<Long> {
	@Override
	public Long readFrom(ResultSet res, int i) throws Exception {
		Long value = res.getLong(i);
		return res.wasNull() ? null : value;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		if (v != null) res.setLong(index, (Long) v);
		else res.setNull(index, Types.BIGINT);
	}
}

class DbDecimalDealer extends BasicTypeAdapter<BigDecimal> {
	@Override
	public BigDecimal readFrom(ResultSet res, int i) throws Exception {
		BigDecimal value = res.getBigDecimal(i);
		return res.wasNull() ? null : value;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setBigDecimal(index, (BigDecimal) v);
	}
}

class DbTextBlock_Varchar_TypeAdapter extends BasicTypeAdapter<String> {
	@Override
	public String readFrom(ResultSet res, int i) throws Exception {
		String value = res.getString(i);
		return res.wasNull() ? null : value;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setString(index, (String) v);
	}
}

class DbString_Varchar_TypeAdapter extends BasicTypeAdapter<String> {
	@Override
	public String readFrom(ResultSet res, int i) throws Exception {
		String value = res.getString(i);
		return res.wasNull() ? null : value;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setString(index, (String) v);
	}
}

class DbDateTypeAdapter extends BasicTypeAdapter<DateTime> {
	@Override
	public DateTime readFrom(ResultSet res, int i) throws Exception {
		Date t = res.getDate(i);
		return t != null ? new DateTime(t): null;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setDate(index, v != null ? new Date(((DateTime) v).getMillis()) : null);
	}
}

class DbTimeTypeAdapter extends BasicTypeAdapter<DateTime> {
	@Override
	public DateTime readFrom(ResultSet res, int i) throws Exception {
		Time t = res.getTime(i);
		return t != null ? new DateTime(t): null;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setTime(index, v != null ? new Time(((DateTime) v).getMillis()) : null);
	}
}

class DbDatetimeTypeAdapter extends BasicTypeAdapter<DateTime> {
	@Override
	public DateTime readFrom(ResultSet res, int i) throws Exception {
		Timestamp t = res.getTimestamp(i);
		return t != null ? new DateTime(t): null;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		res.setTimestamp(index, v != null ? new Timestamp(((DateTime) v).getMillis()) : null);
	}
}

class DbTimestampTypeAdapter extends BasicTypeAdapter<Long> {
	@Override
	public Long readFrom(ResultSet res, int i) throws Exception {
		Timestamp t = res.getTimestamp(i);
		return t != null ? t.getTime() : null;
	}

	@Override
	public void writeTo(int index, Object v, PreparedStatement res) throws Exception {
		if(v!=null){
		res.setTimestamp(index, new Timestamp((Long) v));
		}else{
			res.setTimestamp(index, null);
		}
	}
}
