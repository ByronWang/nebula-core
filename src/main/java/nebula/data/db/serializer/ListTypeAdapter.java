package nebula.data.db.serializer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.regex.Pattern;

import nebula.lang.RawTypes;

public abstract class ListTypeAdapter<T> extends JavaJdbcMapper<List<T>> {

	private static EnumMap<RawTypes, ListTypeAdapter<?>> typeMaps = new EnumMap<RawTypes, ListTypeAdapter<?>>(RawTypes.class);
	static {
		typeMaps.put(RawTypes.Boolean, new DbBooleanTypeAdapter());
		typeMaps.put(RawTypes.Long, new DbLong_BigInt_TypeAdapter());
		typeMaps.put(RawTypes.Decimal, new DbDecimalDealer());
		typeMaps.put(RawTypes.String, new DbString_Varchar_TypeAdapter());
		typeMaps.put(RawTypes.Text, new DbTextBlock_Varchar_TypeAdapter());
		typeMaps.put(RawTypes.Date, new DbDateTypeAdapter());
		typeMaps.put(RawTypes.Time, new DbTimeTypeAdapter());
		typeMaps.put(RawTypes.Datetime, new DbDatetimeTypeAdapter());
		typeMaps.put(RawTypes.Timestamp, new DbTimestampTypeAdapter());
	};
	
	public static ListTypeAdapter<?> getAdapter(RawTypes rawType){
		 return typeMaps.get(rawType);
	 }

	static class DbBooleanTypeAdapter extends ListTypeAdapter<Boolean> {
		@Override
		public List<Boolean> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Boolean>();

			String[] strValues = strValue.split(",");
			List<Boolean> values = new ArrayList<Boolean>(strValues.length);

			for (String v : strValues) {

				values.add(Boolean.parseBoolean(v));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Boolean> values = (List<Boolean>) value;

			StringBuilder sb = new StringBuilder();

			for (Boolean v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbLong_BigInt_TypeAdapter extends ListTypeAdapter<Long> {
		@Override
		public List<Long> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Long>();
			String[] strValues = strValue.split(",");
			List<Long> values = new ArrayList<Long>(strValues.length);

			for (String v : strValues) {

				values.add(Long.parseLong(v, 10));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Long> values = (List<Long>) value;

			StringBuilder sb = new StringBuilder();

			for (Long v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbDecimalDealer extends ListTypeAdapter<BigDecimal> {
		@Override
		public List<BigDecimal> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<BigDecimal>();
			String[] strValues = strValue.split(",");
			List<BigDecimal> values = new ArrayList<BigDecimal>(strValues.length);

			for (String v : strValues) {

				values.add(new BigDecimal(v));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<BigDecimal> values = (List<BigDecimal>) value;

			StringBuilder sb = new StringBuilder();

			for (BigDecimal v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbTextBlock_Varchar_TypeAdapter extends ListTypeAdapter<String> {
		Pattern sep = Pattern.compile("\\]\\]\\^\\~\\[\\[");

		@Override
		public List<String> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<String>();

			String[] strValues = sep.split(strValue, 0);

			List<String> values = new ArrayList<String>(strValues.length);
			for (String v : strValues) {
				values.add(v);
			}
			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<String> values = (List<String>) value;

			StringBuilder sb = new StringBuilder();

			for (String v : values) {
				sb.append(v);
				sb.append("]]^~[[");
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 6));
			} else {
				res.setString(index, "");
			}

		}
	}

	static class DbString_Varchar_TypeAdapter extends ListTypeAdapter<String> {
		Pattern sep = Pattern.compile("(\\]\\]\\^\\~\\[\\[)");

		@Override
		public List<String> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<String>();
			String[] strValues = sep.split(strValue, 0);
			List<String> values = new ArrayList<String>(strValues.length);

			for (String v : strValues) {
				values.add(v);
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<String> values = (List<String>) value;

			StringBuilder sb = new StringBuilder();

			for (String v : values) {
				sb.append(v);
				sb.append("]]^~[[");
			}
			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 6));
			} else {
				res.setString(index, "");
			}

		}
	}

	static class DbDateTypeAdapter extends ListTypeAdapter<Date> {
		@Override
		public List<Date> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Date>();
			String[] strValues = strValue.split(",");
			List<Date> values = new ArrayList<Date>(strValues.length);

			for (String v : strValues) {
				if(v.length()==0 || "null".equals(v)){
					values.add(null);
				}else{
					values.add(Date.valueOf(v));					
				}
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Date> values = (List<Date>) value;

			StringBuilder sb = new StringBuilder();

			for (Date v : values) {
				if(v!=null){
					sb.append(v);
				}
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbTimeTypeAdapter extends ListTypeAdapter<Time> {
		@Override
		public List<Time> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Time>();
			String[] strValues = strValue.split(",");
			List<Time> values = new ArrayList<Time>(strValues.length);

			for (String v : strValues) {

				values.add(Time.valueOf(v));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Time> values = (List<Time>) value;

			StringBuilder sb = new StringBuilder();

			for (Time v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbDatetimeTypeAdapter extends ListTypeAdapter<Timestamp> {
		@Override
		public List<Timestamp> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Timestamp>();

			String[] strValues = strValue.split(",");
			List<Timestamp> values = new ArrayList<Timestamp>(strValues.length);

			for (String v : strValues) {

				values.add(Timestamp.valueOf(v));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Timestamp> values = (List<Timestamp>) value;

			StringBuilder sb = new StringBuilder();

			for (Timestamp v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}

	static class DbTimestampTypeAdapter extends ListTypeAdapter<Timestamp> {
		@Override
		public List<Timestamp> readFrom(ResultSet res, int index) throws Exception {
			String strValue = res.getString(index);
			if (strValue == null || strValue.length() == 0) return new ArrayList<Timestamp>();
			String[] strValues = strValue.split(",");
			List<Timestamp> values = new ArrayList<Timestamp>(strValues.length);

			for (String v : strValues) {
				values.add(Timestamp.valueOf(v));
			}

			return values;
		}

		@Override
		public void writeTo(int index, Object value, PreparedStatement res) throws Exception {
			if (value == null) {
				res.setString(index, "");
				return;
			}
			@SuppressWarnings("unchecked")
			List<Timestamp> values = (List<Timestamp>) value;

			StringBuilder sb = new StringBuilder();

			for (Timestamp v : values) {
				sb.append(v);
				sb.append(',');
			}

			if (sb.length() > 0) {
				res.setString(index, sb.substring(0, sb.length() - 1));
			} else {
				res.setString(index, "");
			}
		}
	}
}
