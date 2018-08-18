package nebula.data.db.serializer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import nebula.data.Entity;
import nebula.lang.RawTypes;

public class SystemTypeFieldSerializer extends DefaultFieldSerializer<Object> {

	protected final BasicTypeAdapter<?> dataDealer;

	public SystemTypeFieldSerializer(String fieldName, String columnName, boolean array, RawTypes rawType) {
		super(fieldName, columnName);
		if (array) dataDealer = ListTypeAdapter.getAdapter(rawType);
		else dataDealer = BasicTypeAdapter.getAdapter(rawType);

	}

	@Override
	public int input(ResultSet in, int pos, Entity parent, Object now) throws Exception {
		Object newly = dataDealer.readFrom(in, pos);
		parent.put(fieldName, newly);
		return ++pos;
	}

	@Override
	public int inputWithoutCheck(ResultSet in, int pos, Entity parent) throws Exception {
		Object newly = dataDealer.readFrom(in, pos);
		parent.put(fieldName, newly);
		return ++pos;
	}

	@Override
	public int output(PreparedStatement out, Object value, int pos) throws Exception {
//		dataDealer.writeTo(pos, value, out);
		return pos;
	}

}
