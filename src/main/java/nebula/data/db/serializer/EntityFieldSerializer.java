package nebula.data.db.serializer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import nebula.data.Entity;
import nebula.data.entity.EditableEntity;
import nebula.data.schema.RowMapper;

public class EntityFieldSerializer extends FieldMapperFactory<Entity> implements RowMapper<Entity> {

	final List<FieldMapperFactory<?>> fieldSerializer;

	public EntityFieldSerializer(List<FieldMapperFactory<?>> fieldSerializer) {
		super(null, null);
		this.fieldSerializer = new CopyOnWriteArrayList<FieldMapperFactory<?>>(fieldSerializer);
	}

	public EntityFieldSerializer(String fieldName, String columnName, List<FieldMapperFactory<?>> fieldSerializer) {
		super(fieldName, columnName);
		this.fieldSerializer = new CopyOnWriteArrayList<FieldMapperFactory<?>>(fieldSerializer);
	}

	int fromEntity(PreparedStatement prepareStatement, Entity entity) throws Exception {
		int pos = 1;
		for (FieldMapperFactory<?> c : fieldSerializer) {
			@SuppressWarnings("unchecked")
			FieldMapperFactory<Object> m = (FieldMapperFactory<Object>) c;
			pos = m.output(prepareStatement, entity.get(c.fieldName), pos);
		}
		return pos;
	}

	EditableEntity toEntity(ResultSet result) throws Exception {
		EditableEntity entity = new EditableEntity();

		int pos = 1;
		for (FieldMapperFactory<?> c : fieldSerializer) {
			pos = c.inputWithoutCheck(result, pos, entity);
		}
		return entity;
	}

	@Override
	public int input(ResultSet in, int pos, Entity parent, Entity now) throws Exception {
		return inputWithoutCheck(in, pos, parent);
	}

	@Override
	public int inputWithoutCheck(ResultSet in, int pos, Entity parent) throws Exception {
		EditableEntity entity = new EditableEntity();
		for (FieldMapperFactory<?> c : fieldSerializer) {
			pos = c.inputWithoutCheck(in, pos, entity);
		}
		parent.put(this.fieldName, entity);
		return pos;
	}

	@Override
	public int output(PreparedStatement out, Entity value, int pos) throws Exception {
		if (value != null) {
			for (FieldMapperFactory<?> c : fieldSerializer) {
				@SuppressWarnings("unchecked")
				FieldMapperFactory<Object> m = (FieldMapperFactory<Object>) c;
				pos = m.output(out, value.get(c.fieldName), pos);
			}
		} else {
			for (FieldMapperFactory<?> c : fieldSerializer) {
				@SuppressWarnings("unchecked")
				FieldMapperFactory<Object> m = (FieldMapperFactory<Object>) c;
				pos = m.output(out, null, pos);
			}
		}
		return pos;
	}

	@Override
	public EditableEntity map(ResultSet result) throws Exception {
		return this.toEntity(result);
	}

	@Override
	public int push(PreparedStatement prepareStatement, Entity entity) throws Exception {
		return this.fromEntity(prepareStatement, entity);
	}

}
