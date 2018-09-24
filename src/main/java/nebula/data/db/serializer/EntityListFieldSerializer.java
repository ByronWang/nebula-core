package nebula.data.db.serializer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import nebula.data.Entity;
import nebula.data.entity.EditableEntity;

public class EntityListFieldSerializer extends FieldMapperFactory<List<Entity>> {
	final List<ListTypeAdapter<?>> adapteres;
	final List<String> subFieldNames;

	public EntityListFieldSerializer(String fieldName, List<ListTypeAdapter<?>> adapteres, List<String> subFieldNames) {
		super(fieldName, null);
		this.adapteres = adapteres;
		this.subFieldNames = subFieldNames;
	}

	public int input(ResultSet in, int pos, Entity parent, List<Entity> now) throws Exception {
		throw new UnsupportedOperationException(
				"public int input(ResultSet in, int pos, Entity parent, List<Entity> now) throws Exception {");
	}

	@Override
	public int inputWithoutCheck(ResultSet in, int pos, Entity parent) throws Exception {
		List<EditableEntity> enities = new ArrayList<EditableEntity>();
		for (int i = 0; i < adapteres.size(); i++) {
			List<?> dataList = (List<?>) adapteres.get(i).readFrom(in, pos);
			if (enities.size() < dataList.size()) {
				for (int j = enities.size(); j < dataList.size(); j++) {
					EditableEntity entity = new EditableEntity();
					enities.add(entity);
				}
			}
			String subFieldName = subFieldNames.get(i);
			for (int j = 0; j < dataList.size(); j++) {
				EditableEntity entity = enities.get(j);
				entity.put(subFieldName, dataList.get(j));
			}
			++pos;
		}
		parent.put(fieldName, enities);

		return pos;
	}

	@Override
	public int output(PreparedStatement out, List<Entity> enities, int pos) throws Exception {
		for (int i = 0; i < adapteres.size(); i++) {
			List<Object> dataList = new ArrayList<Object>();
			if (enities == null) {
				adapteres.get(i).writeTo(pos, dataList, out);
			} else {
				String subFieldName = subFieldNames.get(i);

				for (int j = 0; j < enities.size(); j++) {
					Entity entity = enities.get(j);
					dataList.add(entity.get(subFieldName));
				}
				adapteres.get(i).writeTo(pos, dataList, out);
			}
			++pos;
		}

		return pos;
	}

}
