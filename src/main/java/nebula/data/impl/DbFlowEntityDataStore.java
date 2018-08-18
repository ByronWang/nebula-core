package nebula.data.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nebula.data.DataRepos;
import nebula.data.Entity;
import nebula.data.schema.DbPersister;
import nebula.lang.Field;
import nebula.lang.NebulaNative;
import nebula.lang.Type;
import nebula.lang.TypeStandalone;

public class DbFlowEntityDataStore extends EntityDataStore {

	final DbPersister<Entity> db;

	final IDGenerator idGenerator;
	final String key;
	final Field attachField;

	final List<Field> expands = new ArrayList<Field>();

	DbFlowEntityDataStore(final DbDataRepos dataRepos, Type type, final DbPersister<Entity> exec, final DataRepos repos) {
		super(IdReaderBuilder.getIDReader(type), dataRepos, type);
		this.db = exec;

		Field localKey = null;
		Field attachField = null;
		for (Field f : type.getFields()) {
			if (f.isKey() && localKey == null) {
				if (f.getType().getStandalone() == TypeStandalone.Basic) {
					localKey = f;
				}
				if (f.isArray() && f.getType().getStandalone() == TypeStandalone.Transaction) {
					attachField = f;
				}
			}
			if (f.getAttrs().containsKey("Expand")) {
				expands.add(f);
			}
		}
		this.attachField = attachField;

		key = checkNotNull(localKey).getName();

		idGenerator = IDGenerators.build(type);
		idGenerator.init(exec.getCurrentMaxID());
		idGenerator.setSeed((long) type.getName().hashCode() % (1 << 8));

		List<Entity> list = exec.getAll();
		for (Entity data : list) {
			loadin((EditableEntity) data);
		}
	}

	@Override
	public void save(Entity newV) {
		EditableEntity newEntity = (EditableEntity) newV;
		if (attachField != null) {
			Entity entity = newEntity.get(attachField.getName());
			if (entity != null) {
				newEntity.put(attachField.getName() + "_" + "ID", entity.get("ID")); // TODO
			}
		}

		if (newEntity.source != null) {// update
			assert newEntity.source instanceof DbEntity;
			DbEntity sourceEntity = (DbEntity) newEntity.source;

			assert sourceEntity == this.values.get(sourceEntity.index);
			Object id = null;

			lock.lock();
			try {
				// DB
				id = (Long) sourceEntity.get(key);

				NebulaNative.onSave(null, dataRepos, newEntity, type);
				db.update(newEntity, id);
				EntityImp newSource = loadin(sourceEntity, (EditableEntity) db.get(id));

				newEntity.resetWith(newSource);
			} finally {
				lock.unlock();
			}
		} else { // insert
			Object id = null;

			lock.lock();
			try {
				id = idGenerator.nextValue(newEntity);
				newEntity.put(key, id);

				NebulaNative.onSave(null, dataRepos, newEntity, type);

				// DB
				db.insert(newEntity);
				EntityImp newSource = loadin((EditableEntity) db.get(id));
				newEntity.resetWith(newSource);
			} finally {
				lock.unlock();
			}
		}
	}

	class DbEntity extends EntityImp {
		final int index;

		DbEntity(DataStoreEx<Entity> store, Map<String, Object> data, int index) {
			super(store, data);
			this.index = index;
		}
	}

	private EntityImp loadin(EditableEntity entity) {
		entity.put(Entity.PRIMARY_KEY, String.valueOf((Long) entity.get(key)));

		for (Field f : this.expands) {
			if (!f.isArray()) { // TODO
				Entity ex = this.dataRepos.define(Long.class, Entity.class, f.getName()).get(entity.get(f.getName() + "ID"));
				entity.put(f.getName(), ex);
			}
		}

		DbEntity inner = new DbEntity(this, entity.newData, this.values.size());
		NebulaNative.onLoad(null, dataRepos, entity, this.type);
		this.values.add(inner);
		return inner;
	}

	private EntityImp loadin(DbEntity sourceEntity, EditableEntity newEntity) {
		newEntity.put(Entity.PRIMARY_KEY, String.valueOf((Long) newEntity.get(key)));

		for (Field f : this.expands) {
			if (!f.isArray()) { // TODO
				Entity ex = this.dataRepos.define(Long.class, Entity.class, f.getName()).get(newEntity.get(f.getName() + "ID"));
				newEntity.put(f.getName(), ex);
			}
		}

		DbEntity inner = new DbEntity(this, newEntity.newData, sourceEntity.index);
		NebulaNative.onLoad(null, dataRepos, newEntity, super.type);
		this.values.add(inner);
		return inner;
	}

	@Override
	public void load() {
		// db.init();
	}

	@Override
	public void unload() {
		db.close();
	}
}
