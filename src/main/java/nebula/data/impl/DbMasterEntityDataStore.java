package nebula.data.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import nebula.data.Entity;
import nebula.data.db.DbPersister;
import nebula.lang.Field;
import nebula.lang.NebulaNative;
import nebula.lang.Type;
import nebula.lang.TypeStandalone;

import com.google.common.base.Function;

public class DbMasterEntityDataStore extends EntityDataStore {

	final DbPersister<Entity> db;

	final IDGenerator idGenerator;
	final String key;
	boolean withID = false;

	DbMasterEntityDataStore(final DbDataRepos dataRepos, Type type, final DbPersister<Entity> exec) {
		this(IdReaderBuilder.getIDReader(type), dataRepos, type, exec);
	}

	DbMasterEntityDataStore(Function<Entity, Object> keyMaker, final DbDataRepos dataRepos, Type type, final DbPersister<Entity> exec) {
		super(keyMaker, dataRepos, type);
		this.db = exec;

		Field localKey = null;
		for (Field f : type.getFields()) {
			if (f.isKey()) {
				if (f.getType().getStandalone() == TypeStandalone.Basic) {
					localKey = f;
					break;
				}
			}
		}

		key = checkNotNull(localKey).getName();

		if (localKey.getType().getName().equals("ID")) {
			idGenerator = IDGenerators.build(type);
			idGenerator.init(exec.getCurrentMaxID());
			idGenerator.setSeed((long) type.getName().hashCode() % (1 << 8));
			withID = true;
		} else {
			idGenerator = null;
		}

		List<Entity> list = exec.getAll();
		for (Entity data : list) {
			loadin((EditableEntity)data);
		}
	}

	@Override
	public void save(Entity newV) {
		EditableEntity newEntity = (EditableEntity) newV;
		if (newEntity.source != null) {// update
			assert newEntity.source instanceof DbEntity;
			DbEntity sourceEntity = (DbEntity) newEntity.source;

			assert sourceEntity == this.values.get(sourceEntity.index);
			Object id = null;

			lock.lock();
			try {
				// DB
				if (withID) id = (Long) sourceEntity.get(key);
				else id = sourceEntity.getID();

				NebulaNative.onSave(null, dataRepos, newEntity, type);
				db.update(newEntity, id);
				EntityImp newSource = loadin(sourceEntity, (EditableEntity)db.get(id));

				newEntity.resetWith(newSource);
			} finally {
				lock.unlock();
			}
		} else { // insert
			Object id = null;

			lock.lock();
			try {
				if (withID) {
					id = idGenerator.nextValue(newEntity);
					newEntity.put(key, id);
				} else {
					id = (String) this.idMaker.apply(newEntity);
				}

				NebulaNative.onSave(null, dataRepos, newEntity, type);

				// DB
				db.insert(newEntity);
				EntityImp newSource = loadin((EditableEntity)db.get(id));
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
		entity.put(Entity.PRIMARY_KEY, idMaker.apply(entity));
		DbEntity inner = new DbEntity(this, entity.newData, this.values.size());
		NebulaNative.onLoad(null, dataRepos, entity, this.type);
		this.values.add(inner);
		return inner;
	}

	private EntityImp loadin(DbEntity sourceEntity, EditableEntity newEntity) {
		newEntity.put(Entity.PRIMARY_KEY, sourceEntity.getID());
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
