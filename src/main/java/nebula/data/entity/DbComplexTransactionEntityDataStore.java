package nebula.data.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;

import nebula.data.Entity;
import nebula.data.entity.EditableEntity;
import nebula.data.entity.EntityDataStore;
import nebula.data.entity.EntityImp;
import nebula.data.impl.DataStoreAdv;
import nebula.data.impl.IDGenerator;
import nebula.data.impl.IdReaderBuilder;
import nebula.data.schema.DbPersister;
import nebula.lang.Field;
import nebula.lang.NebulaNative;
import nebula.lang.Type;
import nebula.lang.TypeStandalone;

public class DbComplexTransactionEntityDataStore extends EntityDataStore {

	final DbPersister<Entity> db;

	final IDGenerator idGenerator;
	final IDGenerator idChildGenerator;
	final String keyFieldName;
	final Field complexField;
	final DataStoreAdv<Entity> complexDataStore;

	DbComplexTransactionEntityDataStore(final DbDataRepos dataRepos, Type type, final DbPersister<Entity> exec, final DbPersister<Entity> children) {
		super(IdReaderBuilder.getIDReader(type), dataRepos, type);
		this.db = exec;

		Field localKey = null;
		Field complexField = null;
		for (Field f : type.getFields()) {
			if (f.isKey() && localKey == null) {
				if (f.getType().getStandalone() == TypeStandalone.Basic) {
					localKey = f;
				}
			}

			if (f.isArray() && f.getType().getStandalone() == TypeStandalone.Transaction) {
				complexField = f;
			}
		}

		if (complexField == null) {
			throw new RuntimeException("no complex key");
		}
		this.complexField = complexField;
		this.complexDataStore = (DataStoreAdv<Entity>)dataRepos.define(Long.class, Entity.class, complexField.getType().getName());

		keyFieldName = checkNotNull(localKey).getName();

		idGenerator = IDGenerators.build(type);
		idGenerator.init(exec.getCurrentMaxID());
		idGenerator.setSeed((long) type.getName().hashCode() % (1 << 8));

		idChildGenerator = IDGenerators.build(complexField.getType());
		idChildGenerator.init(children.getCurrentMaxID());
		idChildGenerator.setSeed((long) complexField.getType().getName().hashCode() % (1 << 8));

		List<Entity> list = exec.getAll();
		for (Entity data : list) {
			loadin((EditableEntity) data);
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
				id = (Long) sourceEntity.get(keyFieldName);

				List<Entity> list = newEntity.get(complexField.getName());
				if (list != null) {
					for (Entity item : list) {
						item.put(this.type.getName() + this.keyFieldName, id);
						NebulaNative.onSave(null, dataRepos, item, complexField.getType());
					}
				}
				
				NebulaNative.onSave(null, dataRepos, newEntity, type);
				db.update(newEntity, id);
				
				if (list != null) {
					for (Entity item : list) {
						complexDataStore.save(item);
					}
				}
				
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
				newEntity.put(keyFieldName, id);

				List<Entity> list = newEntity.get(complexField.getName());
				if (list != null) {
					for (Entity item : list) {
						item.put(this.type.getName() + this.keyFieldName, id);
						NebulaNative.onSave(null, dataRepos, item, complexField.getType());
					}
				}

				NebulaNative.onSave(null, dataRepos, newEntity, type);

				// DB
				db.insert(newEntity);
				if (list != null) {
					for (Entity item : list) {
						complexDataStore.save(item);
					}
				}

				EntityImp newSource = loadin((EditableEntity) db.get(id));
				newEntity.resetWith(newSource);
			} finally {
				lock.unlock();
			}
		}
	}

	class DbEntity extends EntityImp {
		final int index;

		DbEntity(DataStoreAdv<Entity> store, Map<String, Object> data, int index) {
			super(store, data);
			this.index = index;
		}
	}

	private EntityImp loadin(EditableEntity entity) {
		String key = String.valueOf((Long) entity.get(keyFieldName));

		entity.put(Entity.PRIMARY_KEY, key);
//		List<Entity> list = complexDataStore.getClassificator(type.getName() + "_" + "ID").getData((String)entity.get(Entity.PRIMARY_KEY));
//		entity.put(complexField.getName(), list);

		List<Entity> newList = complexDataStore.getClassificator(type.getName()).getData(key);
		entity.put(complexField.getName(), newList);

		DbEntity inner = new DbEntity(this, entity.newData, this.values.size());
		NebulaNative.onLoad(null, dataRepos, entity, this.type);
		this.values.add(inner);
		return inner;
	}

	private EntityImp loadin(DbEntity sourceEntity, EditableEntity newEntity) {
		String key = String.valueOf((Long) newEntity.get(keyFieldName));
		newEntity.put(Entity.PRIMARY_KEY,key );
		List<Entity> newList = complexDataStore.getClassificator(type.getName()).getData(key);
		newEntity.put(complexField.getName(), newList);
		
		
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
