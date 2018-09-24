package nebula.data.entity;

import java.sql.Connection;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import nebula.data.DataSession;
import nebula.data.DataStore;
import nebula.data.Entity;
import nebula.data.TransactionCaller;
import nebula.data.db.DBConfig;
import nebula.data.db.DbConfiguration;
import nebula.data.entity.DbDataRepos;
import nebula.data.entity.EditableEntity;
import nebula.data.impl.TypeDatastore;
import nebula.lang.SystemTypeLoader;

public class DbEntityDataPersisterTest extends TestCase {

	DbDataRepos p;
	DataStore<Entity> store;
	DbConfiguration dbconfig;

	protected void setUp() throws Exception {
		dbconfig = DbConfiguration.getEngine(DBConfig.driverclass, DBConfig.url, DBConfig.username, DBConfig.password);

		p = new DbDataRepos(new TypeDatastore(new SystemTypeLoader()), dbconfig);

		dropTable("Person");

		store = p.define(String.class, Entity.class, "Person");
	}

	private void dropTable(String name) {
		Connection connection = null;
		try {
			String sqlDrop = dbconfig.getSchema().builderDrop("N" + name);
			connection = dbconfig.openConnection();
			connection.createStatement().execute(sqlDrop);
		} catch (Exception e) {
		} finally {
			dbconfig.closeConnection(connection);
		}
	}

	protected void tearDown() throws Exception {
		p.unload();
		dbconfig.shutdown();
		super.tearDown();
	}

	public final void testDefine() throws Exception {
		assertNotNull(store);
//		assertEquals("Person", store.getID());

		store.save(new TransactionCaller() {

			@Override
			public void exec(DataSession session) throws Exception {

				Entity v = new EditableEntity();
				assertNotNull(v);

				v.put("Name", "wangshilian");

				assertEquals(true, v.isDirty());
				assertEquals("wangshilian", v.get("Name"));
				assertEquals(null, ((EditableEntity) v).source);

				session.flush();

				assertEquals(true, v.isDirty());
				// assertEquals("wangshilian", v.get("ID"));
				assertEquals("wangshilian", v.get("Name"));
				assertEquals(null, ((EditableEntity) v).source);

				session.add(store, v);
				session.flush();

				assertEquals(false, v.isDirty());
				assertEquals("wangshilian", v.get("Name"));
				assertEquals("wangshilian", store.get("wangshilian").getID());

				v.put("Height", 120L);

				assertEquals(true, v.isDirty());
				assertEquals(120L, v.getLong("Height").longValue());
				assertEquals(null, store.get("wangshilian").get("Height"));

				session.add(store, v);
				session.flush();

				assertEquals(false, v.isDirty());
				assertEquals(120L, v.getLong("Height").longValue());
				assertEquals(120L, store.get("wangshilian").getLong("Height").longValue());

				v.put("Height", 180L);

				assertEquals(true, v.isDirty());
				assertEquals(180L, v.getLong("Height").longValue());
				assertEquals(120L, store.get("wangshilian").getLong("Height").longValue());

				session.clearChanges();
				session.flush();

				assertEquals(true, v.isDirty());
				assertEquals(180L, v.getLong("Height").longValue());
				assertEquals(120L, store.get("wangshilian").getLong("Height").longValue());

				session.add(store, v);
				session.flush();

				assertEquals(false, v.isDirty());
				assertEquals(180L, v.getLong("Height").longValue());
			}
		});

	}

	public final void testEntityList() throws Exception {
		assertNotNull(store);
//		assertEquals("Person", store.getID());

		store.save(new TransactionCaller() {

			@Override
			public void exec(DataSession session) throws Exception {
				Entity v = new EditableEntity();
				assertNotNull(v);

				v.put("Name", "wangshilian");
				/**
				 * Education[..5]{ DateFrom Date; DateTo Date; School Text; };
				 */
				List<EditableEntity> entities = new ArrayList<EditableEntity>();
				EditableEntity education = new EditableEntity();
				education.put("School", "kunming");
				education.put("DateFrom", Date.valueOf("1996-09-01"));
				education.put("DateTo", Date.valueOf("2000-07-01"));
				entities.add(education);

				education = new EditableEntity();
				education.put("School", "fuyang");
				education.put("DateFrom", Date.valueOf("1993-09-01"));
				education.put("DateTo", Date.valueOf("1996-07-01"));
				entities.add(education);

				v.put("Education", entities);

				assertEquals(true, v.isDirty());
				assertEquals("wangshilian", v.get("Name"));
				assertEquals(null, ((EditableEntity) v).source);

				session.flush();

				assertEquals(true, v.isDirty());
				// assertEquals("wangshilian", v.get("ID"));
				assertEquals("wangshilian", v.get("Name"));
				assertEquals(null, ((EditableEntity) v).source);

				session.add(store, v);
				session.flush();

				assertEquals(false, v.isDirty());
				assertEquals("wangshilian", v.get("Name"));
				assertEquals("wangshilian", store.get("wangshilian").get(Entity.PRIMARY_KEY));

				@SuppressWarnings("unchecked")
				List<Entity> educationList = (List<Entity>) v.get("Education");
				assertEquals(2, educationList.size());
				int i = 0;
				Entity edu = educationList.get(i);
				assertEquals("kunming", edu.get("School"));
				assertEquals(Date.valueOf("1996-09-01"), edu.get("DateFrom"));
				assertEquals(Date.valueOf("2000-07-01"), edu.get("DateTo"));

				i++;
				edu = educationList.get(i);
				assertEquals("fuyang", edu.get("School"));
				assertEquals(Date.valueOf("1993-09-01"), edu.get("DateFrom"));
				assertEquals(Date.valueOf("1996-07-01"), edu.get("DateTo"));

				education = new EditableEntity();
				education.put("School", "fuyang");
				education.put("DateFrom", Date.valueOf("1993-09-01"));
				education.put("DateFrom", Date.valueOf("1996-07-01"));
			}
		});
	}
}
