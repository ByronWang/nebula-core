package nebula.data.impl;

import java.sql.Connection;
import java.util.List;

import nebula.data.DataRepos;
import nebula.data.DataStore;
import nebula.data.Entity;
import nebula.data.db.DBConfig;
import nebula.data.db.DbConfiguration;
import nebula.lang.SystemTypeLoader;
import junit.framework.TestCase;

public class DbTransactionEntityDataStoreTest extends TestCase {

	DataRepos p;
	DataStore<Entity> store;
	DbConfiguration dbconfig;

	protected void setUp() throws Exception {
		dbconfig = DbConfiguration.getEngine(DBConfig.driverclass, DBConfig.url, DBConfig.username, DBConfig.password);

		p = new DbDataRepos(new TypeDatastore(new SystemTypeLoader()), dbconfig);

		dropTable("O2");

		store = p.define(Long.class, Entity.class, "O2");
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
		dbconfig.shutdown();
	}

	public final void testSaveEntity() {
		Entity order = null;
		List<Entity> list = null;

		order = new EditableEntity();
		order.put("Name", "F_1");
		store.save(order);

		list = store.listAll();
		assertEquals(1, list.size());
		assertEquals("F_1", list.get(0).get("Name"));

		Entity order2  = new EditableEntity();
		order2.put("Name", "F_222");
		store.save(order2);

		list = store.listAll();
		assertEquals(2, list.size());
		assertEquals("F_1", list.get(0).get("Name"));
		assertEquals("F_222", list.get(1).get("Name"));

//        order = new EditableEntity();
//        order.put("Name", "F_3");
//        store.save(order);
//        list = store.listAll();
//        assertEquals(3,list.size());
//        assertEquals("F_1", list.get(0).get("Name"));
//        assertEquals("F_2", list.get(1).get("Name"));
//        assertEquals("F_3", list.get(2).get("Name"));
//        
//        
//
//        order  = store.get((Long)list.get(0).get("ID")).editable();
//        order.put("Name", "F_11");
//        store.save(order);
//        list = store.listAll();
//        assertEquals(3,list.size());
//        assertEquals("F_2", list.get(0).get("Name"));
//        assertEquals("F_3", list.get(1).get("Name"));
//        assertEquals("F_11", list.get(2).get("Name"));
//        
//        
//
//        order  = store.get((Long)list.get(1).get("ID")).editable();
//        order.put("Name", "F_12");
//        store.save(order);
//        list = store.listAll();
//        assertEquals(3,list.size());
//        assertEquals("F_2", list.get(0).get("Name"));
//        assertEquals("F_11", list.get(1).get("Name"));
//        assertEquals("F_12", list.get(2).get("Name"));
//        
//
//        order  = store.get((Long)list.get(2).get("ID")).editable();
//        order.put("Name", "F_13");
//        store.save(order);
//        list = store.listAll();
//        assertEquals(3,list.size());
//        assertEquals("F_2", list.get(0).get("Name"));
//        assertEquals("F_11", list.get(1).get("Name"));
//        assertEquals("F_13", list.get(2).get("Name"));
//        
//
//        order  = store.get((Long)list.get(0).get("ID")).editable();
//        order.put("Name", "F_14");
//        store.save(order);
//        list = store.listAll();
//        assertEquals(3,list.size());
//        assertEquals("F_11", list.get(0).get("Name"));
//        assertEquals("F_13", list.get(1).get("Name"));
//        assertEquals("F_14", list.get(2).get("Name"));
//        

	}

}
