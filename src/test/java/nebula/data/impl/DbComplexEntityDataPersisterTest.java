package nebula.data.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import nebula.data.DataStore;
import nebula.data.Entity;
import nebula.data.db.DBConfig;
import nebula.data.db.DbConfiguration;
import nebula.lang.SystemTypeLoader;

public class DbComplexEntityDataPersisterTest extends TestCase {

	DbDataRepos p;
	DataStore<Entity> store;
	DbConfiguration dbconfig;

	protected void setUp() throws Exception {
		dbconfig = DbConfiguration.getEngine(DBConfig.driverclass, DBConfig.url, DBConfig.username, DBConfig.password);

		p = new DbDataRepos(new TypeDatastore(new SystemTypeLoader()), dbconfig);

		dropTable("Product");

		store = p.define(String.class, Entity.class, "Product");
//		store.clearChanges();
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

	public final void testInsert() {
		assertNotNull(store);

		Entity v = new EditableEntity();
		assertNotNull(v);
		EditableEntity item = new EditableEntity();
		EditableEntity product = new EditableEntity();
		product.put("ID", 1L);
		product.put("Name", "iPhone");
		product.put("ExpectedPrice", new BigDecimal(12));
		item.put("Product", product);
		List<Entity> list = new ArrayList<Entity>();
		list.add(item);
		v.put("Items", list);
		store.save(v);
		assertEquals(false, v.isDirty());
		System.out.println(store.get(1L));
	}

	public final void testUpdateChild() {
		assertNotNull(store);

		Entity v = new EditableEntity();
		assertNotNull(v);
		store.save(v);

		assertNotNull(v);
		EditableEntity item = new EditableEntity();
		EditableEntity product = new EditableEntity();
		product.put("ID", 1L);
		product.put("Name", "iPhone");
		product.put("ExpectedPrice", new BigDecimal(12));
		item.put("Product", product);
		List<Entity> list = new ArrayList<Entity>();
		list.add(item);
		v.put("Items", list);
		store.save(v);
		assertEquals(false, v.isDirty());
		System.out.println(store.get(1L));

	}
}
