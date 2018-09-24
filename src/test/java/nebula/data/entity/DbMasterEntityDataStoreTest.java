package nebula.data.entity;

import java.sql.Connection;

import junit.framework.TestCase;
import nebula.data.DataRepos;
import nebula.data.DataStore;
import nebula.data.Entity;
import nebula.data.db.DBConfig;
import nebula.data.db.DbConfiguration;
import nebula.data.entity.DbDataRepos;
import nebula.data.entity.EditableEntity;
import nebula.data.impl.TypeDatastore;
import nebula.lang.SystemTypeLoader;

public class DbMasterEntityDataStoreTest extends TestCase {

	DataRepos p;
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
		dbconfig.shutdown();
		super.tearDown();
	}

	public final void testCreateNew() {
		EditableEntity entity = new EditableEntity();
		assertNull(entity.source);
		assertNull(entity.data);
		assertNotNull(entity.newData);
		assertEquals(0, entity.newData.size());
	}
}
