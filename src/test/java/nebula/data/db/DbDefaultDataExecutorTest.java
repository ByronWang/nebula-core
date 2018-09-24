package nebula.data.db;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import nebula.data.DataRepos;
import nebula.data.DataStore;
import nebula.data.Entity;
import nebula.data.entity.EditableEntity;
import nebula.data.schema.DbPersister;
import nebula.data.schema.DbSqlHelper;
import nebula.lang.RawTypes;
import nebula.lang.Type;
import nebula.lang.TypeLoaderForTest;

public class DbDefaultDataExecutorTest extends TestCase {
	TypeLoaderForTest loader;
	Type type;
	DbPersister<Entity> dbExec;
	DbConfiguration dbconfig;

	DataRepos p;
	DataStore<Entity> store;
	Connection conn = null;

	protected void setUp() throws Exception {
		loader = new TypeLoaderForTest();

		dbconfig = DbConfiguration.getEngine(DBConfig.driverclass, DBConfig.url, DBConfig.username, DBConfig.password);
		dbconfig.init();

		dropTable("Person");
		conn = dbconfig.openConnection();
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
		try {
			dbconfig.closeConnection(conn);
			dbconfig.shutdown();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public final void testInlineType() throws Exception {
		Statement statement;
		ResultSet rs;
		ResultSetMetaData metaData;
		int i = 0;

		/*********************************************************/
		/***** test init *****/
		/*********************************************************/
		//@formatter:off
		String text = "" +
				"type Person { " +
				"	!PersonName Name;" + 
				"	Age;" + 
				"};";
		//@formatter:on		

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		Entity data;
		DbSqlHelper helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		dbExec.drop();
		dbExec = null;

		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(3, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		try {
			data = dbExec.get("wangshilian");
			fail("should error");
		} catch (RuntimeException e) {
		}

		data = new EditableEntity();
		data.put("PersonName", "wangshilian");
		data.put("Age", 10L);

		dbExec.insert(data);

		data = dbExec.get("wangshilian");

		assertEquals(10L, data.getLong("Age").longValue());

		assertNotNull(data);

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test add column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!PersonName Name;" + 
				"	Age;" + 
				"	Active;" +
				"	Height;" +
				"	Price;" + 
				"	Name;" + 
				"	Comment;" + 
				"	Date;" + 
				"	Time;" + 
				"	Datetime;" + 
				"	Timestamp;" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(12, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.String), metaData.getColumnType(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Long), metaData.getColumnType(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Timestamp), metaData.getColumnType(i));
		i++;
		assertEquals("Active".toUpperCase(), metaData.getColumnName(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Boolean), metaData.getColumnType(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(Types.BIGINT, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Long), metaData.getColumnType(i));
		i++;
		assertEquals("Price".toUpperCase(), metaData.getColumnName(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Decimal), metaData.getColumnType(i));
		i++;
		assertEquals("Name".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(Types.VARCHAR, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.String), metaData.getColumnType(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Comment".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Text), metaData.getColumnType(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(Types.DATE, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Date), metaData.getColumnType(i));
		i++;
		assertEquals("Time".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Time".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(Types.TIME, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Time), metaData.getColumnType(i));
		i++;
		assertEquals("Datetime".toUpperCase(), metaData.getColumnName(i));
		assertEquals(Types.TIMESTAMP, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Datetime), metaData.getColumnType(i));
		i++;
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(Types.TIMESTAMP, metaData.getColumnType(i));
		assertEquals(dbconfig.getJdbcType(RawTypes.Timestamp), metaData.getColumnType(i));

		rs.close();
		// ************ Check Database table Layout *************/

		data = dbExec.get("wangshilian");
		assertNotNull(data);

		System.out.println(data);

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test modify column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	@MaxLength(250)!PersonName Name;" + 
				"	Age Name;" + 
				"	Height;" + 
				"	Date;" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		data = dbExec.get("wangshilian");
		assertNotNull(data);
		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(5, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(250, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));

		rs.close();
		// ************ Check Database table Layout *************/

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test remove column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!PersonName Name;" + 
				"	Height;" + 
				"	Date;" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		data = dbExec.get("wangshilian");
		assertNotNull(data);
		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(4, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(250, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		/*********************************************************/
		/***** test change key *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!Name;" + 
				"	PersonName Name;" + 
				"	Height;" + 
				"	Date;" + 
				"};";
		// @formatter:on

		assertEquals(1, dbExec.getAll().size());

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		assertEquals(0, dbExec.getAll().size());

		data = new EditableEntity();
		data.put("Name", "wangshilian");
		data.put("Age", 10L);

		dbExec.insert(data);

		assertEquals(1, dbExec.getAll().size());

		data = dbExec.get("wangshilian");
		assertNotNull(data);
		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(5, metaData.getColumnCount());

		i = 1;
		assertEquals("Name".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		dbExec.drop();

		dbExec.close();
		dbExec = null;
	}

	@SuppressWarnings("unchecked")
	public final void testListTypeData() throws Exception {
		Statement statement;
		ResultSet rs;
		ResultSetMetaData metaData;
		int i = 0;

		/*********************************************************/
		/***** test init *****/
		/*********************************************************/
		//@formatter:off
		String text = "" +
				"type Person { " +
				"	!PersonName Name;" + 
				"	Age[1..10];" +
				"	Alies[1..1000] Name;" + 
				"	Comment[1..1000];" + 
				"};";
		//@formatter:on		

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		Entity data;
		DbSqlHelper helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		dbExec.drop();
		dbExec = null;

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(5, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Alies".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Comment".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		try {
			data = dbExec.get("wangshilian");
			fail("should error");
		} catch (RuntimeException e) {
		}

		data = new EditableEntity();
		data.put("PersonName", "wangshilian");
		List<Long> ages = new ArrayList<Long>();
		ages.add(10L);
		ages.add(200L);
		ages.add(30000L);
		data.put("Age", ages);

		List<String> alies = new ArrayList<String>();
		alies.add("10L");
		alies.add("200L");
		alies.add("30[]~^000L");
		data.put("Alies", alies);

		List<String> comments = new ArrayList<String>();
		comments.add("C10L");
		comments.add("C200L");
		comments.add("C30[]~^000L");
		data.put("Comment", comments);

		dbExec.insert(data);

		data = null;

		data = dbExec.get("wangshilian");

		ages = (List<Long>) data.get("Age");
		assertEquals(3, ages.size());
		assertEquals((Long) 10L, ages.get(0));
		assertEquals((Long) 200L, ages.get(1));
		assertEquals((Long) 30000L, ages.get(2));

		alies = (List<String>) data.get("Alies");
		assertEquals(3, alies.size());
		assertEquals("10L", alies.get(0));
		assertEquals("200L", alies.get(1));
		assertEquals("30[]~^000L", alies.get(2));

		alies = (List<String>) data.get("Comment");
		assertEquals(3, alies.size());
		assertEquals("C10L", alies.get(0));
		assertEquals("C200L", alies.get(1));
		assertEquals("C30[]~^000L", alies.get(2));

		assertNotNull(data);

		dbExec.close();
		dbExec = null;

	}

	@SuppressWarnings("unchecked")
	public final void testListType() throws Exception {
		Statement statement;
		ResultSet rs;
		ResultSetMetaData metaData;
		int i = 0;

		/*********************************************************/
		/***** test init *****/
		/*********************************************************/
		//@formatter:off
		String text = "" +
				"type Person { " +
				"	!PersonName Name;" + 
				"	Age[1..10];" + 
				"};";
		//@formatter:on		

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		Entity data;
		DbSqlHelper helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		dbExec.drop();
		dbExec = null;

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(3, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		try {
			data = dbExec.get("wangshilian");
			fail("should error");
		} catch (RuntimeException e) {
		}

		data = new EditableEntity();
		data.put("PersonName", "wangshilian");
		List<Long> ages = new ArrayList<Long>();
		ages.add(10L);
		ages.add(200L);
		ages.add(30000L);
		data.put("Age", ages);

		dbExec.insert(data);

		data = dbExec.get("wangshilian");

		ages = (List<Long>) data.get("Age");

		assertEquals(3, ages.size());
		assertEquals((Long) 10L, ages.get(0));
		assertEquals((Long) 200L, ages.get(1));
		assertEquals((Long) 30000L, ages.get(2));

		assertNotNull(data.get("LastModified_"));

		assertNotNull(data);

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test add column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!PersonName Name;" + 
				"	Age[1..10];" + 
				"	Active[1..10];" +
				"	Height[1..10];" +
				"	Price[1..10];" + 
				"	Name[1..10];" + 
				"	Comment[1..10];" + 
				"	Date[1..10];" + 
				"	Time[1..10];" + 
				"	Datetime[1..10];" + 
				"	Timestamp[1..10];" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(12, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Active".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Price".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Name".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertTrue(4000 <= metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Comment".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertTrue(4000 <= metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Time".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Datetime".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		data = dbExec.get("wangshilian");
		assertNotNull(data);

		System.out.println(data);

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test modify column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	@MaxLength(250)!PersonName Name;" + 
				"	Age;" + 
				"	Height[1..10];" + 
				"	Date[1..10];" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		data = dbExec.get("wangshilian");
		assertNotNull(data);

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(5, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(250, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Age".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Bigint".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		dbExec.close();
		dbExec = null;

		/*********************************************************/
		/***** test remove column *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!PersonName Name;" + 
				"	Height;" + 
				"	Date;" + 
				"};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		data = dbExec.get("wangshilian");
		assertNotNull(data);

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(4, metaData.getColumnCount());

		i = 1;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(250, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		/*********************************************************/
		/***** test change key *****/
		/*********************************************************/
		//@formatter:off
		text = "" + 
				"type Person { " + 
				"	!Name;" + 
				"	PersonName Name;" + 
				"	Height;" + 
				"	Date;" + 
				"};";
		// @formatter:on

		assertEquals(1, dbExec.getAll().size());

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());

		assertEquals(0, dbExec.getAll().size());

		data = new EditableEntity();
		data.put("Name", "wangshilian");
		data.put("Height", 10L);

		dbExec.insert(data);

		assertEquals(1, dbExec.getAll().size());

		data = dbExec.get("wangshilian");
		assertNotNull(data);

		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		assertEquals(5, metaData.getColumnCount());

		i = 1;
		assertEquals("Name".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("PersonName".toUpperCase(), metaData.getColumnName(i));
		assertEquals("VARCHAR".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("Height".toUpperCase(), metaData.getColumnName(i));
		assertEquals("BigInt".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Date".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Date".toUpperCase(), metaData.getColumnTypeName(i));
		i++;
		assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Timestamp".toUpperCase(), metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		dbExec.drop();

		dbExec.close();
		dbExec = null;
	}

	@SuppressWarnings("unchecked")
	public final void testNestedListType() throws Exception {
		Statement statement;
		ResultSet rs;
		ResultSetMetaData metaData;
		int i = 0;

		/*********************************************************/
		/***** test init *****/
		/*********************************************************/

		//@formatter:off
		String textRef = "" +
				"type Company { " +
				"	!Rb1 Name;" +
				"};";
		String text = "" +
				"type TestPerson { " +
				"	!A1 Name;" +
				"   A2{" +
				"		!B1 Name;" +
				"		*B2{" +
				"				C1 Name;" +
				"		};" +
				"		#B3 Company;" +
				"		%B4 Company;" +
				"		B5[] Name;" +
				"		B6[]{" +
				"			D1 Name;" +
				"		};" +
				"	 };" +
				"	A3 Company;" +
				"	%A4 Company;" +
				"	A5[] Name;" +
				"	A6[]{" +
				"		E1 Name;" +
				"		E2{" +
				"			F1 Name;" +
				"		};" +
				"		E3 Company;" +
				"		%E4 Company;" +
				"	};" +
				"};";
		//@formatter:on		

		type = loader.testDefineNebula(new StringReader(textRef)).get(0);
		type = loader.testDefineNebula(new StringReader(text)).get(0);
		Entity data;
		DbSqlHelper helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		dbExec.drop();
		dbExec = null;

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		// ************ Check Database table Layout *************/
		statement = conn.createStatement();
		rs = statement.executeQuery(helper.builderGetMeta());
		metaData = rs.getMetaData();

		// assertEquals(15, metaData.getColumnCount());

		i = 1;
		assertEquals("A1".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		assertEquals(60, metaData.getColumnDisplaySize(i));
		i++;
		assertEquals("A2_B1".toUpperCase(), metaData.getColumnName(i));
		assertEquals("Varchar".toUpperCase(), metaData.getColumnTypeName(i));
		// i=metaData.getColumnCount()-1;
		// assertEquals("Timestamp_".toUpperCase(), metaData.getColumnName(i));
		// assertEquals("Timestamp".toUpperCase(),
		// metaData.getColumnTypeName(i));

		rs.close();
		// ************ Check Database table Layout *************/

		try {
			data = dbExec.get("A1");
			fail("should error");
		} catch (RuntimeException e) {
		}

		data = new EditableEntity();
		data.put("A1", "A1");

		EditableEntity A2 = new EditableEntity();
		A2.put("B1", "B1Data");
		A2.put("B3Rb1", "B3Rb1Data");
		A2.put("B4Rb1", "B4Rb1Data");

		A2.put("B2C1", "B2C1Data");

		List<String> B5 = new ArrayList<String>();
		B5.add("B5001");
		B5.add("B5002");
		B5.add("B5003");
		A2.put("B5", B5);

		List<EditableEntity> B6 = new ArrayList<EditableEntity>();
		EditableEntity B61 = new EditableEntity();
		B61.put("D1", "B6D1001");
		B6.add(B61);
		B61 = new EditableEntity();
		B61.put("D1", "B6D1002");
		B6.add(B61);

		A2.put("B6", B6);
		data.put("A2", A2);

		data.put("A3Rb1", "A3Rb1Data");
		data.put("A4Rb1", "A4Rb1Data");

		List<String> A5 = new ArrayList<String>();
		A5.add("A5001");
		A5.add("A5002");
		A5.add("A5003");
		data.put("A5", A5);

		List<EditableEntity> A6 = new ArrayList<EditableEntity>();
		EditableEntity A61 = new EditableEntity();
		A61.put("E1", "E1001");
		A61.put("E2F1", "E2F1001");
		A61.put("E3Rb1", "E3Rb1001");
		A61.put("E4Rb1", "E4Rb1001");
		A6.add(A61);

		A61 = new EditableEntity();
		A61.put("E1", "E1002");
		A61.put("E2F1", "E2F1002");
		A6.add(A61);

		A61 = new EditableEntity();
		A61.put("E1", "E1003");
		A6.add(A61);

		A61 = new EditableEntity();
		A61.put("E1", "E1004");
		A6.add(A61);

		data.put("A6", A6);

		dbExec.insert(data);

		data = dbExec.get("A1");

		assertNotNull(data);

		assertEquals("A1", data.get("A1"));

		assertEquals("B1Data", data.getEntity("A2").get("B1"));
		assertEquals("B2C1Data", data.getEntity("A2").get("B2C1"));
		assertEquals("B3Rb1Data", data.getEntity("A2").get("B3Rb1"));
		assertEquals("B4Rb1Data", data.getEntity("A2").get("B4Rb1"));

		B5 = (List<String>) data.getEntity("A2").get("B5");
		assertEquals(3, B5.size());
		assertEquals("B5001", B5.get(0));
		assertEquals("B5002", B5.get(1));
		assertEquals("B5003", B5.get(2));

		B6 = (List<EditableEntity>) data.getEntity("A2").get("B6");
		assertEquals(2, B6.size());
		assertEquals("B6D1001", B6.get(0).get("D1"));
		assertEquals("B6D1002", B6.get(1).get("D1"));

		assertEquals("A3Rb1Data", data.get("A3Rb1"));
		assertEquals("A4Rb1Data", data.get("A4Rb1"));

		A5 = (List<String>) data.get("A5");
		assertEquals(3, A5.size());
		assertEquals("A5001", A5.get(0));
		assertEquals("A5002", A5.get(1));
		assertEquals("A5003", A5.get(2));

		A6 = (List<EditableEntity>) data.get("A6");
		assertEquals(4, A6.size());
		assertEquals("E1001", A6.get(0).get("E1"));
		assertEquals("E2F1001", A6.get(0).get("E2F1"));
		assertEquals("E3Rb1001", A6.get(0).get("E3Rb1"));
		assertEquals("E4Rb1001", A6.get(0).get("E4Rb1"));

		assertEquals("E1002", A6.get(1).get("E1"));
		assertEquals("E2F1002", A6.get(1).get("E2F1"));
		assertEquals("E1003", A6.get(2).get("E1"));
		assertEquals("E1004", A6.get(3).get("E1"));

		dbExec.close();

		dbExec.drop();
		dbExec.close();
		dbExec = null;
	}

	public final void testRemoveColumn() {
		//@formatter:off
		String text = "" +
				"type Person { " +
				"	!Name;" +
				"	Age;" +
				"};";
		//@formatter:on		

		type = loader.testDefineNebula(new StringReader(text)).get(0);
		Entity data;
		DbSqlHelper helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		// dbExec.init();

		try {
			data = dbExec.get("wangshilian");
			fail("should error");
		} catch (RuntimeException e) {
		}

		data = new EditableEntity();
		data.put("Name", "wangshilian");
		data.put("Age", 10L);

		dbExec.insert(data);

		data = dbExec.get("wangshilian");

		assertNotNull(data);

		text = "" + "type Person { " + "	!Name;" + "};";
		// @formatter:on

		type = loader.testDefineNebula(new StringReader(text)).get(0);

		helper = new DbSqlHelper(dbconfig.getSchema(), type);
		dbExec = new DbDefaultPersister<Entity>(dbconfig, type, helper, helper.getEntitySerializer());
		// dbExec.init();
		data = dbExec.get("wangshilian");
		assertNotNull(data);

		System.out.println(data);

	}
}
