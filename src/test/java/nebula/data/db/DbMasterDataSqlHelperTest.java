package nebula.data.db;

import java.io.StringReader;

import junit.framework.TestCase;
import nebula.data.db.derby.DerbyConfiguration;
import nebula.data.schema.DbSqlHelper;
import nebula.lang.Type;
import nebula.lang.TypeLoaderForTest;

public class DbMasterDataSqlHelperTest extends TestCase {
	TypeLoaderForTest loader;
	Type t;
	DbSqlHelper h;
	DbConfiguration dbconfig;

	protected void setUp() throws Exception {
		loader = new TypeLoaderForTest();
		dbconfig = new DerbyConfiguration("", "", "", "");

		super.setUp();
	}

	protected void tearDown() throws Exception {
		try {
			dbconfig.shutdown();
			super.tearDown();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public final void testInlineType() {
		//@formatter:off
		String text = "" +
				"type Person { " +
				"	!!Name;" +
				"   !!Test{" +
				"		!Key Name;" +
				"		*Core Age;" +
				"		#Require Age;" +
				"		?Ignore Age;" +
				"	 };" +
				"};";
		//@formatter:on		

		t = loader.testDefineNebula(new StringReader(text)).get(0);
		h = new DbSqlHelper(dbconfig.getSchema(), t);
		assertEquals("NPerson", h.getTableName());

		assertEquals(5, h.userColumns.length);
		int i = 0;
		assertEquals("Name", h.userColumns[i].fieldName);
		assertEquals(true, t.getFields().get(i).isKey());
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("TestKey", h.userColumns[i].fieldName);
		assertEquals(true, t.getFields().get(i).isKey());
		assertEquals(true, t.getFields().get(i).getType().getFields().get(0).isKey());
		assertEquals(false, t.getFields().get(i).getType().getFields().get(0).isNullable());
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("TestCore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestRequire", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestIgnore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		assertEquals("SELECT count(1) FROM NPerson ", h.builderCount());

		assertEquals("CREATE TABLE NPerson(NAME varchar(60) NOT NULL," + "TEST_KEY varchar(60) NOT NULL,"
				+ "TEST_CORE bigint," + "TEST_REQUIRE bigint," + "TEST_IGNORE bigint,"
				+ "PRIMARY KEY ( NAME,TEST_KEY)," + "TIMESTAMP_ timestamp)", h.builderCreate());

	}

	public final void testRefType() {
		//@formatter:off
		String text = "" +
				"type TestPerson { " +
				"	!Name;" +
				"   Test{" +
				"		!Key Name;" +
				"		*Core Age;" +
				"		#Require Age;" +
				"		?Ignore Age;" +
				"	 };" +
				"	TestRef;" +
				"};";
		//@formatter:on		

		t = loader.testDefineNebula(new StringReader(text)).get(0);
		h = new DbSqlHelper(dbconfig.getSchema(), t);
		assertEquals("NTestPerson", h.getTableName());

		int i = 0;
		assertEquals("Name", h.userColumns[i].fieldName);
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("TestKey", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestCore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestRequire", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestIgnore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		i++;
		assertEquals("TestRefKey", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		i++;
		assertEquals("TestRefCore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		assertEquals(i + 1, h.userColumns.length);

		assertEquals("SELECT count(1) FROM NTestPerson ", h.builderCount());

		assertEquals("CREATE TABLE NTestPerson(" + "NAME varchar(60) NOT NULL," + "TEST_KEY varchar(60),"
				+ "TEST_CORE bigint," + "TEST_REQUIRE bigint," + "TEST_IGNORE bigint," + "TESTREF_KEY varchar(60),"
				+ "TESTREF_CORE varchar(60)," + "PRIMARY KEY ( NAME)," + "TIMESTAMP_ timestamp)", h.builderCreate());
	}
	

	public final void testRefTypeAnnotation() {
		//@formatter:off
		String text = "" +
				"type TestPerson { " +
				"	!Name;" +
				"   Test{" +
				"		@Column(\"KeyKeyName\") !Key Name;" +
				"		*Core Age;" +
				"		#Require Age;" +
				"		?Ignore Age;" +
				"	 };" +
				"	TestRef;" +
				"};";
		//@formatter:on		

		t = loader.testDefineNebula(new StringReader(text)).get(0);
		h = new DbSqlHelper(dbconfig.getSchema(), t);
		assertEquals("NTestPerson", h.getTableName());

		int i = 0;
		assertEquals("Name", h.userColumns[i].fieldName);
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("TestKey", h.userColumns[i].fieldName);
		assertEquals("TEST_KEYKEYNAME", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestCore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestRequire", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("TestIgnore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		i++;
		assertEquals("TestRefKey", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		i++;
		assertEquals("TestRefCore", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		assertEquals(i + 1, h.userColumns.length);

		assertEquals("SELECT count(1) FROM NTestPerson ", h.builderCount());

		assertEquals("CREATE TABLE NTestPerson(" + "NAME varchar(60) NOT NULL," + "TEST_KEYKEYNAME varchar(60),"
				+ "TEST_CORE bigint," + "TEST_REQUIRE bigint," + "TEST_IGNORE bigint," + "TESTREF_KEY varchar(60),"
				+ "TESTREF_CORE varchar(60)," + "PRIMARY KEY ( NAME)," + "TIMESTAMP_ timestamp)", h.builderCreate());
	}

	public final void testNestArrayType() {
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

		t = loader.testDefineNebula(new StringReader(textRef)).get(0);
		t = loader.testDefineNebula(new StringReader(text)).get(0);
		h = new DbSqlHelper(dbconfig.getSchema(), t);
		assertEquals("NTestPerson", h.getTableName());

		int i = 0;
		assertEquals("A1", h.userColumns[i].fieldName);
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("A2B1", h.userColumns[i].fieldName);
		assertEquals("A2_B1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A2B2C1", h.userColumns[i].fieldName);
		assertEquals("A2_B2C1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A2B3Rb1", h.userColumns[i].fieldName);
		assertEquals("A2_B3RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A2B4Rb1", h.userColumns[i].fieldName);
		assertEquals("A2_B4RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A2B5", h.userColumns[i].fieldName);
		assertEquals("A2_B5", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A2B6D1", h.userColumns[i].fieldName);
		assertEquals("A2B6_D1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A3Rb1", h.userColumns[i].fieldName);
		assertEquals("A3_RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A4Rb1", h.userColumns[i].fieldName);
		assertEquals("A4_RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);

		i++;
		assertEquals("A5", h.userColumns[i].fieldName);
		assertEquals("A5", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A6E1", h.userColumns[i].fieldName);
		assertEquals("A6_E1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A6E2F1", h.userColumns[i].fieldName);
		assertEquals("A6_E2F1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A6E3Rb1", h.userColumns[i].fieldName);
		assertEquals("A6_E3RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("A6E4Rb1", h.userColumns[i].fieldName);
		assertEquals("A6_E4RB1", h.userColumns[i].columnName);
		assertEquals(false, h.userColumns[i].key);

		assertEquals(i + 1, h.userColumns.length);

		assertEquals("SELECT count(1) FROM NTestPerson ", h.builderCount());

		//@formatter:off
		assertEquals("CREATE TABLE NTestPerson(" +
				"A1 varchar(60) NOT NULL," +
				"A2_B1 varchar(60)," +
				"A2_B2C1 varchar(60)," +
				"A2_B3RB1 varchar(60)," +
				"A2_B4RB1 varchar(60)," +
				"A2_B5 varchar(4000)," +
				"A2B6_D1 varchar(4000)," +
				"A3_RB1 varchar(60)," +
				"A4_RB1 varchar(60)," +
				"A5 varchar(4000)," +
				"A6_E1 varchar(4000)," +
				"A6_E2F1 varchar(4000)," +
				"A6_E3RB1 varchar(4000)," +
				"A6_E4RB1 varchar(4000)," +
				"PRIMARY KEY ( A1)," +
				"TIMESTAMP_ timestamp)", 
				h.builderCreate());
		//@formatter:on
	}

	public final void testTypse() {
		//@formatter:off
		String text = "" +
				"type TestPerson { " +
				"	!Name;" +
				"	Date;" +
				"	Time;" +
				"	Datetime;" +
				"	Timestamp;" +
				"	Quantity;" +
				"	Amount;" +
				"};";
		//@formatter:on		

		t = loader.testDefineNebula(new StringReader(text)).get(0);
		h = new DbSqlHelper(dbconfig.getSchema(), t);
		assertEquals("NTestPerson", h.getTableName());

		int i = 0;
		assertEquals("Name", h.userColumns[i].fieldName);
		assertEquals(true, h.userColumns[i].key);
		i++;
		assertEquals("Date", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("Time", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("Datetime", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("Timestamp", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("Quantity", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);
		i++;
		assertEquals("Amount", h.userColumns[i].fieldName);
		assertEquals(false, h.userColumns[i].key);

		assertEquals(i + 1, h.userColumns.length);

		assertEquals("SELECT count(1) FROM NTestPerson ", h.builderCount());

		assertEquals("CREATE TABLE NTestPerson(" + "NAME varchar(60) NOT NULL," + "DATE date," + "TIME time,"
				+ "DATETIME timestamp," + "TIMESTAMP timestamp," + "QUANTITY bigint," + "AMOUNT numeric(10,2),"
				+ "PRIMARY KEY ( NAME)," + "TIMESTAMP_ timestamp)", h.builderCreate());
	}
}
