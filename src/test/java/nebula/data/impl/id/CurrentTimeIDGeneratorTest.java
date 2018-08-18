package nebula.data.impl.id;

import junit.framework.TestCase;

public class CurrentTimeIDGeneratorTest extends TestCase {
	CurrentTimeIDGenerator idGenerator;

	protected void setUp() throws Exception {
		super.setUp();
		idGenerator = new CurrentTimeIDGenerator();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public final void testCurrentTimeIDGenerator() {
		idGenerator = new CurrentTimeIDGenerator();
	}

	public final void testCurrentTimeIDGeneratorLong() {
		idGenerator = new CurrentTimeIDGenerator(1000L);
		assertEquals(1000L, idGenerator.seed);
	}

	public final void testNextValue() {
		idGenerator = new CurrentTimeIDGenerator(1000L);
		assertEquals(1000L, idGenerator.seed);

		long nextValue = idGenerator.nextValue(null);
		System.out.println(nextValue);

	}

}
