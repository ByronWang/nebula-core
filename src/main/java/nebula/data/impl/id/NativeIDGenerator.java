package nebula.data.impl.id;

import nebula.data.Entity;
import nebula.data.impl.IDGenerator;

public class NativeIDGenerator implements IDGenerator {
	private long currentMaxValue;

	public NativeIDGenerator() {
		this(0L);
	}

	public NativeIDGenerator(Long value) {
		currentMaxValue = value;
	}

	@Override
	public Long nextValue(Entity data) {
		return ++this.currentMaxValue;
	}

	@Override
	public void init(Long initValue) {
		this.currentMaxValue = initValue;
	}

	@Override
	public void setSeed(Long seed) {

	}

	@Override
	public Long nextValue(Entity data, Long seed) {
		return this.currentMaxValue += seed;
	}
}
