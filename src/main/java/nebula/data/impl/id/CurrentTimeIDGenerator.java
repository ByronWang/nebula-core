package nebula.data.impl.id;

import static com.google.common.base.Preconditions.checkArgument;
import nebula.data.Entity;
import nebula.data.impl.IDGenerator;

public class CurrentTimeIDGenerator implements IDGenerator {
	long seed;

	public static final long MAX_STEP = 10;
	public static final long MAX_SEED = 1 << MAX_STEP;

	public CurrentTimeIDGenerator() {
		this(0L);
	}

	public CurrentTimeIDGenerator(Long seed) {
		this.seed = seed;
	}

	@Override
	public Long nextValue(Entity data) {
		return (System.currentTimeMillis() << MAX_STEP) + this.seed;
	}

	@Override
	public void init(Long initValue) {
	}

	@Override
	public void setSeed(Long seed) {
		checkArgument(seed < MAX_SEED);
		this.seed = seed;
	}

	@Override
	public Long nextValue(Entity data, Long seed) {
		return System.currentTimeMillis() << MAX_STEP + seed;
	}

}
