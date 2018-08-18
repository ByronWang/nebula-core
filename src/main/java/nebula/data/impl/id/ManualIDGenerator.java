package nebula.data.impl.id;

import nebula.data.Entity;
import nebula.data.impl.IDGenerator;

public class ManualIDGenerator implements IDGenerator {

	public ManualIDGenerator() {
	}


	@Override
	public Long nextValue(Entity data) {
		return data.getLong("ID");
	}

	@Override
	public void init(Long initValue) {
	}

	@Override
	public void setSeed(Long seed) {
	}

	@Override
	public Long nextValue(Entity data, Long seed) {
        return data.getLong("ID");
	}

}
