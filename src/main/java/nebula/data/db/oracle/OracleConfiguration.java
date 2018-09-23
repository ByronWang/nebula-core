package nebula.data.db.oracle;

import nebula.data.db.DbConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleConfiguration extends DbConfiguration {
	private static final Logger log = LoggerFactory.getLogger(OracleConfiguration.class);

	public OracleConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}
	}
}
