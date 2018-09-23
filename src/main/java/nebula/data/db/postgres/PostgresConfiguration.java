package nebula.data.db.postgres;

import nebula.data.db.DbConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresConfiguration extends DbConfiguration {
	private static final Logger log = LoggerFactory.getLogger(PostgresConfiguration.class);

	public PostgresConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}
	}
}
