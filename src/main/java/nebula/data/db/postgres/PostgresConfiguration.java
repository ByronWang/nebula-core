package nebula.data.db.postgres;

import nebula.data.db.DbConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PostgresConfiguration extends DbConfiguration {
	private static final Log log = LogFactory.getLog(PostgresConfiguration.class);

	public PostgresConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}
	}
}
