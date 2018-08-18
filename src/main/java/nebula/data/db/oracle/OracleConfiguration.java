package nebula.data.db.oracle;

import nebula.data.db.DbConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OracleConfiguration extends DbConfiguration {
	private static final Log log = LogFactory.getLog(OracleConfiguration.class);

	public OracleConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}
	}
}
