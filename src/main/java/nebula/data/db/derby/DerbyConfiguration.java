package nebula.data.db.derby;

import java.sql.DriverManager;
import java.sql.SQLException;

import nebula.data.db.DbConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DerbyConfiguration extends DbConfiguration {
	private static final Logger log = LoggerFactory.getLogger(DerbyConfiguration.class);

	public DerbyConfiguration(String driverClass, String url, String userName, String password) {
		super(driverClass, url, userName, password);
		if (log.isTraceEnabled()) {
			log.trace("init OracleConfiguration");
		}
	}

	@Override
	public void shutdown() {
		super.shutdown();

		try { // perform a clean shutdown
			String shutdownUrl = this.url.replaceAll(";create=true", ";shutdown=true");
			DriverManager.getConnection(shutdownUrl);
			log.info("== shut down database s- " + shutdownUrl);
		} catch (SQLException se) {
		}
	}

	@Override
	protected void finalize() throws Throwable {
		this.shutdown();
	}

}
