package nebula.data.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.inject.Inject;

import nebula.data.Editable;
import nebula.data.Entity;
import nebula.data.TransactionCaller;
import nebula.data.db.DbConfiguration;
import nebula.data.db.DbPersister;
import nebula.lang.Field;
import nebula.lang.Type;
import nebula.lang.TypeStandalone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DbDataRepos extends DefaultDataRepos {
    private Log log = LogFactory.getLog(this.getClass());

	final DbConfiguration dbConfig;

	@Inject
	public DbDataRepos(TypeDatastore typeKind, DbConfiguration dbConfig) {
		super(typeKind);
		this.dbConfig = dbConfig;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected DataStoreEx loadDataStore(String name, Type type) {
		switch (type.getStandalone()) {
		case Flow:
			return new EntityDataStore(IdReaderBuilder.getIDReader(type), this, type);
		case Transaction:
		case Relation:
			if(type.getAttrs().containsKey("Complex")){
				Field complexField = null;
				for (Field f : type.getFields()) {					
					if(f.isArray() && f.getType().getStandalone() == TypeStandalone.Transaction){
						complexField = f;
					}
				}
				return new DbComplexTransactionEntityDataStore(this, type, (DbPersister<Entity>) dbConfig.getPersister(type,Entity.class), (DbPersister<Entity>) dbConfig.getPersister(complexField.getType(),Entity.class));				
			}else{
				return new DbTransactionEntityDataStore(this, type, (DbPersister<Entity>) dbConfig.getPersister(type,Entity.class));				
			}
		case Master:
		default:
			return new DbMasterEntityDataStore(this, type, dbConfig.getPersister(type,Entity.class));
		}
	}


    @Override
    public void save(TransactionCaller caller) throws Exception {
        DefaultDataSession session = new DefaultDataSession(){
            @Override
            public void flush() {
                lock.lock();
                Connection conn = null;
                try {
                	conn = dbConfig.openConnection();
                	conn.setAutoCommit(false);
                    for (Editable e : changes) {
                        e.apply();
                    }
                    conn.commit();
                    conn.setAutoCommit(true);
                   
                    changes.clear();
                } catch (SQLException e) {
                    log.error(e);
                    throw new RuntimeException(e);
                } finally {
                	dbConfig.closeConnection(conn);
                    lock.unlock();
                }
            }
            
        };
        caller.exec(session);
    }
}
