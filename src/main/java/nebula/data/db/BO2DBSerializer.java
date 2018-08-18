package nebula.data.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public interface BO2DBSerializer<T> {
	T fromDb(ResultSet result) throws Exception;
	int toDb(PreparedStatement prepareStatement, T obj) throws Exception;
}
