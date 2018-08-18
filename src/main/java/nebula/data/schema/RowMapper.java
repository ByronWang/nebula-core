package nebula.data.schema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public interface RowMapper<T> {
	T map(ResultSet result) throws Exception;

	int push(PreparedStatement prepareStatement, T obj) throws Exception;
}
