package nebula.data.schema;

import java.util.List;

/**
 * 简单和外部持久化设备交互，不包含复杂缓存机制等 
 * @author wanglocal
 *
 * @param <T>
 */
public interface DbPersister<T> {
	T get(Object... keys);

	void insert(T value);

	void update(T value, Object... keys);

	void deleteAll();

	List<T> query(Object... values);
	
	List<T> getAll();

	void delete(T value);

	void drop();

	void close();
	
	long getCurrentMaxID();

}
