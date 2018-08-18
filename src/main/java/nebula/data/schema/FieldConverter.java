package nebula.data.schema;

public interface FieldConverter<T extends Object, R, W> {
	T readFrom(R in, int index) throws Exception;

	T readFrom(R in, String name) throws Exception;

	void writeTo(String name, Object value, W out) throws Exception;

	void writeTo(int index, Object value, W out) throws Exception;
}
