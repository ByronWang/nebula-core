package nebula.data.db.serializer;

import nebula.data.Entity;

public interface FieldSerializer<T extends Object, I, O> {
	int input(I in,int pos, Entity parent, T now) throws Exception;

	int inputWithoutCheck(I in,int pos, Entity parent) throws Exception;

	int output(O out, T value,int pos) throws Exception;
}
