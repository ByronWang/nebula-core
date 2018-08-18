package nebula.data;

public interface DataHelper<T, I, O> {
	<X extends T> X readFrom(X d, I in);

	<X extends T> void stringifyTo(X d, O o);
}
