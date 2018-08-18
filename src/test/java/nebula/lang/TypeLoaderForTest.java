package nebula.lang;


import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import org.antlr.runtime.RecognitionException;

public class TypeLoaderForTest extends SystemTypeLoader {
	public TypeLoaderForTest() {
	}
	
	public List<TypeImp> testDefineNebula(Reader in) {
		try {
			return super.defineNebula(in);
		} catch (RecognitionException e) {
			log.error(e.getClass().getName(),e);
			throw new RuntimeException(e);
		}
	}

	public TypeImp load(String text) {
		try {
			return super.defineNebula(new StringReader(text)).get(0);
		} catch (RecognitionException e) {
			throw new RuntimeException(e);
		}
	}
}
