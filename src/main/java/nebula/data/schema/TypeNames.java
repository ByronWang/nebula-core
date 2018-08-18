package nebula.data.schema;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import nebula.lang.RawTypes;

/*
 * 根据内部类型构建实际DB类型的字段说明
 */
public class TypeNames {

	private Map<RawTypes, Map<Integer, String>> weighted = new HashMap<RawTypes, Map<Integer, String>>();
	private Map<RawTypes, String> defaults = new HashMap<RawTypes, String>();

	public String get(RawTypes typecode) {
		String result = defaults.get( typecode );
		if (result==null) throw new RuntimeException("No Dialect mapping for JDBC type: " + typecode);
		return result;
	}

	public String get(RawTypes typeCode, int size, int precision, int scale)  {
		Map<Integer, String> map = weighted.get( typeCode );
		if ( map!=null && map.size()>0 ) {
			// iterate entries ordered by capacity to find first fit
			for (Map.Entry<Integer, String> entry: map.entrySet()) {
				if ( size <= entry.getKey() ) {
					return replace( entry.getValue(), size, precision, scale );
				}
			}
		}
		return replace( get(typeCode), size, precision, scale );
	}
	
	private static String replace(String type, int size, int precision, int scale) {
		type = type.replaceFirst("$s", Integer.toString(scale) );
		type = type.replaceFirst("$l", Long.toString(size) );
		return type.replaceFirst("$p", Integer.toString(precision) );
	}

	public void put(RawTypes typecode, int capacity, String value) {
		Map<Integer, String> map = weighted.get( typecode );
		if (map == null) {// add new ordered map
			map = new TreeMap<Integer, String>();
			weighted.put( typecode, map );
		}
		map.put(capacity, value);
	}

	public void put(RawTypes typecode, String value) {
		defaults.put( typecode, value );
	}
}






