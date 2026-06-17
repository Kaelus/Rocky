package rocky.ctrl.cloud;

import java.io.IOException;
import java.text.ParseException;
import java.util.SortedMap;

public interface GenericKeyValueStore {

	// close the key value store
	public void finish() throws IOException;

	public byte[] get(String key) throws IOException;

	public SortedMap<String, byte[]> get(String start, String end) throws IOException, ParseException;
	
	public void put(String key, byte[] value) throws IOException;

	public void remove(String key);
	
	public void clean();

}

