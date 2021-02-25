package rocky.ctrl;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.utils.Timestamper;

/**
 * This class represent generic key-value storage that is a wrapper of leveldb.
 *  
 * Supported type for Key:
 *   - String
 *   
 * Supported type for Value:
 *   - byte[]
 *  
 * @author ben
 *
 */
public class ValueStorageLevelDB implements GenericKeyValueStore {

	private DB db; 
	
	public ValueStorageLevelDB(String filename) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		db = factory.open(new File(filename), options);
	}
		
	public void finish() throws IOException {
		// Make sure you close the db to shutdown the 
		// database and avoid resource leaks.
		db.close();
	}
	
	public byte[] get(String key) {
		return db.get(bytes(key));
	}
	
	public SortedMap<String, byte[]> get(String start, String end) throws IOException, ParseException {
		SortedMap<String, byte[]> histSeg = new TreeMap<String, byte[]>();
		DBIterator iterator = db.iterator();
		try {
			for (iterator.seek(bytes(start)); iterator.hasNext(); iterator.next()) {
				//String key = new String(iterator.peekNext().getKey(), "UTF-8");
			    //String value = new String(iterator.peekNext().getValue(), "UTF-8");
			    //System.out.println(key+" = "+value);
				String nextKey = new String(iterator.peekNext().getKey(), "UTF-8");
				if (Timestamper.compareTimestamp(nextKey, end) > 0) {
					break;
				} else {
					histSeg.put(nextKey, iterator.peekNext().getValue());
				}
			}
		} finally {
			iterator.close();
		}
		return histSeg;
	}
	
	public void put(String key, byte[] value) {
		db.put(bytes(key), value);
	}
	
	public void remove(String key) {
		db.delete(bytes(key));
	}
	
	public static void main(String[] args) throws IOException {
		
		ValueStorageLevelDB vs = new ValueStorageLevelDB("log/ValueStorageGeneric-test");
		byte[] buf = vs.get("hello");
		if (buf == null)
			System.out.println("There is no such key=hello exist currently.");
		else
			System.out.println("For key=hello, returned value=" + new String(buf));
		
		vs.put("hello", "world".getBytes());
		buf = vs.get("hello");
		System.out.println("For key=hello, returned value=" + new String(buf));
		
		vs.put("goodbye", "cruel world".getBytes());
		buf = vs.get("goodbye");
		System.out.println("For key=goodbye, returned value=" + new String(buf));
		
		vs.remove("hello");
		buf = vs.get("hello");
		if (buf != null) {
			System.out.println("For key=hello, returned value=" + new String(buf));
		} else {
			System.out.println("There is no such key=hello exist currently.");
		}

	}
	
	
}

