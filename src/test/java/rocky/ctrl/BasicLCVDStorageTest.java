package rocky.ctrl;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

public class BasicLCVDStorageTest {
		
	@Test
	public void testSimpleReadWrite() throws ExecutionException, InterruptedException {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new BasicLCVDStorage("testing");
		storage.connect();
		byte[] buffer = "hello world".getBytes();
		storage.write(buffer, 0);
		storage.flush();
		buffer = new byte[buffer.length];
		storage.disconnect();
		Assert.assertNotEquals("hello world".getBytes(), buffer);
		storage.connect();
		storage.read(buffer, 0);
		storage.disconnect();
		Assert.assertArrayEquals("hello world".getBytes(), buffer);
		
	}

	@Test
	public void testMultiBlockReadWrite() throws ExecutionException, InterruptedException {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new BasicLCVDStorage("testing");
		storage.connect();
		byte[] buffer = new byte[2048]; 
		for (int i = 0; i < (2048 / 8); i++) {
			System.arraycopy("hello wo".getBytes(), 0, buffer, i * 8, 8);
		}
		storage.write(buffer, 0);
		storage.flush();
		byte[] bufferClone = buffer.clone();
		buffer = new byte[buffer.length];
		storage.disconnect();
		Assert.assertNotEquals(bufferClone, buffer);
		storage.connect();
		storage.read(buffer, 0);
		storage.disconnect();
		Assert.assertArrayEquals(bufferClone, buffer);
	}
	
}
