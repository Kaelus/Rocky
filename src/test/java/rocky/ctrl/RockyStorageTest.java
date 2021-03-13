package rocky.ctrl;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import rocky.ctrl.RockyController.RockyControllerRoleType;

public class RockyStorageTest {

	@Test
	public void testSimpleReadWrite() throws ExecutionException, InterruptedException {
		System.out.println("entered testSimpleReadWrite");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new RockyStorage("testing");
		RockyController.role = RockyControllerRoleType.Owner;
		storage.connect();
		byte[] buffer = new byte[512];
		byte[] srcBytes = "hello world".getBytes();
		System.arraycopy(srcBytes, 0, buffer, 0, srcBytes.length);
		byte[] origBuffer = buffer.clone();
		storage.write(buffer, 0);
		storage.flush();
		buffer = new byte[512];
		storage.disconnect();
		Assert.assertNotEquals(origBuffer, buffer);
		storage.connect();
		storage.read(buffer, 0);
		storage.disconnect();
		Assert.assertArrayEquals(origBuffer, buffer);
		System.out.println("Finishing testSimpleReadWrite");		
	}
	
	@Test
	public void testMultiBlockReadWrite() throws ExecutionException, InterruptedException {
		System.out.println("entered testMultiBlockReadWrite");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new RockyStorage("testing");
		RockyController.role = RockyControllerRoleType.Owner;
		storage.connect();
		byte[] buffer = new byte[2048]; 
		for (int i = 0; i < (2048 / 8); i++) {
			System.arraycopy("hello wo".getBytes(), 0, buffer, i * 8, 8);
		}
		storage.write(buffer, 0);
		storage.flush();
		byte[] bufferClone = buffer.clone();
		buffer = new byte[2048];
		storage.disconnect();
		Assert.assertNotEquals(bufferClone, buffer);
		storage.connect();
		storage.read(buffer, 0);
		storage.disconnect();
		Assert.assertArrayEquals(bufferClone, buffer);
		System.out.println("Finishing testMultiBlockReadWrite");
	}
	
}
