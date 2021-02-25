package rocky.ctrl;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import rocky.ctrl.RockyController.RockyControllerRoleType;

public class AdvancedLCVDStorageTest {

	@Test
	public void testSimpleReadWrite() throws ExecutionException, InterruptedException {
		System.out.println("entered testSimpleReadWrite");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new AdvancedLCVDStorage("testing");
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
	
}
