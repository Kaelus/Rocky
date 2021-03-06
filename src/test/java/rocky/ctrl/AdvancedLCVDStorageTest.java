package rocky.ctrl;

import java.io.IOException;
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
	
	@Test
	public void testMultiBlockReadWrite() throws ExecutionException, InterruptedException {
		System.out.println("entered testMultiBlockReadWrite");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new AdvancedLCVDStorage("testing");
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
	
	@Test
	public void testCloudFlusherBlockDataUpdate() {
		System.out.println("entered testCloudFlusherBlockDataUpdate");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000;
		BasicLCVDStorage storage = new AdvancedLCVDStorage("testing");
		RockyController.role = RockyControllerRoleType.Owner;
		storage.connect();
		storage.cloudPackageManagerThread.start();
		storage.blockDataStore.remove("0");
		storage.blockDataStore.remove("1");
		storage.blockDataStore.remove("4");
		storage.write("hello world 0".getBytes(), 0);
		storage.write("hello world 1".getBytes(), 512);
		storage.write("hello world 4".getBytes(), 2048);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] actualBlock = null;
		try {
			actualBlock = storage.blockDataStore.get("0");
			Assert.assertArrayEquals("hello world 0".getBytes(), actualBlock);
			actualBlock = storage.blockDataStore.get("1");
			Assert.assertArrayEquals("hello world 1".getBytes(), actualBlock);
			actualBlock = storage.blockDataStore.get("4");
			Assert.assertArrayEquals("hello world 4".getBytes(), actualBlock);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		storage.disconnect();
		System.out.println("Finishing testCloudFlusherBlockDataUpdate");
	}
	
}
