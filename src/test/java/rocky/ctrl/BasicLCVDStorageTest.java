package rocky.ctrl;

import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import rocky.ctrl.BasicLCVDStorage.CloudFlusher;
import rocky.ctrl.utils.ByteUtils;

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
	
	@Test
	public void testGetEpoch() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		storage.blockDataStore.remove("EpochCount");
		long epoch = storage.getEpoch();
		Assert.assertEquals(0, epoch);
		try {
			storage.blockDataStore.put("EpochCount", ByteUtils.longToBytes(999));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		epoch = storage.getEpoch();
		Assert.assertEquals(999, epoch);
	}

	@Test
	public void testCloudPackageManagerWriteMapUpdate() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		byte[] buffer = "hello world 0".getBytes();
		storage.write(buffer, 0);
		buffer = "hello world 1".getBytes();
		storage.write(buffer, 512);
		buffer = "hello world 4".getBytes();
		storage.write(buffer, 2048);
		storage.flush();
		byte[] actualBlock = storage.writeMap.get(0);
		Assert.assertArrayEquals("hello world 0".getBytes(), actualBlock);
		actualBlock = storage.writeMap.get(1);
		Assert.assertArrayEquals("hello world 1".getBytes(), actualBlock);
		actualBlock = storage.writeMap.get(2);
		Assert.assertArrayEquals(null, actualBlock);
		actualBlock = storage.writeMap.get(4);
		Assert.assertArrayEquals("hello world 4".getBytes(), actualBlock);
		buffer = "hello world 4 new".getBytes();
		storage.write(buffer, 2048);
		storage.flush();
		actualBlock = storage.writeMap.get(4);
		Assert.assertArrayEquals("hello world 4 new".getBytes(), actualBlock);
		storage.disconnect();
	}
	
	@Test
	public void testCloudFlusherBlockDataUpdate() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
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
	}
	
	@Test
	public void testCloudFlusherDirtyBitmapAndEpochUpdate() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.blockDataStore.remove("EpochCount");
		long epoch = storage.getEpoch();
		Assert.assertEquals(0, epoch);
		BasicLCVDStorage.epochCnt = 0;
		storage.connect();
		
		storage.write("hello world 0".getBytes(), 0);
		storage.write("hello world 1".getBytes(), 512);
		storage.write("hello world 4".getBytes(), 2048);

		BitSet dBmClone = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		Assert.assertTrue(dBmClone.get(0));
		Assert.assertTrue(dBmClone.get(1));
		Assert.assertFalse(dBmClone.get(2));
		Assert.assertFalse(dBmClone.get(3));
		Assert.assertTrue(dBmClone.get(4));
		
		CloudFlusher flusher = storage.new CloudFlusher();
		flusher.run();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		storage.write("hello world 2".getBytes(), 1024);
		storage.write("hello world 4".getBytes(), 2048);
		flusher.run();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		byte[] dBmBytes = null;
		try {
			dBmBytes = storage.dBmStore.get("0-bitmap");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BitSet dBm = BitSet.valueOf(dBmBytes);
		Assert.assertTrue(dBm.get(0));
		Assert.assertTrue(dBm.get(1));
		Assert.assertFalse(dBm.get(2));
		Assert.assertFalse(dBm.get(3));
		Assert.assertTrue(dBm.get(4));
		
		try {
			dBmBytes = storage.dBmStore.get("1-bitmap");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dBm = BitSet.valueOf(dBmBytes);
		Assert.assertFalse(dBm.get(0));
		Assert.assertFalse(dBm.get(1));
		Assert.assertTrue(dBm.get(2));
		Assert.assertFalse(dBm.get(3));
		Assert.assertTrue(dBm.get(4));
		storage.disconnect();
	}
}
