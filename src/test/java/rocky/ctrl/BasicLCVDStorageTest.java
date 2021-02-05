package rocky.ctrl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import rocky.ctrl.BasicLCVDStorage.CloudFlusher;
import rocky.ctrl.BasicLCVDStorage.Prefetcher;
import rocky.ctrl.RockyController.RockyControllerRoleType;
import rocky.ctrl.utils.ByteUtils;

public class BasicLCVDStorageTest {
		
	@Test
	public void testSimpleReadWrite() throws ExecutionException, InterruptedException {
		System.out.println("entered testSimpleReadWrite");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		Storage storage = new BasicLCVDStorage("testing");
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
		Storage storage = new BasicLCVDStorage("testing");
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
	public void testGetEpoch() {
		System.out.println("entered testGetEpoch");
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
		System.out.println("Finishing testGetEpoch");
		storage.disconnect();
	}

	@Test
	public void testCloudPackageManagerWriteMapUpdate() {
		System.out.println("entered testCloudPackageManagerWriteMapUpdate");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		storage.cloudPackageManagerThread.start();
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
		System.out.println("Finishing testCloudPackageManagerWriteMapUpdate");
	}
	
	@Test
	public void testCloudFlusherBlockDataUpdate() {
		System.out.println("entered testCloudFlusherBlockDataUpdate");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
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
	
	@Test
	public void testCloudFlusherDirtyBitmapAndEpochUpdate() {
		System.out.println("entered testCloudFlusherDirtyBitmapAndEpochUpdate");
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.blockDataStore.remove("EpochCount");
		long epoch = storage.getEpoch();
		Assert.assertEquals(0, epoch);
		BasicLCVDStorage.epochCnt = 0;
		storage.connect();
		storage.cloudPackageManagerThread.start();		
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
		System.out.println("Finishing testCloudFlusherDirtyBitmapAndEpochUpdate");
	}
	
	@Test
	public void testFetchNextEpochBitmaps() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		RockyController.nodeID = "tn1";
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.blockDataStore.remove("EpochCount");
		storage.blockDataStore.remove("PrefetchedEpoch-" + RockyController.nodeID);
		long epoch = storage.getEpoch();
		Assert.assertEquals(0, epoch);
		BasicLCVDStorage.epochCnt = 0;
		storage.connect();
		storage.cloudPackageManagerThread.start();
		storage.blockDataStore.remove("PrefetchedEpoch-" + RockyController.nodeID);
		byte[] myPrefetchedEpochBytes = null; 
		try {
			storage.blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(0));
			myPrefetchedEpochBytes = storage.blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long myPrefetchedEpoch = ByteUtils.bytesToLong(myPrefetchedEpochBytes);
		Assert.assertEquals(0, myPrefetchedEpoch);
		
		// clean up
		for (int i = 0; i < 5; i++) {
			storage.blockDataStore.remove(i + "-bitmap");
		}
		
		// test base case: EpochCount = 0, PrefetchedEpoch = 0
		List<BitSet> epochBitmapList = storage.fetchNextEpochBitmaps(0, myPrefetchedEpoch);
		Assert.assertNull(epochBitmapList);
		byte[] actualEpochBytes = null;
		byte[] actualPrefetchedEpochBytes = null;
		long actualEpoch = -1;
		long actualPrefetchedEpoch = -1;
		try {
			actualEpochBytes = storage.blockDataStore.get("EpochCount");
			actualPrefetchedEpochBytes = storage.blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		actualEpoch = ByteUtils.bytesToLong(actualEpochBytes);
		actualPrefetchedEpoch = ByteUtils.bytesToLong(actualPrefetchedEpochBytes);
		Assert.assertEquals(epoch, actualEpoch);
		Assert.assertEquals(myPrefetchedEpoch, actualPrefetchedEpoch);
		
		// test with several epoch bitmaps to fetch: EpochCount = 2, PrefetchedEpoch = 0
		storage.write("hello world 0".getBytes(), 0);
		storage.write("hello world 1".getBytes(), 512);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BitSet dBmClone1 = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone1 = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		CloudFlusher flusher = storage.new CloudFlusher();
		flusher.run();
		
		storage.write("hello world 1".getBytes(), 512);
		storage.write("hello world 2".getBytes(), 1024);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BitSet dBmClone2 = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone2 = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		flusher.run();
		
		storage.write("hello world 3".getBytes(), 1536);
		storage.write("hello world 4".getBytes(), 2048);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BitSet dBmClone3 = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone3 = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		flusher.run();
		
		try {
			actualEpochBytes = storage.blockDataStore.get("EpochCount");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		actualEpoch = ByteUtils.bytesToLong(actualEpochBytes);
		Assert.assertEquals(3, actualEpoch);
		Assert.assertEquals(0, myPrefetchedEpoch);
		epochBitmapList = storage.fetchNextEpochBitmaps(actualEpoch, myPrefetchedEpoch);
		Assert.assertEquals(3, epochBitmapList.size());

		Assert.assertEquals(dBmClone1, epochBitmapList.get(0));
		Assert.assertEquals(dBmClone2, epochBitmapList.get(1));
		Assert.assertEquals(dBmClone3, epochBitmapList.get(2));

		// test fetching for more than once: EpochCount = 4, PrefetchedEpoch = 2
		storage.write("hello world 0".getBytes(), 0);
		storage.write("hello world 1".getBytes(), 512);
		storage.write("hello world 2".getBytes(), 1024);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BitSet dBmClone4 = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone4 = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		flusher.run();
		
		storage.write("hello world 2".getBytes(), 1024);
		storage.write("hello world 3".getBytes(), 1536);
		storage.write("hello world 4".getBytes(), 2048);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		BitSet dBmClone5 = null;
		synchronized(BasicLCVDStorage.dirtyBitmap) {
			dBmClone5 = (BitSet) BasicLCVDStorage.dirtyBitmap.clone();
		}
		flusher.run();
		
		try {
			actualEpochBytes = storage.blockDataStore.get("EpochCount");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		actualEpoch = ByteUtils.bytesToLong(actualEpochBytes);
		Assert.assertEquals(5, actualEpoch);
		myPrefetchedEpoch = 3;
		epochBitmapList = storage.fetchNextEpochBitmaps(BasicLCVDStorage.epochCnt, myPrefetchedEpoch);
		Assert.assertEquals(2, epochBitmapList.size());

		Assert.assertEquals(dBmClone4, epochBitmapList.get(0));
		Assert.assertEquals(dBmClone5, epochBitmapList.get(1));

		storage.stopCloudPackageManager();
		storage.disconnect();
	}
	
	@Test
	public void testGetPrefetchBlockIDList() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		
		BitSet bitmap1 = new BitSet();
		BitSet bitmap2 = new BitSet();
		BitSet bitmap3 = new BitSet();
		
		List<BitSet> bitmapList = null;
		
		// test null bitmapList
		HashSet<Integer> blockIDList = storage.getPrefetchBlockIDList(bitmapList);
		Assert.assertNull(blockIDList);
		
		// test empty bitmapList
		bitmapList = new ArrayList<BitSet>();
		blockIDList = storage.getPrefetchBlockIDList(bitmapList);
		Assert.assertNull(blockIDList);
		
		// test bitmapList containing some fully unset bitmaps
		bitmapList.add(bitmap1);
		bitmapList.add(bitmap2);
		bitmapList.add(bitmap3);
		blockIDList = storage.getPrefetchBlockIDList(bitmapList);
		Assert.assertNull(blockIDList);
		
		// test bitmapList containing some partially set bitmaps
		bitmap1.set(1);
		bitmap1.set(3);
		bitmap1.set(5);
		bitmap3.set(0);
		bitmap3.set(3);
		blockIDList = storage.getPrefetchBlockIDList(bitmapList);
		HashSet<Integer> expectedList = new HashSet<Integer>();
		expectedList.add(0);
		expectedList.add(1);
		expectedList.add(3);
		expectedList.add(5);
		Assert.assertEquals(expectedList, blockIDList);
		
		storage.disconnect();
	}
	
	@Test
	public void testPrefetchBlocks() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		storage.cloudPackageManagerThread.start();
		byte[] buffer1 = new byte[512];
		byte[] buffer2 = new byte[512];
		byte[] buffer3 = new byte[512];
		byte[] buffer4 = new byte[512];
		byte[] buffer5 = new byte[512];
		byte[] srcBytes1 = "Local Nothing 1".getBytes();
		byte[] srcBytes2 = "Local Nothing 2".getBytes();
		byte[] srcBytes3 = "Local Nothing 3".getBytes();
		byte[] srcBytes4 = "Local Nothing 4".getBytes();
		byte[] srcBytes5 = "Local Nothing 5".getBytes();
		System.arraycopy(srcBytes1, 0, buffer1, 0, srcBytes1.length);
		System.arraycopy(srcBytes2, 0, buffer2, 0, srcBytes2.length);
		System.arraycopy(srcBytes3, 0, buffer3, 0, srcBytes3.length);
		System.arraycopy(srcBytes4, 0, buffer4, 0, srcBytes4.length);
		System.arraycopy(srcBytes5, 0, buffer5, 0, srcBytes5.length);
		byte[] buffer4Clone = buffer4.clone();
		byte[] buffer5Clone = buffer5.clone();
		storage.prefetchWrite(buffer1, 512);
		storage.prefetchWrite(buffer2, 1024);
		storage.prefetchWrite(buffer3, 1536);
		storage.prefetchWrite(buffer4, 2048);
		storage.prefetchWrite(buffer5, 2560);
		
		byte[] cloudBuffer1 = new byte[512];
		byte[] cloudBuffer2 = new byte[512];
		byte[] cloudBuffer3 = new byte[512];
		byte[] cloudBuffer4 = new byte[512];
		byte[] cloudBuffer5 = new byte[512];
		srcBytes1 = "Cloud Nothing 1".getBytes();
		srcBytes2 = "Cloud Nothing 2".getBytes();
		srcBytes3 = "Cloud Nothing 3".getBytes();
		srcBytes4 = "Cloud Nothing 4".getBytes();
		srcBytes5 = "Cloud Nothing 5".getBytes();
		System.arraycopy(srcBytes1, 0, cloudBuffer1, 0, srcBytes1.length);
		System.arraycopy(srcBytes2, 0, cloudBuffer2, 0, srcBytes2.length);
		System.arraycopy(srcBytes3, 0, cloudBuffer3, 0, srcBytes3.length);
		System.arraycopy(srcBytes4, 0, cloudBuffer4, 0, srcBytes4.length);
		System.arraycopy(srcBytes5, 0, cloudBuffer5, 0, srcBytes5.length);
		try {
			storage.blockDataStore.put("1", cloudBuffer1);
			storage.blockDataStore.put("2", cloudBuffer2);
			storage.blockDataStore.put("3", cloudBuffer3);
			storage.blockDataStore.put("4", cloudBuffer4);
			storage.blockDataStore.put("5", cloudBuffer5);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		HashSet<Integer> blockIDList = new HashSet<Integer>();
		blockIDList.add(1);
		blockIDList.add(2);
		blockIDList.add(3);
		storage.prefetchBlocks(storage, blockIDList);

		byte[] actualBuffer1 = new byte[512];
		byte[] actualBuffer2 = new byte[512];
		byte[] actualBuffer3 = new byte[512];
		byte[] actualBuffer4 = new byte[512];
		byte[] actualBuffer5 = new byte[512];
		storage.localRead(actualBuffer1, 512);
		storage.localRead(actualBuffer2, 1024);
		storage.localRead(actualBuffer3, 1536);
		storage.localRead(actualBuffer4, 2048);
		storage.localRead(actualBuffer5, 2560);

		Assert.assertArrayEquals(cloudBuffer1, actualBuffer1);
		Assert.assertArrayEquals(cloudBuffer2, actualBuffer2);
		Assert.assertArrayEquals(cloudBuffer3, actualBuffer3);
		Assert.assertArrayEquals(buffer4Clone, actualBuffer4);
		Assert.assertArrayEquals(buffer5Clone, actualBuffer5);
		
		storage.disconnect();

	}
	
	@Test
	public void testPrefetcherRun() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		storage.blockDataStore.remove("EpochCount");
		storage.blockDataStore.remove("PrefetchedEpoch-" + RockyController.nodeID);
				
		// Populate the local storage
		byte[] buffer1 = new byte[512];
		byte[] buffer2 = new byte[512];
		byte[] buffer3 = new byte[512];
		byte[] buffer4 = new byte[512];
		byte[] buffer5 = new byte[512];
		byte[] srcBytes1 = "Local Nothing 1".getBytes();
		byte[] srcBytes2 = "Local Nothing 2".getBytes();
		byte[] srcBytes3 = "Local Nothing 3".getBytes();
		byte[] srcBytes4 = "Local Nothing 4".getBytes();
		byte[] srcBytes5 = "Local Nothing 5".getBytes();
		System.arraycopy(srcBytes1, 0, buffer1, 0, srcBytes1.length);
		System.arraycopy(srcBytes2, 0, buffer2, 0, srcBytes2.length);
		System.arraycopy(srcBytes3, 0, buffer3, 0, srcBytes3.length);
		System.arraycopy(srcBytes4, 0, buffer4, 0, srcBytes4.length);
		System.arraycopy(srcBytes5, 0, buffer5, 0, srcBytes5.length);
		storage.prefetchWrite(buffer1, 512);
		storage.prefetchWrite(buffer2, 1024);
		storage.prefetchWrite(buffer3, 1536);
		storage.prefetchWrite(buffer4, 2048);
		storage.prefetchWrite(buffer5, 2560);
		
		// Populate the remote storage
		byte[] cloudBuffer1 = new byte[512];
		byte[] cloudBuffer2 = new byte[512];
		byte[] cloudBuffer3 = new byte[512];
		byte[] cloudBuffer4 = new byte[512];
		byte[] cloudBuffer5 = new byte[512];
		srcBytes1 = "Cloud Nothing 1".getBytes();
		srcBytes2 = "Cloud Nothing 2".getBytes();
		srcBytes3 = "Cloud Nothing 3".getBytes();
		srcBytes4 = "Cloud Nothing 4".getBytes();
		srcBytes5 = "Cloud Nothing 5".getBytes();
		System.arraycopy(srcBytes1, 0, cloudBuffer1, 0, srcBytes1.length);
		System.arraycopy(srcBytes2, 0, cloudBuffer2, 0, srcBytes2.length);
		System.arraycopy(srcBytes3, 0, cloudBuffer3, 0, srcBytes3.length);
		System.arraycopy(srcBytes4, 0, cloudBuffer4, 0, srcBytes4.length);
		System.arraycopy(srcBytes5, 0, cloudBuffer5, 0, srcBytes5.length);
		try {
			storage.blockDataStore.put("1", cloudBuffer1);
			storage.blockDataStore.put("2", cloudBuffer2);
			storage.blockDataStore.put("3", cloudBuffer3);
			storage.blockDataStore.put("4", cloudBuffer4);
			storage.blockDataStore.put("5", cloudBuffer5);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Increase Epoch and Uploads Epoch Bitmaps
		try {
			storage.blockDataStore.put("EpochCount", ByteUtils.longToBytes(3));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BitSet bitmap1 = new BitSet();
		BitSet bitmap2 = new BitSet();
		BitSet bitmap3 = new BitSet();
		bitmap1.set(1);
		bitmap1.set(2);
		bitmap2.set(3);
		bitmap3.set(4);
		bitmap3.set(5);
		byte[] dmBytes1 = bitmap1.toByteArray();
		byte[] dmBytes2 = bitmap2.toByteArray();
		byte[] dmBytes3 = bitmap3.toByteArray();
		try {
			storage.dBmStore.put(Long.toString(1) + "-bitmap", dmBytes1);
			storage.dBmStore.put(Long.toString(2) + "-bitmap", dmBytes2);		
			storage.dBmStore.put(Long.toString(3) + "-bitmap", dmBytes3);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Assert.assertEquals(0, BasicLCVDStorage.prefetchedEpoch);
		long actualEpochCnt = storage.getEpoch();
		Assert.assertEquals(3, actualEpochCnt);
		
		storage.prefetcherThread.start();
		
		System.out.println("Wait to give a prefetcher enough time");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		byte[] actualBuffer1 = new byte[512];
		byte[] actualBuffer2 = new byte[512];
		byte[] actualBuffer3 = new byte[512];
		byte[] actualBuffer4 = new byte[512];
		byte[] actualBuffer5 = new byte[512];
		storage.localRead(actualBuffer1, 512);
		storage.localRead(actualBuffer2, 1024);
		storage.localRead(actualBuffer3, 1536);
		storage.localRead(actualBuffer4, 2048);
		storage.localRead(actualBuffer5, 2560);

		Assert.assertArrayEquals(cloudBuffer1, actualBuffer1);
		Assert.assertArrayEquals(cloudBuffer2, actualBuffer2);
		Assert.assertArrayEquals(cloudBuffer3, actualBuffer3);
		Assert.assertArrayEquals(cloudBuffer4, actualBuffer4);
		Assert.assertArrayEquals(cloudBuffer5, actualBuffer5);
		
		Assert.assertEquals(3, BasicLCVDStorage.prefetchedEpoch);
		byte[] actualPrefetchedEpochBytesOnCloud = null;
		try {
			actualPrefetchedEpochBytesOnCloud = storage.blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
			long actualPrefetchedEpochOnCloud = ByteUtils.bytesToLong(actualPrefetchedEpochBytesOnCloud);
			Assert.assertEquals(3, actualPrefetchedEpochOnCloud);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		storage.stopPrefetcher();
		storage.disconnect();
	}
	
	@Test
	public void testRoleSwitching() {
		RockyController.backendStorage = RockyController.BackendStorageType.DynamoDBLocal;
		RockyController.epochPeriod = 1000000;
		BasicLCVDStorage storage = new BasicLCVDStorage("testing");
		storage.connect();
		
		RockyControllerRoleType curRole = null;
		synchronized(storage.roleSwitcherThread) {
			curRole = RockyController.role;
		}
		Assert.assertEquals(RockyControllerRoleType.None, curRole);
		
		// None to NonOwner
		storage.cui.invokeRoleSwitching("2");
		synchronized(storage.roleSwitcherThread) {
			curRole = RockyController.role;
		}
		Assert.assertEquals(RockyControllerRoleType.NonOwner, curRole);
		
		// NonOwner to Owner
		storage.cui.invokeRoleSwitching("3");
		synchronized(storage.roleSwitcherThread) {
			curRole = RockyController.role;
		}
		Assert.assertEquals(RockyControllerRoleType.Owner, curRole);
		
		// Owner to NonOwner
		storage.cui.invokeRoleSwitching("2");
		synchronized(storage.roleSwitcherThread) {
			curRole = RockyController.role;
		}
		Assert.assertEquals(RockyControllerRoleType.NonOwner, curRole);

		
		// NonOwner to None
		storage.cui.invokeRoleSwitching("1");
		synchronized(storage.roleSwitcherThread) {
			curRole = RockyController.role;
		}
		Assert.assertEquals(RockyControllerRoleType.None, curRole);
		
		storage.disconnect();
	}

}