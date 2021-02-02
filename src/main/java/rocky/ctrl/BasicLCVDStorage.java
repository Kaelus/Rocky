package rocky.ctrl;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.cloud.ValueStorageDynamoDB;
import rocky.ctrl.utils.ByteUtils;

public class BasicLCVDStorage extends FDBStorage {

	String nodeID;
	public static final long MAX_SIZE = 51200; // HARD-CODED  512 bytes * 100
	public static final int blockSize = 512;
	
	public static BitSet presenceBitmap;
	public static BitSet dirtyBitmap;

	public String pBmTableName = "presenceBitmapTable";
	public String dBmTableName = "dirtyBitmapTable";
	public String blockDataTableName = "blockDataTable";
	
	public GenericKeyValueStore pBmStore;
	public GenericKeyValueStore dBmStore;
	public GenericKeyValueStore blockDataStore;

	private Thread cloudPackageManagerThread;
	//private final AtomicBoolean running = new AtomicBoolean(false);
	private final BlockingQueue<WriteRequest> queue;
	public HashMap<Integer, byte[]> writeMap;

	private Thread roleSwitcherThread;
	
	public static long epochCnt;
	
	class WriteRequest {
		public byte[] buf;
		public long offset;
	}
	
//	private final String lcvdFilePath;
//	private final LongAdder writesStarted;
//	private final LongAdder writesComplete;
//	private final long size;
//	private final String exportName;
	
	// ToDo: variables for temporary usage; replace later with better ones
	protected static boolean opened = false;
	
	//// HARD-CODED for now
	//String dirName = "/home/ben/experiment/rocky/basic/localCopyVirtualDisks"; 
	//String lcvdFileName = "lcvd";
	//long diskSize = 64; //64 bytes
	
	public BasicLCVDStorage(String exportName) {
		super(exportName);
		if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDBLocal)) {
			pBmStore = new ValueStorageDynamoDB(pBmTableName, true);
			dBmStore = new ValueStorageDynamoDB(dBmTableName, true);
			blockDataStore = new ValueStorageDynamoDB(blockDataTableName, true);
		} else if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDB)) {
			pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			dBmStore = new ValueStorageDynamoDB(dBmTableName, false);
			blockDataStore = new ValueStorageDynamoDB(blockDataTableName, false);			
		} else {
			System.err.println("Error: Unknown backendStorageType");
 		   	System.exit(1);
		}
		int numBlock = (int) (size() / 512); //blocksize=512bytes
		presenceBitmap = new BitSet(numBlock);
		dirtyBitmap = new BitSet(numBlock);
		presenceBitmap.set(0, numBlock);
		queue = new LinkedBlockingDeque<WriteRequest>();
		epochCnt = getEpoch();
		writeMap = new HashMap<Integer, byte[]>();
		CloudPackageManager cpm = new CloudPackageManager(queue);
		cloudPackageManagerThread = new Thread(cpm);
		roleSwitcherThread = new Thread(new RoleSwitcher());
	}

	public long getEpoch() {
		long retLong = 0;
		byte[] epochBytes;
		try {
			epochBytes = blockDataStore.get("EpochCount");
			if (epochBytes == null) {
				System.out.println("EpochCount key is not allocated yet. Allocate it now");
				try {
					blockDataStore.put("EpochCount", ByteUtils.longToBytes(retLong));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else {
				retLong = ByteUtils.bytesToLong(epochBytes);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return retLong;
	}
	
	@Override
	public void connect() {
		// TODO Auto-generated method stub
//		if (!opened) {
//			opened = true;
//		} else {
//			throw new IllegalStateException("Volume " + exportName + " is already leased");
//		}
		super.connect();
		roleSwitcherThread.start();
	}

	@Override
	public void disconnect() {
//		if (opened) {
//			opened = false;		
//		} else {
//		      throw new IllegalStateException("Not connected to " + exportName);
//	    }
		super.disconnect();
		switchRole(RockyController.role, RockyController.RockyControllerRoleType.None);
		roleSwitcherThread.interrupt();
		try {
			roleSwitcherThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public CompletableFuture<Void> read(byte[] buffer, long offset) {
		// TODO Auto-generated method stub
//		File lcvdFile = new File(lcvdFilePath);
//		try {
//			RandomAccessFile raf = new RandomAccessFile( lcvdFile, "r" );
//			raf.seek(offset);
//			raf.readFully(buffer);
//			raf.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		return null;
		
		long firstBlock = offset / blockSize;
	    int length = buffer.length;
	    long lastBlock = (offset + length) / blockSize;
	    // we assume that buffer size to be blockSize when it is used
	    // as a block device properly. O.W. it can be smaller than that.
	    // To make it compatible with the original intention, when 
	    // we get buffer smaller than blockSize, we increment the loastBlock
	    // by one.
	    if (firstBlock == lastBlock) {
	    	lastBlock++; 
	    }
	    for (int i = (int) firstBlock; i < (int) lastBlock || firstBlock == lastBlock; i++) {
	    	byte[] blockData = new byte[blockSize];
	    	boolean isPresent = false;
	    	synchronized(presenceBitmap) {
	    		isPresent = presenceBitmap.get(i);
	    	}
	    	if (isPresent) {
				super.read(blockData, i * blockSize);
				System.arraycopy(blockData, 0, buffer, i * blockSize, blockSize);
			} else {
				// read from the cloud backend (slow path)
				try {
					blockData = blockDataStore.get(String.valueOf(i));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.arraycopy(blockData, 0, buffer, i * blockSize, blockSize);
				super.write(blockData, i * blockSize);
				super.flush();
				synchronized(presenceBitmap) {
					presenceBitmap.set(i);
				}
			}
	    }
		return super.read(buffer, offset);
	}

	@Override
	public CompletableFuture<Void> write(byte[] buffer, long offset) {
//		// TODO Auto-generated method stub
//		return null;
		
		long firstBlock = offset / blockSize;
		int length = buffer.length;
	    long lastBlock = (offset + length) / blockSize;
	    // we assume that buffer size to be blockSize when it is used
	    // as a block device properly. O.W. it can be smaller than that.
	    // To make it compatible with the original intention, when 
	    // we get buffer smaller than blockSize, we increment the loastBlock
	    // by one.
	    if (firstBlock == lastBlock) {
	    	lastBlock++; 
	    }
	    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
	    	System.out.println("setting dirtyBitmap for blockID=" + i);
	    	synchronized(dirtyBitmap) {
	    		dirtyBitmap.set(i);
	    	}
	    }
	    WriteRequest wr = new WriteRequest();
	    wr.buf = buffer;
	    wr.offset = offset;
	    try {
	    	//System.out.println("[BasicLCVDStorage] enqueuing WriteRequest for blockID=" 
	    	//		+ ((int) wr.offset / blockSize));
			queue.put(wr);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return super.write(buffer, offset);
	}

	@Override
	public CompletableFuture<Void> flush() {
//		// TODO Auto-generated method stub
//		return null;

		return super.flush();
	}

	@Override
	public long size() {
//		// TODO Auto-generated method stub
//		return 0;
		
		return super.size();
	}

	@Override
	public long usage() {
//		// TODO Auto-generated method stub
//		return 0;
		
		return super.usage();
	}
	
	/*
	 * Role switching
	 */
	
	public class RoleSwitcher implements Runnable {
		public void run() {
			System.out.println("[RoleSwitcher] entered run");
			try {
				while (true) {
					RockyController.RockyControllerRoleType prevRole = null;
					synchronized(RockyController.role) {
						prevRole = RockyController.role;
					}
					RockyController.role.wait();
					RockyController.RockyControllerRoleType newRole = null;
					synchronized(RockyController.role) {
						newRole = RockyController.role;
					}
					System.out.println("Role switching from " 
							+ prevRole + " to " + newRole);
					switchRole(prevRole, newRole);
				}
			} catch (Exception e) {
				
			}
		}
	}
	
	public void switchRole(RockyController.RockyControllerRoleType prevRole, 
			RockyController.RockyControllerRoleType newRole) {
		if (prevRole.equals(RockyController.RockyControllerRoleType.Owner) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner)) {
			stopCloudPackageManager();
			instantCloudFlushing();
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.None) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner)) {
			// ToDo
			

		} else if (prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner)) {
			// ToDo
			cloudPackageManagerThread.start();				
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.None)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner)) {
			cloudPackageManagerThread.start();				
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.Owner)
				&& newRole.equals(RockyController.RockyControllerRoleType.None)) {
			stopCloudPackageManager();
			instantCloudFlushing();
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.None)) {
			
		} else {
			System.err.println("ASSERT: unallowed role switching scenario");
			System.err.println("From=" + prevRole.toString() + " To=" + newRole.toString());
			System.exit(1);
		}
	}

	public void stopRoleSwitcher() {
		System.out.println("interrupting the role switcher thread to terminate");
		roleSwitcherThread.interrupt();
		try {
			roleSwitcherThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	
	/*
	 * Periodic Flushing
	 */

	public void instantCloudFlushing() {
		WriteRequest wr = null;
		while ((wr = queue.poll()) != null) {
			synchronized(writeMap) {
				writeMap.put((int) (wr.offset / blockSize), wr.buf);
			}
		}
		Thread lastFlusherThread = new Thread(new CloudFlusher());
		lastFlusherThread.start();
		try {
			lastFlusherThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void stopCloudPackageManager() {
		System.out.println("interrupting the cloud package manager thread to terminate");
		cloudPackageManagerThread.interrupt();
		try {
			cloudPackageManagerThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public class CloudPackageManager implements Runnable {
		private final BlockingQueue<WriteRequest> q;
		public CloudPackageManager(BlockingQueue<WriteRequest> q) { 
			this.q = q; 
		}
		public void run() {
			System.out.println("[CloudPackageManager] run entered");
			try {
				Timer timer = new Timer();
				while (true) { 
					timer.schedule(new CloudFlusher(), RockyController.epochPeriod);
					WriteRequest wr = q.take();
			    	//System.out.println("[CloudPackageManager] dequeued WriteRequest for blockID=" 
			    	//		+ ((int) wr.offset / blockSize));
					//System.err.println("[CloudPackageManager] writeMap lock acquire attempt");
			    	synchronized(writeMap) {
						//System.err.println("[CloudPackageManager] writeMap lock acquired");
						writeMap.put((int) (wr.offset / blockSize), wr.buf);
						//System.err.println("[CloudPackageManager] writeMap put for blockID=" 
						//		+ (int) (wr.offset / blockSize));
					}
					//System.err.println("[CloudPackageManager] writeMap lock released");
				}
			} catch (InterruptedException e) { 
				System.out.println("[CloudPackageManager] Get interrupted");
			}
			System.out.println("[CloudPackageManager] Terminating CloudPackageManager Thread");
		}
	}
	
	public class CloudFlusher extends TimerTask {
		public void run() {
			System.out.println("[CloudFlusher] Entered CloudFlusher run");
			//System.err.println("[CloudFlusher] writeMap lock acquire attempt");
			HashMap<Integer, byte[]> writeMapClone = null;
			BitSet dirtyBitmapClone = null;
			synchronized(writeMap) {
				//System.err.println("[CloudFlusher] writeMap lock acquired");
				synchronized (dirtyBitmap) {
					writeMapClone = (HashMap<Integer, byte[]>) writeMap.clone();
					dirtyBitmapClone = (BitSet) dirtyBitmap.clone();
					dirtyBitmap.clear();
				}				
			}
			//System.err.println("[CloudFlusher] writeMap lock released");
			for (Integer i : writeMapClone.keySet()) {
				byte[] buf = writeMapClone.get(i);
				try {
					blockDataStore.put(Integer.toString(i), buf);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			long curEpoch = epochCnt;
			byte[] dmBytes = dirtyBitmapClone.toByteArray();
			try {
				System.out.println("dBmStore put for epoch=" + curEpoch);
				dBmStore.put(Long.toString(curEpoch) + "-bitmap", dmBytes);
				blockDataStore.put("EpochCount", ByteUtils.longToBytes(curEpoch));
				epochCnt++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	
//	class EpochFlusher {
//
//		ScheduledExecutorService executorService;
//		
//		public EpochFlusher() {
//			executorService = Executors.newSingleThreadScheduledExecutor();
//		}
//		
//		public void startPeriodicFlushing(int epochPeriod) {
//			executorService.scheduleAtFixedRate(BasicLCVDStorage::periodicFlushToCloud, 0, epochPeriod, TimeUnit.SECONDS);
//		}
//		
//		public void stopPeriodicFlushing() {
//			executorService.shutdown();
//			try {
//				executorService.awaitTermination(60, TimeUnit.SECONDS);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//				System.err.println("awaitTermination is interrupted");
//				System.exit(1);
//			}
//		}
//		public void periodicFlushToCloud() {
//			RockyControllerRoleType myRole = RockyControllerRoleType.None;
//			synchronized(RockyController.role) {
//				myRole = RockyController.role;
//			}
//			BitSet dirtyBitmapClone;
//			synchronized(dirtyBitmap) {
//				dirtyBitmapClone = (BitSet) dirtyBitmap.clone();
//			}
//			if (myRole.equals(RockyControllerRoleType.Owner)) {
//				// writes dirty blocks to the cloud storage service
//				for (int i = 0; i < dirtyBitmapClone.length(); i++) {
//					if (dirtyBitmapClone.get(i)) {
//						byte[] blockDataToFlush = new byte[512];
//						NBDVolumeServer.storage.read(blockDataToFlush, i * blockSize);
//					}
//				}
//				
//				// uploads dirty bitmaps to the cloud storage service
//				
//				// unset bits in the dirty blocks for flushed dirty blocks
//
//			} else {
//				System.err.println("ASSERT: Not an Owner, nothing to flush periodically");
//				System.exit(1);
//			}
//		}
//	}
	
//	public static void periodicFlushToCloud() {
//		RockyControllerRoleType myRole = RockyControllerRoleType.None;
//		synchronized(RockyController.role) {
//			myRole = RockyController.role;
//		}
//		BitSet dirtyBitmapClone;
//		synchronized(dirtyBitmap) {
//			dirtyBitmapClone = (BitSet) dirtyBitmap.clone();
//		}
//		if (myRole.equals(RockyControllerRoleType.Owner)) {
//			// writes dirty blocks to the cloud storage service
//			for (int i = 0; i < dirtyBitmapClone.length(); i++) {
//				if (dirtyBitmapClone.get(i)) {
//					byte[] blockDataToFlush = new byte[512];
//					NBDVolumeServer.storage.read(blockDataToFlush, i * blockSize);
//					NBDVolumeServer.storage
//				}
//			}
//			
//			// uploads dirty bitmaps to the cloud storage service
//			
//			// unset bits in the dirty blocks for flushed dirty blocks
//
//		} else {
//			System.err.println("ASSERT: Not an Owner, nothing to flush periodically");
//			System.exit(1);
//		}
//	}


	/*
	 * Periodic Prefetching
	 */
	
}

