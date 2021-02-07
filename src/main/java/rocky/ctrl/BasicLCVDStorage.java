package rocky.ctrl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import rocky.ctrl.RockyController.RockyControllerRoleType;
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

	Thread cloudPackageManagerThread;
	//private final AtomicBoolean running = new AtomicBoolean(false);
	private final BlockingQueue<WriteRequest> queue;
	public HashMap<Integer, byte[]> writeMap;

	Thread roleSwitcherThread;
	public Object prefetchFlush;
	
	public static boolean roleSwitchFlag;
	
	Thread prefetcherThread;
	
	ControlUserInterfaceRunner cui;
	Thread controlUIThread;
	
	public static long epochCnt;
	public static long prefetchedEpoch;
	
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
		prefetchedEpoch = getPrefetchedEpoch();
		//System.out.println(">>>> epochCn=" + epochCnt);
		writeMap = new HashMap<Integer, byte[]>();
		CloudPackageManager cpm = new CloudPackageManager(queue);
		cloudPackageManagerThread = new Thread(cpm);
		Prefetcher prefetcher = new Prefetcher(this);
		prefetcherThread = new Thread(prefetcher);
		RockyController.role = RockyControllerRoleType.None;
		roleSwitchFlag = false;
		roleSwitcherThread = new Thread(new RoleSwitcher());
		roleSwitcherThread.start();
		cui = new ControlUserInterfaceRunner(roleSwitcherThread);
		controlUIThread = new Thread(cui);
		controlUIThread.start();
	}

	public long getPrefetchedEpoch() {
		long retLong = 0;
		byte[] epochBytes;
		try {
			epochBytes = blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
			if (epochBytes == null) {
				System.out.println("PrefetchedEpoch-" + RockyController.nodeID 
						+ " key is not allocated yet. Allocate it now");
				try {
					blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID
							, ByteUtils.longToBytes(retLong));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else {
				//System.out.println("epochBytes=" + epochBytes.hashCode());
				//System.out.println("Long.MAX=" + Long.MAX_VALUE);
				//System.out.println("epochBytes length=" + epochBytes.length);
				retLong = ByteUtils.bytesToLong(epochBytes);
				System.out.println("getPrefetchedEpoch returns=" + retLong);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return retLong;
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
				//System.out.println("epochBytes=" + epochBytes.hashCode());
				//System.out.println("Long.MAX=" + Long.MAX_VALUE);
				//System.out.println("epochBytes length=" + epochBytes.length);
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
		//roleSwitcherThread.start();
	}

	@Override
	public void disconnect() {
//		if (opened) {
//			opened = false;		
//		} else {
//		      throw new IllegalStateException("Not connected to " + exportName);
//	    }
		super.disconnect();
		//switchRole(RockyController.role, RockyController.RockyControllerRoleType.None);
		//roleSwitcherThread.interrupt();
		//try {
		//	roleSwitcherThread.join();
		//} catch (InterruptedException e) {
		//	// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
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
		
//		synchronized(roleSwitcherThread) {
//			if (!(RockyController.role.equals(RockyControllerRoleType.Owner)
//					|| RockyController.role.equals(RockyControllerRoleType.NonOwner))) {
//				System.err.println("ASSERT: read cannot be served by None role");
//				System.err.println("currently, my role=" + RockyController.role);
//				return null;
//			}
//		}
		
		long firstBlock = offset / blockSize;
	    int length = buffer.length;
	    long lastBlock = 0;
	    if (length % blockSize == 0) {
	    	lastBlock = (offset + length) / blockSize - 1;
	    } else {
	    	lastBlock = (offset + length) / blockSize;
	    }
	    //long lastBlock = (offset + length) / blockSize;
	    
//	    // we assume that buffer size to be blockSize when it is used
//	    // as a block device properly. O.W. it can be smaller than that.
//	    // To make it compatible with the original intention, when 
//	    // we get buffer smaller than blockSize, we increment the loastBlock
//	    // by one.
//	    if (firstBlock == lastBlock) {
//	    	lastBlock++; 
//	    }
//	    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
	    for (int i = (int) firstBlock; i <= (int) lastBlock; i++) {
	    	byte[] blockData = new byte[blockSize];
	    	boolean isPresent = false;
	    	synchronized(presenceBitmap) {
	    		isPresent = presenceBitmap.get(i);
	    	}
	    	if (isPresent) {
				super.read(blockData, i * blockSize);
				//System.out.println("i=" + i);
				//System.out.println("firstBlock=" + firstBlock);
				//System.out.println("blockData length=" + blockData.length);
				//System.out.println("buffer length=" + buffer.length);
				System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
			} else {
				// read from the cloud backend (slow path)
				try {
					blockData = blockDataStore.get(String.valueOf(i));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
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
		
		synchronized(roleSwitcherThread) {
			if (!RockyController.role.equals(RockyControllerRoleType.Owner)) {
				System.err.println("ASSERT: write can be served only by the Owner");
				System.err.println("currently, my role=" + RockyController.role);
				return null;
			}
		}
		
		System.out.println("write entered. buffer size=" + buffer.length);
		System.out.println("offset=" + offset);
		
		long firstBlock = offset / blockSize;
		int length = buffer.length;
	    long lastBlock = 0;
	    if (length % blockSize == 0) {
	    	lastBlock = (offset + length) / blockSize - 1;
	    } else {
	    	lastBlock = (offset + length) / blockSize;
	    }
	    //long lastBlock = (offset + length) / blockSize;
	    
	    System.out.println("firstBlock=" + firstBlock + " lastBlock=" + lastBlock + " length=" + length);
	    
//	    // we assume that buffer size to be blockSize when it is used
//	    // as a block device properly. O.W. it can be smaller than that.
//	    // To make it compatible with the original intention, when 
//	    // we get buffer smaller than blockSize, we increment the loastBlock
//	    // by one.
//	    if (firstBlock == lastBlock) {
//	    	lastBlock++; 
//	    }
//	    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
    	for (int i = (int) firstBlock; i <= (int) lastBlock; i++) {
	    	System.out.println("setting dirtyBitmap for blockID=" + i);
	    	synchronized(dirtyBitmap) {
	    		dirtyBitmap.set(i);
	    	}
	    	WriteRequest wr = new WriteRequest();
	    	int copySize = 0;
	    	if (i == lastBlock) {
	    		System.out.println("copySize first");
	    		int residual = buffer.length % blockSize;
	    		if (residual == 0) {
	    			copySize = blockSize;
	    		} else {
	    			copySize = residual;
	    		}
	    	} else {
	    		System.out.println("copySize second");
	    		copySize = blockSize;
	    	}
	    	byte[] copyBuf = new byte[copySize];
	    	System.out.println("copySize=" + copySize);
	    	int bufferStartOffset = (int) ((i - firstBlock) * blockSize);
	    	System.out.println("bufferStartOffset=" + bufferStartOffset + " i=" + i + " firstBlock=" + firstBlock + " blockSize=" + blockSize);
	    	System.arraycopy(buffer, (int) ((i - firstBlock) * blockSize), copyBuf, 0, copySize);
	    	wr.buf = copyBuf;
		    wr.offset = offset;
		    try {
		    	//System.out.println("[BasicLCVDStorage] enqueuing WriteRequest for blockID=" 
		    	//		+ ((int) wr.offset / blockSize));
				queue.put(wr);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
		return super.write(buffer, offset);
	}

	@Override
	public CompletableFuture<Void> flush() {
//		// TODO Auto-generated method stub
//		return null;

		synchronized(roleSwitcherThread) {
			if (!RockyController.role.equals(RockyControllerRoleType.Owner)) {
				System.err.println("ASSERT: flush can be served only by the Owner");
				System.err.println("currently, my role=" + RockyController.role);
				return null;
			}
		}
		
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

		@Override
		public void run() {
			System.out.println("[RoleSwitcher] entered run");
			try {
				while (true) {
					RockyController.RockyControllerRoleType prevRole = null;
					RockyController.RockyControllerRoleType newRole = null;
					//synchronized(RockyController.role) {
					synchronized(roleSwitcherThread) {
						prevRole = RockyController.role;
						if (prevRole == null) {
							System.err.println("ASSERT: RockyController.role should not be initialized to null");
							System.exit(1);
						}
						while (RockyController.role.equals(prevRole)) {
							//RockyController.role.wait();
							roleSwitcherThread.wait();
						}
						newRole = RockyController.role;
					}
					if (newRole == null) {
						System.err.println("ASSERT: new RockyController.role should not be null");
						System.exit(1);
					}
					System.out.println("Role switching from " 
							+ prevRole + " to " + newRole);
					switchRole(prevRole, newRole);
				}
			} catch (InterruptedException e) {
				System.out.println("[RoleSwitcher] Get interrupted");
			}
		}
	}
	
	
	
	/**
	 *  State change and activated threads:
	 *  
	 *     None    -->    NonOwner    <-->        Owner
	 *  ---------------------------------------------------------
	 *  RoleSwitcher	RoleSwitcher	       RoleSwitcher
	 *  				 Prefetcher             Prefetcher
	 *                                       CloudPackageManager
	 */
	public void switchRole(RockyController.RockyControllerRoleType prevRole, 
			RockyController.RockyControllerRoleType newRole) {
		if (prevRole.equals(RockyController.RockyControllerRoleType.None) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner)) {
			prefetcherThread.start();
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner)) {
			cloudPackageManagerThread.start();
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.Owner) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner)) {
			stopCloudPackageManager();
			instantCloudFlushing();
		} else if (prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.None)) {
			stopPrefetcher();
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
	 * Periodic Prefetching: Enable for both Owner and NonOwner
	 */
	
	public class Prefetcher implements Runnable {
		BasicLCVDStorage myStorage = null;
		public Prefetcher(BasicLCVDStorage storage) {
			myStorage = storage;
		}
		
		@Override
		public void run() {
			try {
				System.out.println("Running Prefetcher");
				// Get status parameters
				long latestEpoch = -1;
				//long myPrefetchedEpoch = 0;
				while (true) {
					try {
						byte[] latestEpochBytes = blockDataStore.get("EpochCount");
						if (latestEpochBytes == null) {
							System.out.println("Prefetcher thread gets interrupted, exit the main loop here");
							break;
						}
						latestEpoch = ByteUtils.bytesToLong(latestEpochBytes);
						System.out.println("latestEpoch=" + latestEpoch);
						//byte[] myPrefetchedEpochBytes = blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
						//if (myPrefetchedEpochBytes == null) {
						//	blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(myPrefetchedEpoch));
						//}
						//myPrefetchedEpoch = ByteUtils.bytesToLong(myPrefetchedEpochBytes);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("prefetchedEpoch=" + BasicLCVDStorage.prefetchedEpoch);
					System.out.println("epochCnt=" + epochCnt);
					if (latestEpoch > epochCnt) { // if I am Owner, I don't need to prefetch as I am the most up-to-date node
						if (latestEpoch > BasicLCVDStorage.prefetchedEpoch) { // if I am nonOwner with nothing more to prefetch, I don't prefetch
							// Get all epoch bitmaps
							//List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, myPrefetchedEpoch);
							List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, BasicLCVDStorage.prefetchedEpoch);
							
							// Get a list of blockIDs to prefetch
							HashSet<Integer> blockIDList = getPrefetchBlockIDList(epochBitmapList);
						
							if (blockIDList != null) { // if blockIDList is null, we don't need to prefetch anything
								// Prefetch loop
								prefetchBlocks(myStorage, blockIDList);
								
								// Update PrefetchedEpoch-<nodeID> on cloud and prefetchedEpoch
								try {
									blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(latestEpoch));
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								prefetchedEpoch = latestEpoch;
							}
						}
					}
					Thread.sleep(RockyController.prefetchPeriod);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("[Prefetcher] Get interrupted");
			}
			System.out.println("Terminating Prefetcher");
		}
	}


	public void prefetchBlocks(BasicLCVDStorage myStorage, HashSet<Integer> blockIDList) {
		Iterator<Integer> iter = blockIDList.iterator();
		int blockID = -1;
		byte[] blockData = null;
		while (iter.hasNext()) {
			blockID = iter.next();
			try {
				blockData = blockDataStore.get(String.valueOf(blockID));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			myStorage.prefetchWrite(blockData, blockID * blockSize);
			myStorage.prefetchFlush();
			synchronized(presenceBitmap) {
				presenceBitmap.set(blockID);
			}
		}
	} 
	
	public HashSet<Integer> getPrefetchBlockIDList(List<BitSet> epochBitmapList) {
		if (epochBitmapList == null) {
			return null;
		}
		
		HashSet<Integer> retSet = null;
		for (BitSet bs : epochBitmapList) {
			for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
				// operate on index i here
				if (i == Integer.MAX_VALUE) {
					break; // or (i+1) would overflow
				}
				if (retSet == null) {
					retSet = new HashSet<Integer>();
				}
				retSet.add(i);
			}
		}
		return retSet;
	}
	
	public void prefetchFlush() {
		super.flush();		
	}

	public void prefetchWrite(byte[] blockData, long i) {
		super.write(blockData, i);		
	}
	
	public byte[] localRead(byte[] buffer, long offset) {
		CompletableFuture<Void> readFuture = super.read(buffer, offset);
		readFuture.join();
		return buffer;
	}
	
	public void localRemove() {
		
	}

	public List<BitSet> fetchNextEpochBitmaps(long latestEpoch, long myPrefetchedEpoch) {
		List<BitSet> retList = null;
		byte[] epochBitmap = null;
		for (int i = (int) (myPrefetchedEpoch + 1); i <= latestEpoch; i++) {
			try {
				epochBitmap = dBmStore.get(i + "-bitmap");
				if (epochBitmap == null) {
					System.err.println("ASSERT: failed to fetch " + i + "-bitmap");
					System.exit(1);
				} else {
					if (retList == null) {
						retList = new ArrayList<BitSet>();
					}
					retList.add(BitSet.valueOf(epochBitmap));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retList;
	}
	
	public void stopPrefetcher() {
		System.out.println("interrupting the prefetcher thread to terminate");
		prefetcherThread.interrupt();
		try {
			prefetcherThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * Periodic Flushing: Only enable for Owner
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
		@Override
		public void run() {
			System.out.println("[CloudPackageManager] run entered");
			try {
				Timer timer = new Timer();
				timer.schedule(new CloudFlusher(), RockyController.epochPeriod);
				while (true) { 
					WriteRequest wr = q.take();
			    	System.out.println("[CloudPackageManager] dequeued WriteRequest for blockID=" 
			    			+ ((int) wr.offset / blockSize));
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
		@Override
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
			long curEpoch = epochCnt + 1;
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
			Timer timer = new Timer();
			timer.schedule(new CloudFlusher(), RockyController.epochPeriod);
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

