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
import rocky.ctrl.RockyStorage.CloudFlusher;
import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.cloud.ValueStorageDynamoDB;
import rocky.ctrl.utils.ByteUtils;
import rocky.ctrl.utils.ObjectSerializer;

public class RockyStorage extends FDBStorage {
		String nodeID;
		public static final long MAX_SIZE = 51200; // HARD-CODED  512 bytes * 100
		public static final int blockSize = 512;
		
		public static boolean debugPrintoutFlag = false;
		
		public static boolean mutationSnapEvalFlag = true;
		public static int numBlockWrites = 0;
		public static int numMutationSnapshotBlocks = 0;
		
		public static boolean snapMergeEvalFlag = true;
		public static int numPastEpochsPrefetched = 0;
		public static int numBlockWrittenForPastEpochs = 0;
		public static int numBlocksMergedSnapshot = 0;
		
		public static int numBlock;
		
		public static BitSet presenceBitmap;
		public static BitSet dirtyBitmap;

		//public String pBmTableName = "presenceBitmapTable";
		public static String cloudEpochBitmapsTableName = "cloudEpochBitmapsTable";
		public static String localEpochBitmapsTableName = "localEpochBitmapsTable";
		public static String cloudBlockSnapshotStoreTableName = "cloudBlockSnapshotStoreTable";
		public static String versionMapTableName = "versionMapTable";
		public static String localBlockSnapshotStoreTableName = "localBlockSnapshotStoreTable";
		
		//public GenericKeyValueStore pBmStore;
		public static GenericKeyValueStore cloudEpochBitmaps;
		public static GenericKeyValueStore localEpochBitmaps;
		public static GenericKeyValueStore cloudBlockSnapshotStore;
		public static GenericKeyValueStore versionMap;
		public static GenericKeyValueStore localBlockSnapshotStore;
		
		public static Thread cloudPackageManagerThread;
		public static Timer flusherTimer;
		public static TimerTask nextFlusherTask;
		//private final AtomicBoolean running = new AtomicBoolean(false);
		protected final BlockingQueue<WriteRequest> queue;
		public HashMap<Integer, byte[]> writeMap;

		Thread roleSwitcherThread;
		public Object prefetchFlush;
		
		public static boolean roleSwitchFlag;
		
		public static Thread prefetcherThread;
		
		ControlUserInterfaceRunner cui;
		public static Thread controlUIThread;
		
		public static long epochCnt;
		public static long prefetchedEpoch;
		
		class WriteRequest {
			public byte[] buf;
			public long offset;
		}
		
//		private final String lcvdFilePath;
//		private final LongAdder writesStarted;
//		private final LongAdder writesComplete;
//		private final long size;
//		private final String exportName;
		
		// ToDo: variables for temporary usage; replace later with better ones
		protected static boolean opened = false;
		
		//// HARD-CODED for now
		//String dirName = "/home/ben/experiment/rocky/basic/localCopyVirtualDisks"; 
		//String lcvdFileName = "lcvd";
		//long diskSize = 64; //64 bytes
		
		public RockyStorage(String exportName) {
			super(exportName);
			System.out.println("RockyStorage constructor entered");
			if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDBLocal)) {
				//pBmStore = new ValueStorageDynamoDB(pBmTableName, true);
				//cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, true);
				//cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, true);
				cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
				cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
			} else if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDB_SEOUL)) {
				//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
				//cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, false);
				//cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, false);
				cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
				cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
			} else if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDB_LONDON)) {
				//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
				//cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, false);
				//cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, false);
				cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
				cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
			} else if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDB_OHIO)) {
				//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
				//cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, false);
				//cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, false);
				cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.OHIO);
				cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.OHIO);
			} else {
				System.err.println("Error: Unknown backendStorageType");
	 		   	System.exit(1);
			}
			try {
				localEpochBitmaps = new ValueStorageLevelDB(localEpochBitmapsTableName);
				localBlockSnapshotStore = new ValueStorageLevelDB(localBlockSnapshotStoreTableName);
				versionMap = new ValueStorageLevelDB(versionMapTableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			numBlock = (int) (size() / 512); //blocksize=512bytes
			presenceBitmap = new BitSet(numBlock);
			dirtyBitmap = new BitSet(numBlock);
			presenceBitmap.set(0, numBlock);
			dirtyBitmap.clear();
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
			cui.rockyStorage = this;
			controlUIThread = new Thread(cui);
			controlUIThread.start();
		}
		
//		public long getRealBlockID(long epoch, long bid) {
//			long rbid = 0;
//			long highBits = epoch % 256;
//			rbid = bid & 0x00ffffff; 
//			rbid = rbid & (highBits << 56);
//			return rbid;
//		}
		
		public long getPrefetchedEpoch() {
			long retLong = 0;
			byte[] epochBytes;
			try {
				epochBytes = cloudBlockSnapshotStore.get("PrefetchedEpoch-" + RockyController.nodeID);
				if (epochBytes == null) {
					System.out.println("PrefetchedEpoch-" + RockyController.nodeID 
							+ " key is not allocated yet. Allocate it now");
					try {
						cloudBlockSnapshotStore.put("PrefetchedEpoch-" + RockyController.nodeID
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
				epochBytes = cloudBlockSnapshotStore.get("EpochCount");
				if (epochBytes == null) {
					System.out.println("EpochCount key is not allocated yet. Allocate it now");
					try {
						cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(retLong));
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				} else {
					//System.out.println("epochBytes=" + epochBytes.hashCode());
					//System.out.println("Long.MAX=" + Long.MAX_VALUE);
					//System.out.println("epochBytes length=" + epochBytes.length);
					retLong = ByteUtils.bytesToLong(epochBytes);
					System.out.println("currently epoch is " + epochCnt);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return retLong;
		}
		
		@Override
		public void connect() {
			// TODO Auto-generated method stub
//			if (!opened) {
//				opened = true;
//			} else {
//				throw new IllegalStateException("Volume " + exportName + " is already leased");
//			}
			super.connect();
			//roleSwitcherThread.start();
		}

		@Override
		public void disconnect() {
//			if (opened) {
//				opened = false;		
//			} else {
//			      throw new IllegalStateException("Not connected to " + exportName);
//		    }
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
//			File lcvdFile = new File(lcvdFilePath);
//			try {
//				RandomAccessFile raf = new RandomAccessFile( lcvdFile, "r" );
//				raf.seek(offset);
//				raf.readFully(buffer);
//				raf.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//			return null;
			
//			synchronized(roleSwitcherThread) {
//				if (!(RockyController.role.equals(RockyControllerRoleType.Owner)
//						|| RockyController.role.equals(RockyControllerRoleType.NonOwner))) {
//					System.err.println("ASSERT: read cannot be served by None role");
//					System.err.println("currently, my role=" + RockyController.role);
//					return null;
//				}
//			}
			
			if (debugPrintoutFlag) {
				System.out.println("read entered. buffer size=" + buffer.length);
				System.out.println("offset=" + offset);
			}
			
			long firstBlock = offset / blockSize;
		    int length = buffer.length;
		    long lastBlock = 0;
		    if (length % blockSize == 0) {
		    	if ((offset + length) / blockSize == 0) {
		    		lastBlock = firstBlock;
 		    	} else {
 		    		lastBlock = (offset + length) / blockSize - 1;
 		    	}
		    } else {
		    	lastBlock = (offset + length) / blockSize;
		    }
		    //long lastBlock = (offset + length) / blockSize;
		    
		    if (debugPrintoutFlag) {
		    	System.out.println("firstBlock=" + firstBlock + " lastBlock=" + lastBlock + " length=" + length);
		    }
		    	
//		    // we assume that buffer size to be blockSize when it is used
//		    // as a block device properly. O.W. it can be smaller than that.
//		    // To make it compatible with the original intention, when 
//		    // we get buffer smaller than blockSize, we increment the loastBlock
//		    // by one.
//		    if (firstBlock == lastBlock) {
//		    	lastBlock++; 
//		    }
//		    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
		    for (int i = (int) firstBlock; i <= (int) lastBlock; i++) {
		    	byte[] blockData = new byte[blockSize];
		    	boolean isPresent = false;
		    	synchronized(presenceBitmap) {
		    		isPresent = presenceBitmap.get(i);
		    	}
		    	if (isPresent) {
		    		if (debugPrintoutFlag) {
		    			System.out.println("blockID=" + i + " is locally present");
		    		}
//		    		super.read(blockData, i * blockSize);
					//System.out.println("i=" + i);
					//System.out.println("firstBlock=" + firstBlock);
					//System.out.println("blockData length=" + blockData.length);
					//System.out.println("buffer length=" + buffer.length);
//					System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
				} else {
					if (debugPrintoutFlag) {
						System.out.println("blockID=" + i + " is NOT locally present");
					}
					// read from the cloud backend (slow path)
					byte[] epochToReqBytes = null;
					String realBlockID = "";
					try {
						epochToReqBytes =  versionMap.get(String.valueOf(i));
						if (epochToReqBytes == null) {
							epochToReqBytes = new byte[8];
						}
						long epochToReq = ByteUtils.bytesToLong(epochToReqBytes);
						realBlockID = epochToReq + ":" + i;
						//blockData = blockDataStore.get(String.valueOf(i));
						if (debugPrintoutFlag) {
							System.out.println("fetching with real blockID=" + realBlockID);
						}
						if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDBLocal)) {
							try {
								Thread.sleep(50);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						blockData = cloudBlockSnapshotStore.get(realBlockID);
						if (blockData == null) {
							blockData = new byte[blockSize];
						}
						localBlockSnapshotStore.put(realBlockID, blockData);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//					System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
					prefetchWrite(blockData, i * blockSize);
					prefetchFlush();
					//super.write(blockData, i * blockSize);
					//super.flush();
					synchronized(presenceBitmap) {
						presenceBitmap.set(i);
					}
				}
		    }
			return super.read(buffer, offset);
		}

		@Override
		public CompletableFuture<Void> write(byte[] buffer, long offset) {
//			// TODO Auto-generated method stub
//			return null;
			
			synchronized(roleSwitcherThread) {
				if (!RockyController.role.equals(RockyControllerRoleType.Owner)) {
					System.err.println("ASSERT: write can be served only by the Owner");
					System.err.println("currently, my role=" + RockyController.role);
					return null;
				}
			}
			
			if (debugPrintoutFlag) {
				System.out.println("write entered. buffer size=" + buffer.length);
				System.out.println("offset=" + offset);
			}
			
			long firstBlock = offset / blockSize;
			int length = buffer.length;
		    long lastBlock = 0;
		    if (length % blockSize == 0) {
			if ((offset + length) / blockSize == 0) {
			    lastBlock = firstBlock;
 		    	} else {
			    lastBlock = (offset + length) / blockSize - 1;
 		    	}
		    } else {
		    	lastBlock = (offset + length) / blockSize;
		    }
		    //long lastBlock = (offset + length) / blockSize;
		    
		    if (debugPrintoutFlag) {
		    	System.out.println("firstBlock=" + firstBlock + " lastBlock=" + lastBlock + " length=" + length);
		    }
		    	
		    if (mutationSnapEvalFlag) {
		    	numBlockWrites += (int) (lastBlock - firstBlock + 1);
		    }
		    
//		    // we assume that buffer size to be blockSize when it is used
//		    // as a block device properly. O.W. it can be smaller than that.
//		    // To make it compatible with the original intention, when 
//		    // we get buffer smaller than blockSize, we increment the loastBlock
//		    // by one.
//		    if (firstBlock == lastBlock) {
//		    	lastBlock++; 
//		    }
//		    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
	    	for (int i = (int) firstBlock; i <= (int) lastBlock; i++) {
	    		if (debugPrintoutFlag) {
	    			System.out.println("setting dirtyBitmap for blockID=" + i);
	    		}
	    		synchronized(dirtyBitmap) {
		    		dirtyBitmap.set(i);
		    	}
		    	WriteRequest wr = new WriteRequest();
		    	int copySize = 0;
		    	if (i == lastBlock) {
		    		if (debugPrintoutFlag) {
		    			System.out.println("copySize first");
		    		}
		    		int residual = buffer.length % blockSize;
		    		if (residual == 0) {
		    			copySize = blockSize;
		    		} else {
		    			copySize = residual;
		    		}
		    	} else {
		    		if (debugPrintoutFlag) {
		    			System.out.println("copySize second");
		    		}
		    		copySize = blockSize;
		    	}
		    	byte[] copyBuf = new byte[copySize];
		    	if (debugPrintoutFlag) {
		    		System.out.println("copySize=" + copySize);
		    	}
		    	int bufferStartOffset = (int) ((i - firstBlock) * blockSize);
		    	if (debugPrintoutFlag) {
		    		System.out.println("bufferStartOffset=" + bufferStartOffset + " i=" + i + " firstBlock=" + firstBlock + " blockSize=" + blockSize);
		    	}
		    	System.arraycopy(buffer, (int) ((i - firstBlock) * blockSize), copyBuf, 0, copySize);
		    	wr.buf = copyBuf;
		    	//wr.offset = offset;
			    wr.offset = i * blockSize;
			    try {
			    	//System.out.println("[RockyStorage] enqueuing WriteRequest for blockID=" 
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
//			// TODO Auto-generated method stub
//			return null;

			// It seems nbd-client can send flush request spontaneously.
			// There is no way we can enforce the nbd-client not to send the flush
			// when this rocky node's role is non-owner.
			// We guard the data on non-owern to be read-only by disallowing
			// write request when this rocky node's role is non-owner.
			// Then, the flush on non-owner actually flush nothing (there is no write to flush)
			/* synchronized(roleSwitcherThread) {
				if (!RockyController.role.equals(RockyControllerRoleType.Owner)) {
					System.err.println("ASSERT: flush can be served only by the Owner");
					System.err.println("currently, my role=" + RockyController.role);
					return null;
				}
			}*/
			
			return super.flush();
		}

		@Override
		public long size() {
//			// TODO Auto-generated method stub
//			return 0;
			
			return super.size();
		}

		@Override
		public long usage() {
//			// TODO Auto-generated method stub
//			return 0;
			
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
		public void prefetch () throws IOException {
			long latestEpoch = -1;
				byte[] latestEpochBytes = cloudBlockSnapshotStore.get("EpochCount");
				if (latestEpochBytes == null) {
					if (debugPrintoutFlag) {
						System.out.println("Prefetcher thread gets interrupted, exit the main loop here");
					}
					return;
				}
				latestEpoch = ByteUtils.bytesToLong(latestEpochBytes);
				if (debugPrintoutFlag) {
					System.out.println("latestEpoch=" + latestEpoch);
				}
				//byte[] myPrefetchedEpochBytes = blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
				//if (myPrefetchedEpochBytes == null) {
				//	blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(myPrefetchedEpoch));
				//}
				//myPrefetchedEpoch = ByteUtils.bytesToLong(myPrefetchedEpochBytes);
			if (debugPrintoutFlag) { 
				System.out.println("prefetchedEpoch=" + RockyStorage.prefetchedEpoch);
				System.out.println("epochCnt=" + epochCnt);
			}
			if (latestEpoch > RockyStorage.prefetchedEpoch) { // if I am nonOwner with nothing more to prefetch, I don't prefetch
				// Get all epoch bitmaps
				//List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, myPrefetchedEpoch);
				List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, RockyStorage.prefetchedEpoch);
				
				// Get a list of blockIDs to prefetch
				HashSet<Integer> blockIDList = getPrefetchBlockIDList(epochBitmapList);
			
				if (blockIDList != null) { // if blockIDList is null, we don't need to prefetch anything
					// Prefetch loop
					prefetchBlocks(this, blockIDList);
					
					// Update PrefetchedEpoch-<nodeID> on cloud and prefetchedEpoch
					try {
						cloudBlockSnapshotStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(latestEpoch));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					prefetchedEpoch = latestEpoch;
				}
			}	
		}
		
		
		public class Prefetcher implements Runnable {
			RockyStorage myStorage = null;
			public Prefetcher(RockyStorage storage) {
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
							byte[] latestEpochBytes = cloudBlockSnapshotStore.get("EpochCount");
							if (latestEpochBytes == null) {
								if (debugPrintoutFlag) {
									System.out.println("Prefetcher thread gets interrupted, exit the main loop here");
								}
								break;
							}
							latestEpoch = ByteUtils.bytesToLong(latestEpochBytes);
							if (debugPrintoutFlag) {
								System.out.println("latestEpoch=" + latestEpoch);
							}
							//byte[] myPrefetchedEpochBytes = blockDataStore.get("PrefetchedEpoch-" + RockyController.nodeID);
							//if (myPrefetchedEpochBytes == null) {
							//	blockDataStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(myPrefetchedEpoch));
							//}
							//myPrefetchedEpoch = ByteUtils.bytesToLong(myPrefetchedEpochBytes);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if (debugPrintoutFlag) { 
							System.out.println("prefetchedEpoch=" + RockyStorage.prefetchedEpoch);
							System.out.println("epochCnt=" + epochCnt);
						}
						if (latestEpoch > epochCnt) { // if I am Owner, I don't need to prefetch as I am the most up-to-date node
							if (latestEpoch > RockyStorage.prefetchedEpoch) { // if I am nonOwner with nothing more to prefetch, I don't prefetch
								// Get all epoch bitmaps
								//List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, myPrefetchedEpoch);
								List<BitSet> epochBitmapList = fetchNextEpochBitmaps(latestEpoch, RockyStorage.prefetchedEpoch);
								
								// Get a list of blockIDs to prefetch
								HashSet<Integer> blockIDList = getPrefetchBlockIDList(epochBitmapList);
							
								if (blockIDList != null) { // if blockIDList is null, we don't need to prefetch anything
									// Prefetch loop
									prefetchBlocks(myStorage, blockIDList);
									
									// Update PrefetchedEpoch-<nodeID> on cloud and prefetchedEpoch
									try {
										cloudBlockSnapshotStore.put("PrefetchedEpoch-" + RockyController.nodeID, ByteUtils.longToBytes(latestEpoch));
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


		public void prefetchBlocks(RockyStorage myStorage, HashSet<Integer> blockIDList) {
			Iterator<Integer> iter = blockIDList.iterator();
			int blockID = -1;
			byte[] blockData = null;
			while (iter.hasNext()) {
				blockID = iter.next();
				try {
					byte[] epochBytes = versionMap.get(String.valueOf(blockID));
					long epochToRead = ByteUtils.bytesToLong(epochBytes);
					String realBlockID = epochToRead + ":" + blockID;
					if (debugPrintoutFlag) {
						System.out.println("readBlockID to prefetch=" + realBlockID);
					}
					blockData = cloudBlockSnapshotStore.get(realBlockID);
					if (blockData == null) {
						blockData = new byte[blockSize];
					}
					localBlockSnapshotStore.put(realBlockID, blockData);
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
			CompletableFuture<Void> flushFuture = super.flush();
			if (debugPrintoutFlag) {
				System.out.println("prefetchFlush joining");
			}
			flushFuture.join();
			if (debugPrintoutFlag) {
				System.out.println("prefetchFlush joined");
			}
		}

		public void prefetchWrite(byte[] blockData, long i) {
			CompletableFuture<Void> writeFuture = super.write(blockData, i);
			if (debugPrintoutFlag) {
				System.out.println("prefetchWrite joining");
			}
			writeFuture.join();
			if (debugPrintoutFlag) {
				System.out.println("prefetchWrite joined");
			}
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
			for (long i = myPrefetchedEpoch + 1; i <= latestEpoch; i++) {
				if (debugPrintoutFlag) {
					System.out.println("myPrefetchedEpoch=" + myPrefetchedEpoch 
						+ " i=" + i + " latestEpoch=" + latestEpoch);
				}
				try {
					epochBitmap = cloudEpochBitmaps.get(i + "-bitmap");
					if (epochBitmap == null) {
						System.err.println("ASSERT: failed to fetch " + i + "-bitmap");
						System.exit(1);
					} else {
						if (debugPrintoutFlag) {
							System.out.println("epochBitmap is received for epoch=" + i);
						}
						if (retList == null) {
							retList = new ArrayList<BitSet>();
						}
						BitSet epochBitmapBitSet = BitSet.valueOf(epochBitmap);
						retList.add(epochBitmapBitSet);
						localEpochBitmaps.put(i + "-bitmap", epochBitmap);
						byte[] thisEpochBytes = ByteUtils.longToBytes(i);
						if (debugPrintoutFlag) {
							System.out.println("about to enter the loop updating versionMap");
						}
						for (int j = epochBitmapBitSet.nextSetBit(0); j >= 0; j = epochBitmapBitSet.nextSetBit(j+1)) {
							// operate on index i here
						    if (i == Integer.MAX_VALUE) {
						    	break; // or (i+1) would overflow
						    }
						    versionMap.put(j + "", thisEpochBytes);
						}
						//while((j = epochBitmapBitSet.nextSetBit(j)) >= 0) {
						//	versionMap.put(j + "", thisEpochBytes);
						//}
						if (debugPrintoutFlag) {
							System.out.println("finished with updating versionMap for epoch=" + i);
						}
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
			protected final BlockingQueue<WriteRequest> q;
			public CloudPackageManager(BlockingQueue<WriteRequest> q) { 
				this.q = q; 
			}
			@Override
			public void run() {
				System.out.println("[CloudPackageManager] run entered");
				try {
					flusherTimer = new Timer();
					nextFlusherTask = new CloudFlusher();
					flusherTimer.schedule(nextFlusherTask, RockyController.epochPeriod);
					while (true) { 
						WriteRequest wr = q.take();
						if (debugPrintoutFlag) {
							System.out.println("[CloudPackageManager] dequeued WriteRequest for blockID=" 
								+ ((int) wr.offset / blockSize));
						}
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
						writeMap.clear();
						dirtyBitmapClone = (BitSet) dirtyBitmap.clone();
						dirtyBitmap.clear();
					}				
				}
				//System.err.println("[CloudFlusher] writeMap lock released");
				long curEpoch = epochCnt + 1;
				for (Integer i : writeMapClone.keySet()) {
					byte[] buf = writeMapClone.get(i);
					try {
						String blockSnapshotID = curEpoch + ":" + i;
						if (debugPrintoutFlag) {
							System.out.println("For blockID=" + i + " buf is written to the cloud");
							System.out.println("blockSnapshotID=" + blockSnapshotID);
						}
						localBlockSnapshotStore.put(blockSnapshotID, buf);
						versionMap.put(String.valueOf(i), ByteUtils.longToBytes(curEpoch));
						cloudBlockSnapshotStore.put(blockSnapshotID, buf);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				byte[] dmBytes = dirtyBitmapClone.toByteArray();
				try {
					if (debugPrintoutFlag) {
						System.out.println("dBmStore put for epoch=" + curEpoch);
					}
					cloudEpochBitmaps.put(Long.toString(curEpoch) + "-bitmap", dmBytes);
					cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(curEpoch));
					epochCnt++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				flusherTimer = new Timer();
				nextFlusherTask = new CloudFlusher();
				flusherTimer.schedule(nextFlusherTask, RockyController.epochPeriod);
			}
		}

		
//		class EpochFlusher {
	//
//			ScheduledExecutorService executorService;
//			
//			public EpochFlusher() {
//				executorService = Executors.newSingleThreadScheduledExecutor();
//			}
//			
//			public void startPeriodicFlushing(int epochPeriod) {
//				executorService.scheduleAtFixedRate(RockyStorage::periodicFlushToCloud, 0, epochPeriod, TimeUnit.SECONDS);
//			}
//			
//			public void stopPeriodicFlushing() {
//				executorService.shutdown();
//				try {
//					executorService.awaitTermination(60, TimeUnit.SECONDS);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//					System.err.println("awaitTermination is interrupted");
//					System.exit(1);
//				}
//			}
//			public void periodicFlushToCloud() {
//				RockyControllerRoleType myRole = RockyControllerRoleType.None;
//				synchronized(RockyController.role) {
//					myRole = RockyController.role;
//				}
//				BitSet dirtyBitmapClone;
//				synchronized(dirtyBitmap) {
//					dirtyBitmapClone = (BitSet) dirtyBitmap.clone();
//				}
//				if (myRole.equals(RockyControllerRoleType.Owner)) {
//					// writes dirty blocks to the cloud storage service
//					for (int i = 0; i < dirtyBitmapClone.length(); i++) {
//						if (dirtyBitmapClone.get(i)) {
//							byte[] blockDataToFlush = new byte[512];
//							NBDVolumeServer.storage.read(blockDataToFlush, i * blockSize);
//						}
//					}
//					
//					// uploads dirty bitmaps to the cloud storage service
//					
//					// unset bits in the dirty blocks for flushed dirty blocks
	//
//				} else {
//					System.err.println("ASSERT: Not an Owner, nothing to flush periodically");
//					System.exit(1);
//				}
//			}
//		}
		
//		public static void periodicFlushToCloud() {
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
//						NBDVolumeServer.storage
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


		/*
		 * Periodic Prefetching
		 */
		
	


}
