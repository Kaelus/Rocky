package rocky.ctrl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.utils.ByteUtils;
import rocky.ctrl.utils.ObjectSerializer;

public class AdvancedLCVDStorage extends BasicLCVDStorage {

	String nodeID;
	public static final long MAX_SIZE = 51200; // HARD-CODED  512 bytes * 100
	public static final int blockSize = 512;
	
	public GenericKeyValueStore blockDataStore;
	public GenericKeyValueStore mutationStore;
	public List<MutationRecord> mutationLog;
	public GenericKeyValueStore versionMap;
	
	public class MutationRecord implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5679557532754097569L;
		public long blockID;
		public long epoch;
		public int blockHash;
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
	
	public AdvancedLCVDStorage(String exportName) {
		// TODO Auto-generated constructor stub
		super(exportName, false);	
		CloudPackageManager cpm = new CloudPackageManager(queue);
		cloudPackageManagerThread = new Thread(cpm);
		Prefetcher prefetcher = new Prefetcher(this);
		prefetcherThread = new Thread(prefetcher);
		roleSwitcherThread = new Thread(new RoleSwitcher());
		roleSwitcherThread.start();
		cui = new ControlUserInterfaceRunner(roleSwitcherThread);
		controlUIThread = new Thread(cui);
		controlUIThread.start();
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
		
	}

	@Override
	public void disconnect() {
//		if (opened) {
//			opened = false;		
//		} else {
//		      throw new IllegalStateException("Not connected to " + exportName);
//	    }
		super.disconnect();
	}
	
	public long getRealBlockID(long epoch, int blockID) {
		return epoch << 32 + blockID;
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
		
		System.out.println("read entered. buffer size=" + buffer.length);
		System.out.println("offset=" + offset);
		
		if (offset >= (1 << 32)) {
			System.err.println("ASSERT: offset cannot be larger than or equal to " + (1 << 32));
			System.exit(1);
		}
		
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
	    for (int i = (int) firstBlock; i <= (int) lastBlock; i++) {
	    	boolean isPresent = false;
	    	synchronized(presenceBitmap) {
	    		isPresent = presenceBitmap.get(i);
	    	}
	    	if (isPresent) {
	    		System.out.println("blockID=" + i + " is locally present");
	    		//super.read(blockData, i * blockSize);
				//System.out.println("i=" + i);
				//System.out.println("firstBlock=" + firstBlock);
				//System.out.println("blockData length=" + blockData.length);
				//System.out.println("buffer length=" + buffer.length);
				//System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
			} else {
				byte[] epochBytes = new byte[Long.BYTES];
		    	byte[] blockData = new byte[blockSize];
		    	byte[] fetchedBytes = new byte[Long.BYTES + blockSize];
		    	
				System.out.println("blockID=" + i + " is NOT locally present");
				// read from the cloud backend (slow path)
				
				try {
					//blockData = blockDataStore.get(String.valueOf(i));
					fetchedBytes = blockDataStore.get(String.valueOf(i));
					System.arraycopy(fetchedBytes, 0, epochBytes, 0, Long.BYTES);
					System.arraycopy(fetchedBytes, Long.BYTES, blockData, 0, blockSize);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.arraycopy(blockData, 0, buffer, (int) ((i - firstBlock) * blockSize), blockSize);
				//prefetchWrite(blockData, i * blockSize);
				long blockEpoch = ByteUtils.bytesToLong(epochBytes);
				long lcvdOffset = (blockEpoch << 32) + i;
				try {
					versionMap.put(i + "", epochBytes);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//prefetchWrite(blockData, i * blockSize);
				prefetchWrite(blockData, lcvdOffset);
				prefetchFlush();
				//super.write(blockData, i * blockSize);
				//super.flush();
				synchronized(presenceBitmap) {
					presenceBitmap.set(i);
				}
			}
	    }
	    
		return super.readPassthrough(buffer, offset);
	}

	@Override
	public CompletableFuture<Void> write(byte[] buffer, long offset) {
//		// TODO Auto-generated method stub
//		return null;

		long blockID = offset / blockSize;
		long curEpoch = epochCnt + 1;
		try {
			versionMap.put("" + blockID, ByteUtils.longToBytes(curEpoch));
		} catch (IOException e) {
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
	 * Periodic Flushing: Only enable for Owner
	 */

	@Override
	public void instantCloudFlushing() {
		WriteRequest wr = null;
		while ((wr = queue.poll()) != null) {
			synchronized(writeMap) {
				writeMap.put((int) (wr.offset / blockSize), wr.buf);
			}
		}
		Thread lastFlusherThread = new Thread(new CloudFlusherAdvanced());
		lastFlusherThread.start();
		try {
			lastFlusherThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public class CloudPackageManagerAdvanced extends CloudPackageManager {
		
		public CloudPackageManagerAdvanced(BlockingQueue<WriteRequest> q) {
			super(q);
		}

		@Override
		public void run() {
			System.out.println("[CloudPackageManagerAdvanced] run entered");
			try {
				Timer timer = new Timer();
				timer.schedule(new CloudFlusherAdvanced(), RockyController.epochPeriod);
				while (true) { 
					WriteRequest wr = q.take();
			    	System.out.println("[CloudPackageManagerAdvanced] dequeued WriteRequest for blockID=" 
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
				System.out.println("[CloudPackageManagerAdvanced] Get interrupted");
			}
			System.out.println("[CloudPackageManagerAdvanced] Terminating CloudPackageManagerAdvanced Thread");
		}
	}
	
	public class CloudFlusherAdvanced extends CloudFlusher {
		@Override
		public void run() {
			System.out.println("[CloudFlusherAdvanced] Entered CloudFlusher run");
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
			List<MutationRecord> mutationRecordSegment = null;
			for (Integer i : writeMapClone.keySet()) {
				byte[] buf = writeMapClone.get(i);
				System.out.println("For blockID=" + i + " buf is written to the cloud");
				MutationRecord rec = new MutationRecord();
				rec.blockID = i;
				rec.epoch = epochCnt + 1;
				rec.blockHash = buf.hashCode();
				mutationLog.add(rec);
				if (mutationRecordSegment == null) {
					mutationRecordSegment = new ArrayList<MutationRecord>();
				}
				mutationRecordSegment.add(rec);
			}
			long curEpoch = epochCnt + 1;
			byte[] dmBytes = dirtyBitmapClone.toByteArray();
			try {
				System.out.println("dBmStore put for epoch=" + curEpoch);
				mutationStore.put("" + curEpoch, ObjectSerializer.serialize(mutationRecordSegment));
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
	
}
