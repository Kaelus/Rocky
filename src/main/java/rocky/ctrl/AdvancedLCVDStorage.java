package rocky.ctrl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;

import rocky.ctrl.BasicLCVDStorage.WriteRequest;
import rocky.ctrl.MutationLog.BlockVersion;
import rocky.ctrl.MutationLog.MutationRecord;
import rocky.ctrl.RockyController.RockyControllerRoleType;
import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.utils.ByteUtils;

public class AdvancedLCVDStorage extends BasicLCVDStorage {

	String nodeID;
	public static final long MAX_SIZE = 51200; // HARD-CODED  512 bytes * 100
	public static final int blockSize = 512;
	
	public GenericKeyValueStore blockDataStore;
	public GenericKeyValueStore mutationLog;
	public GenericKeyValueStore versionMap;
	
	public HashMap<Long, MutationRecord> mutationMap;
	
	public class MutationRecord implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5679557532754097569L;
		public long blockID;
		public BlockVersion version;
		public long timestamp;
		public byte[] blockHash;
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
		super(exportName);		
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
				byte[] epochBytes = null;
		    	byte[] blockData = null;
		    	byte[] fetchedBytes = null;
				epochBytes = new byte[Long.SIZE];
		    	blockData = new byte[blockSize];
		    	fetchedBytes = new byte[Long.SIZE + blockSize];
		    	
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
	    	//wr.offset = offset;
		    //wr.offset = i * blockSize;
	    	wr.offset = getRealBlockID(epochCnt, i) * blockSize;
		    try {
		    	//System.out.println("[BasicLCVDStorage] enqueuing WriteRequest for blockID=" 
		    	//		+ ((int) wr.offset / blockSize));
				queue.put(wr);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
		return super.writePassthrough(buffer, offset);
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

	
	
}
