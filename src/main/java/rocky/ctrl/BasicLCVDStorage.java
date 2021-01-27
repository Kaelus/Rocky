package rocky.ctrl;

import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.cloud.ValueStorageDynamoDB;

public class BasicLCVDStorage extends FDBStorage {

	String nodeID;
	public static final long MAX_SIZE = 51200; // HARD-CODED  512 bytes * 100
	public static final int blockSize = 512;
	
	public BitSet presenceBitmap;
	public BitSet dirtyBitmap;

	public String pBmTableName = "presenceBitmapTable";
	public String dBmTableName = "dirtyBitmapTable";
	public String blockDataTableName = "blockDataTable";
	
	GenericKeyValueStore pBmStore;
	GenericKeyValueStore dBmStore;
	GenericKeyValueStore blockDataStore;
			
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
	    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
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
	    for (int i = (int) firstBlock; i < (int) lastBlock; i++) {
	    	synchronized(dirtyBitmap) {
	    		dirtyBitmap.set(i);
	    	}
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
}
