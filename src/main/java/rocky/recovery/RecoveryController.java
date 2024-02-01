package rocky.recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.google.common.base.Charsets;

import rocky.ctrl.FDBArray;
import rocky.ctrl.RockyController;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.ValueStorageLevelDB;
import rocky.ctrl.utils.ByteUtils;
import rocky.ctrl.utils.DebugLog;
import rocky.recovery.Coordinator.BackendStorageType;

public class RecoveryController {
	
	// storage related
	public static RockyStorage storage;
	
	// connection related
	private static String coordinatorID;
	
	// recovery related
	public static long epochEa;
	public static boolean hasCloudFailed;
	public static BitSet localBlockResetBitmap;
	public static ValueStorageLevelDB localBlockResetEpochAndBlockIDPairStore;

	private static void parseRecoveryConfig(String configFile) {
		File confFile = new File(configFile);
		if (!confFile.exists()) {
			if (!confFile.mkdir()) {
				System.err.println("Unable to find " + confFile);
	            System.exit(1);
	        }
		}
		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
		    String line;
			while ((line = br.readLine()) != null) {
				if (line.startsWith("e_a")) {
					String[] tokens = line.split("=");
					String rollbackEpochStr = tokens[1];
					epochEa = Integer.parseInt(rollbackEpochStr);
					if (epochEa > RockyStorage.epochCnt) {
						System.err.println("ASSERT: epochEa should not be greater than the latest epoch.");
						System.exit(1);
					}
				} else if (line.startsWith("cloud_failure")) {
					String[] tokens = line.split("=");
					hasCloudFailed = Boolean.parseBoolean(tokens[1]);
				} else if (line.startsWith("coordinator")) {
					String[] tokens = line.split("=");
					coordinatorID = tokens[1];					
				} 
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static void printStaticVariables() {
		DebugLog.log("epochEa=" + epochEa);
		DebugLog.log("hasCloudFailed=" + hasCloudFailed);
		DebugLog.log("coordinatorID=" + coordinatorID);
	}
	
	protected static void recoverCloud() {
		DebugLog.log("Inside rollbackCloudNode.");
		try {
			RockyStorage.cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(epochEa - 1));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected static void recoverVersionMap(long beginEpoch, long endEpoch) {
		System.out.println("recoverVersionMap beginEpoch=" + beginEpoch + " endEpoch=" + endEpoch);
		byte[] epochBitmap = null;
		for (long i = beginEpoch; i <= endEpoch; i++) {
			if (RockyStorage.debugPrintoutFlag) {
				DebugLog.log("beginEpoch=" + beginEpoch 
					+ " i=" + i + " endEpoch=" + endEpoch);
			}
			try {
				epochBitmap = RockyStorage.cloudEpochBitmaps.get(i + "-bitmap");
				if (epochBitmap == null) {
					DebugLog.elog("ASSERT: failed to fetch " + i + "-bitmap");
					System.exit(1);
				} else {
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("epochBitmap is received for epoch=" + i);
					}
					BitSet epochBitmapBitSet = BitSet.valueOf(epochBitmap);
					RockyStorage.localEpochBitmaps.put(i + "-bitmap", epochBitmap);
					byte[] thisEpochBytes = ByteUtils.longToBytes(i);
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("about to enter the loop updating versionMap");
					}
					for (int j = epochBitmapBitSet.nextSetBit(0); j >= 0; j = epochBitmapBitSet.nextSetBit(j+1)) {
						// operate on index i here
					    if (i == Integer.MAX_VALUE) {
					    	break; // or (i+1) would overflow
					    }
					    RockyStorage.versionMap.put(j + "", thisEpochBytes);
					    localBlockResetBitmap.set(j);
					    localBlockResetEpochAndBlockIDPairStore.put(j + "", (i + ":" + j).getBytes());
					}
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("finished with updating versionMap for epoch=" + i);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("recoverVersionMap is done");
	}
		
	protected static void recoverRockyStorage(long beginEpoch, long endEpoch) {
		// fetch required blocks from CBSS and store it into the rocky storage (i.e. FDBArray).
		System.out.println("resetRockyStorage entered");
		byte[] resetValue;
		byte[] epochAndBlockIDPairBytes;
		String epochAndBlockIDPairStr;
		Database db = FDB.selectAPIVersion(510).open();
		System.out.println("FDBArray opened");
		FDBArray fdbArray = FDBArray.open(db, RockyController.lcvdName);
		for (int i = localBlockResetBitmap.nextSetBit(0); i >= 0; i = localBlockResetBitmap.nextSetBit(i+1)) {
			System.out.println("block ID=" + i + " needs to be updated");
			epochAndBlockIDPairBytes = localBlockResetEpochAndBlockIDPairStore.get(i + "");
			epochAndBlockIDPairStr = new String(epochAndBlockIDPairBytes, Charsets.UTF_8);
			System.out.println("epochAndBlockIDPairStr=" + epochAndBlockIDPairStr);
			try {
				resetValue = RockyStorage.cloudBlockSnapshotStore.get(epochAndBlockIDPairStr);
				System.out.println("resetValue String=" + new String(resetValue, Charsets.UTF_8));
				RockyStorage.localBlockSnapshotStore.put(epochAndBlockIDPairStr, resetValue);
				fdbArray.write(resetValue, i * RockyStorage.blockSize);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("resetRockyStorage is done");
	}
	
	protected static void recoverLocal() {
		DebugLog.log("Inside rollbackLocalNode. TBD");
		recoverVersionMap(1, epochEa - 1);
		recoverRockyStorage(1, epochEa - 1);

		// presence bitmap should be reset to 1 for all bits
		RockyStorage.presenceBitmap.set(0, RockyStorage.numBlock);
	}
	
	public static void initialize() {
		// initialize required static variables
		storage = new RockyStorage(RockyController.lcvdName);
		coordinatorID = "127.0.0.1:10810";
		epochEa = -1;
		hasCloudFailed = false;
		RecoveryController.localBlockResetBitmap = new BitSet(RockyStorage.presenceBitmap.length());
		try {
			RecoveryController.localBlockResetEpochAndBlockIDPairStore = new ValueStorageLevelDB(RockyStorage.prefixPathForLocalStorage + "-localBlockResetEpochAndBlockIDPairStoreTable");
			RecoveryController.localBlockResetEpochAndBlockIDPairStore.clean(); // clean the effect from the previous run 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Initialization of variables is done.");	
	}
	
	public static void runRecovery(String[] args) {
		System.out.println("Entered RecoveryController runRecovery!");
		
		initialize();
		
		//update variables using config if given
		if (args.length < 2) {
			System.out.println("no config file is given.");
		} else if (args.length >= 2) {
			System.out.println("given config file=" + args[1]);
			parseRecoveryConfig(args[1]);
		}
		printStaticVariables();
		
		if (!hasCloudFailed) { // if cloud is alive
			if (RockyController.nodeID.equals(RecoveryController.coordinatorID)) {
				recoverCloud();
			}
			recoverLocal();
		} else { // if cloud is dead
			// TBD
		}
		
		System.out.println("Recovery is done. Goodbye!");
		System.exit(1);
	}
}
