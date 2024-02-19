package rocky.recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.google.common.base.Charsets;

import rocky.communication.Message;
import rocky.communication.MessageType;
import rocky.communication.PeerCommunication;
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

	// recovery for cloud dead situation
	public static ArrayList<String> nonCoordinatorWaitingList;
	public static Boolean canSendResponse;
	public static long latestOwnerEpoch; // latest e1
	public static long latestPrefetchEpoch; // latest e2
	public static String epochLeader;
	public static String prefetchLeader;
	public static String contiguousEpochListL;
	public static long latestEpochInL;
	
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
		
		// additional initialization for dead cloud situation
		if (hasCloudFailed) {
			nonCoordinatorWaitingList = new ArrayList<String>();
			canSendResponse = false;
			latestOwnerEpoch = 0;
			latestPrefetchEpoch = 0;
			epochLeader = null;
			prefetchLeader = null;
		}
		System.out.println("Initialization of variables is done.");	
	}
	
	protected static boolean isCoordinator() {
		boolean retBool = false;
		
		if (RockyController.nodeID.equals(coordinatorID)) {
			retBool = true;
		}
		
		return retBool;
	}
	
	private static boolean arrayListsHasSameContents(ArrayList<String> wl, ArrayList<String> pl) {
		boolean retBool = true;
		if (wl.size() == pl.size()) {
			for (String s : wl) {
				if (!pl.contains(s)) {
					retBool = false;
					break;
				}
			}
		}
		return retBool;
	}
	
	protected static void waitsForNonCoordinators() {
			synchronized(nonCoordinatorWaitingList) {
				while (!arrayListsHasSameContents(nonCoordinatorWaitingList, RockyController.peerAddressList)) {
					try {
						nonCoordinatorWaitingList.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
	}
	
	protected static void prepareRecoveryDeadCloud() {
		if (isCoordinator()) {
			nonCoordinatorWaitingList.clear();
			waitsForNonCoordinators();
			synchronized(canSendResponse) {
				canSendResponse.notifyAll();
			}
		} else {
			Message ackMsg = RockyStorage.pCom.sendPeerRequest(coordinatorID, PeerCommunication.PeerRequestType.CLOUD_FAILURE_RECOVERY_PREP_REQUEST);
			if (ackMsg.msgType != MessageType.MSG_T_ACK) {
				DebugLog.elog("ERROR: We haven't implemented retry for peer request for cloud failure recovery preparation yet. It is error.");
				System.exit(1);
			}
			String retStr = (String) ackMsg.msgContent;
			if (retStr == null) {
				DebugLog.elog("ASSERT: server sents null content for ack");
				System.exit(1);
			}
			hasCloudFailed = Boolean.parseBoolean(retStr.split(";")[0]);
			epochEa = Long.parseLong(retStr.split(";")[1]);
			DebugLog.log("Server sent: hasCloudFailed=" + hasCloudFailed + " and epochEa=" + epochEa);
		}
	}
	
	protected static void initializationProcedure() {
		if (isCoordinator()) {
			nonCoordinatorWaitingList.clear();
			waitsForNonCoordinators();
			synchronized(canSendResponse) {
				canSendResponse.notifyAll();
			}
		} else {
			try {
				byte[] eoBytes = RockyStorage.localEpochBitmaps.get("epochsOwned");
				String eoString = new String(eoBytes, Charsets.UTF_8);
				DebugLog.log("epochsOwned=" + eoString);
				byte[] peBytes = RockyStorage.localBlockSnapshotStore.get("prefetchedEpochs");
				String peString = new String(peBytes, Charsets.UTF_8);
				DebugLog.log("prefetchedEpochs=" + peString);
				String[] eoTokens = eoString.split(";");
				String[] peTokens = peString.split(";");
				long eoLong = 0;
				long latestEpochOwned = 0;
				for (String t1 : eoTokens) {
					eoLong = Long.parseLong(t1);
					if (eoLong >= epochEa) {
						break;
					} else {
						if (eoLong > latestEpochOwned) {
							latestEpochOwned = eoLong;
						}
					}
				}
				long peLong = 0;
				long latestPrefetchedEpoch = 0;
				for (String t2 : peTokens) {
					peLong = Long.parseLong(t2);
					if (peLong >= epochEa) {
						break;
					} else {
						if (peLong > latestPrefetchedEpoch) {
							latestPrefetchedEpoch = peLong;
						}
					}
				}
				Message ackMsg = RockyStorage.pCom.sendPeerRequest(coordinatorID, PeerCommunication.PeerRequestType.CLOUD_FAILURE_RECOVERY_IP_REQUEST);
				if (ackMsg.msgType != MessageType.MSG_T_ACK) {
					DebugLog.elog("ERROR: We haven't implemented retry for peer request for cloud failure recovery initialization procedure yet. It is error.");
					System.exit(1);
				}
				String retStr = (String) ackMsg.msgContent;
				if (retStr == null) {
					DebugLog.elog("ASSERT: server sents null content for ack");
					System.exit(1);
				}
				String[] retStrTokens = retStr.split(";");
				latestOwnerEpoch = Long.parseLong(retStrTokens[0]);
				epochLeader = retStrTokens[1];
				latestPrefetchEpoch = Long.parseLong(retStrTokens[2]);
				prefetchLeader = retStrTokens[3];
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private static boolean isPrefetchLeader() {
		boolean retBool = false;
		
		if (RockyController.nodeID.equals(prefetchLeader)) {
			retBool = true;
		}
		
		return retBool;
	}
	
	private static void uploadEpochBitmapsForEpochsOwned() {
		long beginEpoch = 1;
		long endEpoch = latestOwnerEpoch;	
		ArrayList<String> epochsOwned = null;
		byte[] epochBitmap = null;
		try {
			byte[] epochsOwnedBytes = RockyStorage.localEpochBitmaps.get("epochsOwned");
			String epochsOwnedStr = new String(epochsOwnedBytes, Charsets.UTF_8);
			String[] epochsOwnedStrArr = epochsOwnedStr.split(";");
			epochsOwned = new ArrayList<String>();
			for (int i = 0; i < epochsOwnedStrArr.length; i++) {
				epochsOwned.add(epochsOwnedStrArr[i]);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (long i = beginEpoch; i <= endEpoch; i++) {
			if (epochsOwned.contains(i + "")) {
				try {
					epochBitmap = RockyStorage.localEpochBitmaps.get(i + "-bitmap");
					RockyStorage.cloudEpochBitmaps.put(i + "-bitmap", epochBitmap);
					RockyStorage.cloudEpochBitmaps.put(i + "-owner", RockyController.nodeID.getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private static void uploadCoherentBlockDeviceSnapshot() {
		System.out.println("uploadCoherentBlockDeviceSnapshot entered");
		byte[] epochBitmap = null;
		long beginEpoch = 1;
		long endEpoch = latestPrefetchEpoch;
		byte[] resetValue;
		byte[] epochAndBlockIDPairBytes;
		String epochAndBlockIDPairStr;

		// if this node is not the prefetchLeader, we do nothing and just return here.
		if (!prefetchLeader.equals(RockyController.nodeID)) {
			return;
		}

		// collecting the pointer to the latest block snapshot that is earlier than latestPrefetchEpoch
		for (long i = beginEpoch; i <= endEpoch; i++) {
			if (RockyStorage.debugPrintoutFlag) {
				DebugLog.log("beginEpoch=" + beginEpoch 
					+ " i=" + i + " endEpoch=" + endEpoch);
			}
			try {
				epochBitmap = RockyStorage.localEpochBitmaps.get(i + "-bitmap");
				if (epochBitmap == null) {
					DebugLog.elog("ASSERT: failed to load " + i + "-bitmap");
					System.exit(1);
				} else {
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("epochBitmap is loaded for epoch=" + i);
					}
					BitSet epochBitmapBitSet = BitSet.valueOf(epochBitmap);
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("about to enter the loop collecting the latest block snapshots");
					}
					for (int j = epochBitmapBitSet.nextSetBit(0); j >= 0; j = epochBitmapBitSet.nextSetBit(j+1)) {
						// operate on index i here
					    if (i == Integer.MAX_VALUE) {
					    	break; // or (i+1) would overflow
					    }
					    localBlockResetBitmap.set(j);
					    localBlockResetEpochAndBlockIDPairStore.put(j + "", (i + ":" + j).getBytes());
					}
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("finished with collecting the latest block snapshot for epoch=" + i);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// reading the block snapshots from the local storage and upload to the cloud
		System.out.println("uploading to cloud starts");
		for (int i = localBlockResetBitmap.nextSetBit(0); i >= 0; i = localBlockResetBitmap.nextSetBit(i+1)) {
			System.out.println("block ID=" + i + " needs to be updated");
			epochAndBlockIDPairBytes = localBlockResetEpochAndBlockIDPairStore.get(i + "");
			epochAndBlockIDPairStr = new String(epochAndBlockIDPairBytes, Charsets.UTF_8);
			System.out.println("epochAndBlockIDPairStr=" + epochAndBlockIDPairStr);
			try {
				resetValue = RockyStorage.localBlockSnapshotStore.get(epochAndBlockIDPairStr);
				System.out.println("resetValue String=" + new String(resetValue, Charsets.UTF_8));
				RockyStorage.cloudBlockSnapshotStore.put(epochAndBlockIDPairStr, resetValue);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("uploadCoherentBlockDeviceSnapshot is done");
	}
	
	protected static void constructContiguousEpochList() {
		contiguousEpochListL = "";
		byte[] epochOwnerBytes = null;
		String epochOwnerString = null;
		for (long i = latestPrefetchEpoch + 1; i < latestOwnerEpoch; i++) {
			try {
				epochOwnerBytes = RockyStorage.cloudEpochBitmaps.get(i + "-owner");
				epochOwnerString = new String(epochOwnerBytes, Charsets.UTF_8);
				if (RockyController.peerAddressList.contains(epochOwnerString)) {
					contiguousEpochListL += i + ";";
				} else {
					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected static void initialRecoveryProcedure() {

		uploadEpochBitmapsForEpochsOwned();
		
		if (isPrefetchLeader()) {
			uploadCoherentBlockDeviceSnapshot();
		}
		
		if (isCoordinator()) {
			nonCoordinatorWaitingList.clear();
			waitsForNonCoordinators();
			constructContiguousEpochList();
			synchronized(canSendResponse) {
				canSendResponse.notifyAll();
			}
		} else {
			Message ackMsg = RockyStorage.pCom.sendPeerRequest(coordinatorID, PeerCommunication.PeerRequestType.CLOUD_FAILURE_RECOVERY_IRP_REQUEST);
			if (ackMsg.msgType != MessageType.MSG_T_ACK) {
				DebugLog.elog("ERROR: We haven't implemented retry for peer request for cloud failure recovery initialization procedure yet. It is error.");
				System.exit(1);
			}
			String retStr = (String) ackMsg.msgContent;
			if (retStr == null) {
				DebugLog.elog("ASSERT: server sents null content for ack");
				System.exit(1);
			}
			contiguousEpochListL = retStr;
		}
	}
	
	protected static void uploadForwardingMutationSnapshot() {
		String[] epochTokens = contiguousEpochListL.split(";");
		ArrayList<String> epochArrList = new ArrayList<String>();
		byte[] epochsOwnedBytes = null;
		String epochsOwnedStr = null;
		String[] epochsOwnedTokens = null;
		byte[] ownedDirtyBitmapBytes = null;
		BitSet ownedDirtyBitmapBitSet = null;
		String epochBlockID = null;
		byte[] blockSnap = null;
		for (int i = 0; i < epochTokens.length; i++) {
			epochArrList.add(epochTokens[i]);
		}
		try {
			epochsOwnedBytes = RockyStorage.localEpochBitmaps.get("epochsOwned");
			epochsOwnedStr = new String(epochsOwnedBytes, Charsets.UTF_8);
			epochsOwnedTokens = epochsOwnedStr.split(";");
			for (int i = 0; i < epochsOwnedTokens.length; i++) {
				String epochOwned = epochsOwnedTokens[i];
				if (epochArrList.contains(epochOwned)) {
					ownedDirtyBitmapBytes = RockyStorage.localEpochBitmaps.get(epochOwned + "-bitmap");
					ownedDirtyBitmapBitSet = BitSet.valueOf(ownedDirtyBitmapBytes);
					epochBlockID = null;
					for (int j = ownedDirtyBitmapBitSet.nextSetBit(0); j >= 0; j = ownedDirtyBitmapBitSet.nextSetBit(j+1)) {
						epochBlockID = epochOwned + ":" + j;
						blockSnap = RockyStorage.localBlockSnapshotStore.get(epochBlockID);
						RockyStorage.cloudBlockSnapshotStore.put(epochBlockID, blockSnap);
					}
				}
			}
			latestEpochInL = Long.parseLong(epochsOwnedTokens[epochsOwnedTokens.length - 1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected static void resetNewCloudEpochCount() {
		
		try {
			RockyStorage.cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(latestEpochInL));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	protected static void resetPresenceBitmapNewCloudAndLocal() {
		RockyStorage.presenceBitmap.clear();
		byte[] pBmBytes = RockyStorage.presenceBitmap.toByteArray();
		try {
			RockyStorage.localEpochBitmaps.put(RockyController.nodeID + "-pBm", pBmBytes);
			RockyStorage.cloudEpochBitmaps.put(RockyController.nodeID + "-pBm", pBmBytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected static void resetLocalEpochMetadataState() {
		byte[] epochsOwnedBytes;
		byte[] prefetchedEpochsBytes;
		try {
			// reset epochsOwned
			epochsOwnedBytes = RockyStorage.localEpochBitmaps.get("epochsOwned");
			String epochsOwnedString = new String(epochsOwnedBytes, Charsets.UTF_8);
			String[] epochsOwnedTokens = epochsOwnedString.split(";");
			String resetEpochsOwnedStr = "";
			for (int i = 0; i < epochsOwnedTokens.length; i++) {
				int epochOwned = Integer.parseInt(epochsOwnedTokens[i]);
				if (epochOwned > latestEpochInL) {
					break;
				} else {
					resetEpochsOwnedStr += (epochOwned + ";");
				}
			}
			RockyStorage.localEpochBitmaps.put("epochsOwned", resetEpochsOwnedStr.getBytes());
			
			// reset prefetchedEpochs
			prefetchedEpochsBytes = RockyStorage.localBlockSnapshotStore.get("prefetchedEpochs");
			String prefetchedEpochsString = new String(prefetchedEpochsBytes, Charsets.UTF_8);
			String[] prefetchedEpochsTokens = prefetchedEpochsString.split(";");
			String resetPrefetchedEpochsStr = "";
			long lastPrefetchedEpochLessThanLatestEpochInL = 0;
			for (int i = 0; i < prefetchedEpochsTokens.length; i++) {
				int prefEpoch = Integer.parseInt(prefetchedEpochsTokens[i]);
				if (prefEpoch > latestEpochInL) {
					break;
				} else {
					resetPrefetchedEpochsStr += (prefEpoch + ";");
					lastPrefetchedEpochLessThanLatestEpochInL = prefEpoch;
				}
			}
			RockyStorage.localBlockSnapshotStore.put("prefetchedEpochs", resetPrefetchedEpochsStr.getBytes());
			RockyStorage.cloudBlockSnapshotStore.put("PrefetchedEpoch-" + RockyController.nodeID, (lastPrefetchedEpochLessThanLatestEpochInL + "").getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	protected static void prefetchForDeadCloudRecovery() {
		try {
			RockyStorage.prefetcherThread.start();
		} catch (IllegalThreadStateException itse) {
			DebugLog.log("prefetcherThreadThread has stopped. Recreate to start again.");
			RockyStorage.prefetcherThread = new Thread(RockyStorage.prefetcher);
		}	
		while (true) {
			try {
				byte[] lastPrefetchedEpochBytes = RockyStorage.cloudBlockSnapshotStore.get("PrefetchedEpoch-" + RockyController.nodeID);
				String lastPrefetchedEpochStr = new String(lastPrefetchedEpochBytes, Charsets.UTF_8);
				long lastPrefetchedEpoch = Long.parseLong(lastPrefetchedEpochStr);
				if (RockyStorage.debugPrintoutFlag) {
					DebugLog.log("checking if prefetcher thread has done its job");
				}
				if (lastPrefetchedEpoch == latestOwnerEpoch ) {
					if (RockyStorage.debugPrintoutFlag) {
						DebugLog.log("prefetcher thread has done its job! break the loop.");
					}
					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				if (RockyStorage.debugPrintoutFlag) {
					DebugLog.log("prefetcher thread has NOT done its job yet. Wait for 1 sec and check again.");
				}
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected static void forwardRecoveryProcedure() {
		
		uploadForwardingMutationSnapshot();
		
		if (isCoordinator()) {
			nonCoordinatorWaitingList.clear();
			waitsForNonCoordinators();
			resetNewCloudEpochCount();
			resetPresenceBitmapNewCloudAndLocal();
			resetLocalEpochMetadataState();
			synchronized(canSendResponse) {
				canSendResponse.notifyAll();
			}
		} else {
			Message ackMsg = RockyStorage.pCom.sendPeerRequest(coordinatorID, PeerCommunication.PeerRequestType.CLOUD_FAILURE_RECOVERY_FRP_REQUEST);
			if (ackMsg.msgType != MessageType.MSG_T_ACK) {
				DebugLog.elog("ERROR: We haven't implemented retry for peer request for cloud failure recovery initialization procedure yet. It is error.");
				System.exit(1);
			}
			String retStr = (String) ackMsg.msgContent;
			if (retStr == null) {
				DebugLog.elog("ASSERT: server sents null content for ack");
				System.exit(1);
			}
		}
		prefetchForDeadCloudRecovery();
		
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

			// 1. Preparation
			prepareRecoveryDeadCloud();
			
			// 2. IP (Initialization Procedure)
			initializationProcedure();
			
			// 3. IRP (Initial Recovery Procedure)
			initialRecoveryProcedure();
			
			// 4. FRP (Forward Recovery Procedure)
			forwardRecoveryProcedure();
			
		}
		
		System.out.println("Recovery is done. Goodbye!");
		System.exit(1);
	}
}
