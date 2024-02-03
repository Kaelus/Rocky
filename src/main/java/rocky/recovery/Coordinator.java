package rocky.recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.fusesource.leveldbjni.internal.NativeDB.DBException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.google.common.base.Charsets;

import rocky.communication.Message;
import rocky.communication.MessageType;
import rocky.communication.ReceiverXMLRPC;
import rocky.ctrl.FDBArray;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.ValueStorageLevelDB;
import rocky.ctrl.cloud.GenericKeyValueStore;
import rocky.ctrl.cloud.ValueStorageDynamoDB;
import rocky.ctrl.utils.ByteUtils;
import rocky.ctrl.utils.DebugLog;
import rocky.ctrl.utils.ObjectSerializer;

public class Coordinator {

	// storage related
	public enum BackendStorageType {DynamoDBLocal, DynamoDB_SEOUL, 
		DynamoDB_LONDON, DynamoDB_OHIO, Unknown};
	public static BackendStorageType backendStorage;
	public static String exportName;
	public static String rockyLocalStoragePath;
	
	// connection related
	private static String myIP;
	private static String myPort;
	private static String coordinatorID;
	private static ReceiverXMLRPC receiver;
	public static ArrayList<String> recoveryCommittee; // contains all endpoints participating in recovery except for this coordinator
	
	// logging info
	public String loggerID;
	public static String myID;
	public static int myInstanceNo;
	
	// debugging related
	public static boolean debugPrintoutFlag;
	
	// metadata related (local and cloud)
	// public String pBmTableName = "presenceBitmapTable";
	public static String cloudEpochBitmapsTableName;
	public static String localEpochBitmapsTableName;
	public static String cloudBlockSnapshotStoreTableName;
	public static String versionMapTableName;
	public static String localBlockSnapshotStoreTableName;

	// public GenericKeyValueStore pBmStore;
	public static GenericKeyValueStore cloudEpochBitmaps;
	public static GenericKeyValueStore localEpochBitmaps;
	public static GenericKeyValueStore cloudBlockSnapshotStore;
	public static GenericKeyValueStore versionMap;
	public static GenericKeyValueStore localBlockSnapshotStore;

	// recovery related (common)
	public static long epochEa;
	public static long epochEp;
	public static boolean hasAllCommitteeNodes;
	
	// recovery related (no cloud failure)
	public static boolean hasCloudFailed;
	public static NoCloudFailureRecoveryWorker ncfrWorker;
	public static Thread noCloudFailureRecoveryWorkerThread;
	public static boolean noCloudFailureRecoveryFlag;
	public static int handleNoCloudFailureRecoveryCount;
		
	public static LinkedBlockingQueue<EndpointController> endpoints;
	
	public class EndpointController {
		public String endpointID;
		public String ipStr;
		public String portStr;
		
		public LinkedBlockingQueue<Message> reqQueue;
		
		public EndpointController() {
			reqQueue = new LinkedBlockingQueue<Message>();
		}
	}
	
	/**
	 * This constructor is called multiple times. On client's request, WebServer
	 * creates this class' instance on the process of creating a separate worker to
	 * handle the request. (out of our control)
	 * 
	 * Shared variable instantiation should be done in another constructor that is
	 * used for starting up the WebServer
	 * 
	 * NOTE: This should not be called by code that is not WebServer
	 * 
	 * @throws Exception
	 */
	public Coordinator() throws Exception {
		if (loggerID == null) {
			this.loggerID = myID + (myInstanceNo++);
		}
		DebugLog.log("Coordinator constructor each worker thread");
	}

	/**
	 * This constructor is to be used to start up the WebServer. This is meant to be
	 * called only once. Also, this should be the only constructor used by other
	 * than WebServer.
	 * 
	 * @param srvName
	 * @throws Exception
	 */
	public Coordinator(String coordinatorName) {
		loggerID = "Coordinator" + coordinatorName;
		cloudEpochBitmapsTableName = exportName + "-cloudEpochBitmapsTable";
		localEpochBitmapsTableName = rockyLocalStoragePath + "/" + exportName + "-localEpochBitmapsTable";
		cloudBlockSnapshotStoreTableName = exportName + "-cloudBlockSnapshotStoreTable";
		versionMapTableName = rockyLocalStoragePath + "/" + exportName + "-versionMapTable";
		localBlockSnapshotStoreTableName = rockyLocalStoragePath + "/" + exportName + "-localBlockSnapshotStoreTable";
		
		if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDBLocal)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, true);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_SEOUL)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_LONDON)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_OHIO)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
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
		} catch (DBException e2) {
			DebugLog.log("DBException occurred. probably under test. handle it gracefully.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		// get prefetched epoch and the latest epoch from the cloud
		epochEp = getPrefetchedEpoch();
		epochEa = getEpoch();
		DebugLog.log("epochEp=" + epochEp + " epochEa=" + epochEa);
	}

	public void runServer() {
		receiver = new ReceiverXMLRPC(Integer.valueOf(myPort));
	}

	public void stopServer() {
		receiver.webServer.shutdown();
	}

	/*
	 * This function handles Endpoint's request accordingly
	 */
	public Object handleEndpointRequest(byte[] epReq) throws Exception {

		Message coordiMsg = null;
		Object returnObj = null;
		Message epMsg = (Message) ObjectSerializer.deserialize(epReq);

		DebugLog.log(epMsg.toString());
		DebugLog.log("handleEndpointRequest. Thread ID: " + Thread.currentThread().getId());
		DebugLog.log("Coordinator has received a request in handleEndpointRequest", this.loggerID);

		Message ack;

		switch (epMsg.msgType) {
		case MessageType.MSG_T_REGISTER_ENDPOINT:
			DebugLog.log("Handling A Request: REGISTER_ENDPOINT", this.loggerID);
			returnObj = handleRegisterEndpoint(epMsg);
			ack = new Message();
			if (returnObj == null) {
				ack.msgType = MessageType.MSG_T_NACK;
			} else {
				ack.msgType = MessageType.MSG_T_ACK;
			}
			ack.msgContent = returnObj;
			coordiMsg = (Message) ack;
			break;
		case MessageType.MSG_T_NO_CLOUD_FAILURE_RECOVERY:
			DebugLog.log("Handling A Request: NO_CLOUD_FAILURE_RECOVERY", this.loggerID);
			returnObj = handleNoCloudFailureRecovery(epMsg);
			ack = new Message();
			ack.msgType = MessageType.MSG_T_ACK;
			ack.msgContent = returnObj;
			coordiMsg = (Message) ack;
			break;
		default:
			break;
		}

		// long beginObjSerialization = System.currentTimeMillis();
		Object retObj = ObjectSerializer.serialize(coordiMsg);
		// long endObjSerialization = System.currentTimeMillis();
		// DebugLog.log("Obj Serialization takes=" + (endObjSerialization -
		// beginObjSerialization) + "ms\n");
		DebugLog.log("Server is responding to a client request", this.loggerID);
		return retObj;
	}

	private Object handleRegisterEndpoint(Message epMsg) {
		Object retObj = null; 
		String[] ipPortPair = (String[]) epMsg.msgContent;
		EndpointController epCtrlr = new EndpointController();
		epCtrlr.ipStr = ipPortPair[0];
		epCtrlr.portStr = ipPortPair[1];
		epCtrlr.endpointID = epCtrlr.ipStr + ":" + epCtrlr.portStr;
		if (!recoveryCommittee.contains(epCtrlr.endpointID)) {
			retObj = null;
		} else {
			synchronized (endpoints) {
				boolean hasEndpointsRegistered = false;
				for (EndpointController cont : endpoints) {
					if (cont.endpointID.equals(epCtrlr.endpointID)) {
						hasEndpointsRegistered = true;
						break;
					}							
				}
				if (!hasEndpointsRegistered) {
					endpoints.add(epCtrlr);
					DebugLog.log("handleRegisterEndpoint registers the endpoint=" 
							+ epCtrlr.endpointID);
					if (endpoints.size() == recoveryCommittee.size()) {
						hasAllCommitteeNodes = true; // this will trigger recovery procedure
					}
					retObj = (Object) epCtrlr.endpointID;
				} else {
					DebugLog.elog("WARN: Repeatedly registering the endpoint=" 
							+ epCtrlr.endpointID);
					retObj = null;
				}
			}
		}
		return retObj;
	}

	private Object handleNoCloudFailureRecovery(Message epMsg) {
		Object retObj = null;
		if (hasAllCommitteeNodes) {
			String[] args = new String[2];
			args[0] = "" + hasCloudFailed;
			args[1] = "" + epochEa; 
			retObj = (Object) args;
			if (++handleNoCloudFailureRecoveryCount == recoveryCommittee.size()) {
				noCloudFailureRecoveryFlag = true;
			}
		} else {
			DebugLog.elog("ERROR: I don't have all committee nodes registered yet!");
			retObj = null;
		}
		
		return (Object) retObj;
	}
	
	public void startNoCloudFailureRecoveryWorker() {
		System.out.println("startNoCloudFailureRecoveryWorker entered");
		ncfrWorker = new NoCloudFailureRecoveryWorker();
		noCloudFailureRecoveryWorkerThread = new Thread(ncfrWorker);
		noCloudFailureRecoveryWorkerIsAliveFlag = true;
		noCloudFailureRecoveryWorkerThread.start();
	}
	
	public void waitNoCloudFailureRecoveryWorker() {
		System.out.println("waiting for the NoCloudFailureRecoveryWorker thread to terminate");
		while (noCloudFailureRecoveryWorkerIsAliveFlag) {
			try {
				//System.out.println("noCloudFailureRecoveryWorker thread is still alive. Wait for 30sec and retry.");
				noCloudFailureRecoveryWorkerThread.join(30000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("noCloudFailureRecoveryWorker thread is now terminated. Waiting is over.");
	}
	
	public void stopNoCloudFailureRecoveryWorker() {
		System.out.println("interrupting the NoCloudFailureRecoveryWorker thread to terminate");
		noCloudFailureRecoveryWorkerThread.interrupt();
		try {
			noCloudFailureRecoveryWorkerThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static boolean noCloudFailureRecoveryWorkerIsAliveFlag;
	public class NoCloudFailureRecoveryWorker implements Runnable {
		public NoCloudFailureRecoveryWorker() {
		}
		@Override
		public void run() {
			DebugLog.log("[NoCloudFailureRecoveryWorker] run entered");
			while(!noCloudFailureRecoveryFlag) {
				DebugLog.log("[NoCloudFailureRecoveryWorker] noCloudFailureRecoveryFlag is false.. wait");
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
					DebugLog.elog("[NoCloudFailureRecoveryWorker] Waiting for noCloudFailureRecoveryFlag " 
							+ "is interrupted unexpectedly.. Stop running this thread");
					return;
				}
			}
			Coordinator.rollbackLocalNode();
			Coordinator.rollbackCloudNode();
			noCloudFailureRecoveryWorkerIsAliveFlag = false;
			DebugLog.log("[NoCloudFailureRecoveryWorker] Terminating NoCloudFailureRecoveryWorker Thread");
		}
	}
	static BitSet localBlockResetBitmap;
	static ValueStorageLevelDB localBlockResetEpochAndBlockIDPairStore;
	protected static void resetVersionMap(long beginEpoch, long endEpoch) {
		System.out.println("resetVersionMap beginEpoch=" + beginEpoch + " endEpoch=" + endEpoch);
		byte[] epochBitmap = null;
		for (long i = beginEpoch; i <= endEpoch; i++) {
			if (debugPrintoutFlag) {
				DebugLog.log("beginEpoch=" + beginEpoch 
					+ " i=" + i + " endEpoch=" + endEpoch);
			}
			try {
				epochBitmap = cloudEpochBitmaps.get(i + "-bitmap");
				if (epochBitmap == null) {
					DebugLog.elog("ASSERT: failed to fetch " + i + "-bitmap");
					System.exit(1);
				} else {
					if (debugPrintoutFlag) {
						DebugLog.log("epochBitmap is received for epoch=" + i);
					}
					if (localBlockResetBitmap == null) {
						localBlockResetBitmap = new BitSet(epochBitmap.length);
						try {
							localBlockResetEpochAndBlockIDPairStore = new ValueStorageLevelDB(exportName + "-localBlockResetEpochAndBlockIDPairStoreTable");
							localBlockResetEpochAndBlockIDPairStore.clean(); // clean the effect from the previous run 
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					BitSet epochBitmapBitSet = BitSet.valueOf(epochBitmap);
					localEpochBitmaps.put(i + "-bitmap", epochBitmap);
					byte[] thisEpochBytes = ByteUtils.longToBytes(i);
					if (debugPrintoutFlag) {
						DebugLog.log("about to enter the loop updating versionMap");
					}
					for (int j = epochBitmapBitSet.nextSetBit(0); j >= 0; j = epochBitmapBitSet.nextSetBit(j+1)) {
						// operate on index i here
					    if (i == Integer.MAX_VALUE) {
					    	break; // or (i+1) would overflow
					    }
					    versionMap.put(j + "", thisEpochBytes);
					    localBlockResetBitmap.set(j);
					    localBlockResetEpochAndBlockIDPairStore.put(j + "", (i + ":" + j).getBytes());
					}
					if (debugPrintoutFlag) {
						DebugLog.log("finished with updating versionMap for epoch=" + i);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("resetVersionMap is done");
	}
	
	protected static void resetRockyStorageFromEBSS(long beginEpoch, long endEpoch) {
		System.out.println("resetRockyStorageFromEBSS entered");
		byte[] resetValue;
		byte[] epochAndBlockIDPairBytes;
		String epochAndBlockIDPairStr;
		Database db = FDB.selectAPIVersion(510).open();
		System.out.println("FDBArray opened");
		FDBArray fdbArray = FDBArray.open(db, exportName);
		for (int i = localBlockResetBitmap.nextSetBit(0); i >= 0; i = localBlockResetBitmap.nextSetBit(i+1)) {
			System.out.println("block ID=" + i + " needs to be updated");
			epochAndBlockIDPairBytes = localBlockResetEpochAndBlockIDPairStore.get(i + "");
			epochAndBlockIDPairStr = new String(epochAndBlockIDPairBytes, Charsets.UTF_8);
			System.out.println("epochAndBlockIDPairStr=" + epochAndBlockIDPairStr);
			try {
				resetValue = localBlockSnapshotStore.get(epochAndBlockIDPairStr);
				System.out.println("resetValue String=" + new String(resetValue, Charsets.UTF_8));
				fdbArray.write(resetValue, i * RockyStorage.blockSize);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("resetRockyStorageFromEBSS is done");
	}
	
	protected static void resetRockyStorageFromCBSS(long beginEpoch, long endEpoch) {
		// TBD: fetch required blocks from CBSS and store it into the rocky storage (i.e. FDBArray).
	}
	
	protected static void rollbackLocalNode() {
		DebugLog.log("Inside rollbackLocalNode. TBD");
		if (epochEp < epochEa) {
			DebugLog.log("epochEp < epochEa: Need to rollback the local state to e_p");
			resetVersionMap(1, epochEa - 1);
			resetRockyStorageFromEBSS(1, epochEp);
			resetRockyStorageFromCBSS(epochEp + 1, epochEa - 1);
		} else { // epochEp >= epochEa
			DebugLog.log("epochEp >= epochEa: Need to rollback the local state to e_a - 1");
			epochEp = epochEa - 1;
			System.out.println("resetVersionMap is called");
			resetVersionMap(1, epochEp);
			System.out.println("resetRockyStorageFromEBSS is called");
			resetRockyStorageFromEBSS(1, epochEp);
		}
		//TODO: presence bitmap should be reset to 1 for all bits here
		
	}
	
	protected static void rollbackCloudNode() {
		DebugLog.log("Inside rollbackCloudNode. TBD");
		try {
			if (epochEp < epochEa) {
				cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(epochEa - 1));
			} else {
				cloudBlockSnapshotStore.put("EpochCount", ByteUtils.longToBytes(epochEp));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
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
				if (line.startsWith("ip")) {
					String[] tokens = line.split("=");
					String ipPortStr = tokens[1];
					String[] ipPortStrArr = ipPortStr.split(":");
					myIP = ipPortStrArr[0];
					myPort = ipPortStrArr[1];
					myID = myIP + ":" + myPort;					
				} else if (line.startsWith("coordinator")) {
					String[] tokens = line.split("=");
					coordinatorID = tokens[1];					
				} else if (line.startsWith("recovery_committee")) {
					String[] tokens = line.split("=");
					if (tokens.length == 1) {
						// coordinator is the only one participating in recovery
						hasAllCommitteeNodes = true; // this will trigger recovery procedure
						//System.out.println(
						//		"recoveryCommittee={} (coordinator is the only one participating in recovery)");
					} else {
						String committeeStr = tokens[1];
						String[] committeeNodesStr = committeeStr.split(",");
						for (int i = 0; i < committeeNodesStr.length; i++) {
							recoveryCommittee.add(committeeNodesStr[i]);
						}
						//System.out.println("recoveryCommittee=" + recoveryCommittee.toString());
					}
				} else if (line.startsWith("e_a")) {
					String[] tokens = line.split("=");
					String rollbackEpochStr = tokens[1];
					epochEa = Integer.parseInt(rollbackEpochStr);
				} else if (line.startsWith("cloud_failure")) {
					String[] tokens = line.split("=");
					hasCloudFailed = Boolean.parseBoolean(tokens[1]);
				} else if (line.startsWith("backendStorageType")) {
					String[] tokens = line.split("=");
					String backendStorageTypeStr = tokens[1];
					if (backendStorageTypeStr.equals("dynamoDBLocal")) {
						backendStorage = BackendStorageType.DynamoDBLocal;
					} else if (backendStorageTypeStr.equals("dynamoDBSeoul")) {
						backendStorage = BackendStorageType.DynamoDB_SEOUL;
					} else if (backendStorageTypeStr.equals("dynamoDBLondon")) {
						backendStorage = BackendStorageType.DynamoDB_LONDON;
					} else if (backendStorageTypeStr.equals("dynamoDBOhio")) {
						backendStorage = BackendStorageType.DynamoDB_OHIO;
					} else {
						backendStorage = BackendStorageType.Unknown;
					}
					if (backendStorage.equals(BackendStorageType.Unknown)) {
						System.err.println("Error: Cannot support Unknown backendStorageType");
						System.exit(1);
					}
				} else if (line.startsWith("exportName")) {
					String[] tokens = line.split("=");
					exportName = tokens[1];
				} else if (line.startsWith("rockyLocalStoragePath")) {
					String[] tokens = line.split("=");
					rockyLocalStoragePath = tokens[1];
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
	
	public long getEpoch() {
		long retLong = 0;
		byte[] epochBytes;
		try {
			epochBytes = cloudBlockSnapshotStore.get("EpochCount");
			if (epochBytes == null) {
				DebugLog.elog("There is no EpochCount in Cloud.");
				System.exit(1);
			} else {
				retLong = ByteUtils.bytesToLong(epochBytes);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return retLong;
	}
	
	private long getPrefetchedEpoch() {
		long retLong = 0;
		byte[] epochBytes;
		try {
			epochBytes = cloudBlockSnapshotStore.get("PrefetchedEpoch-" + myID);
			if (epochBytes == null) {
				System.out.println("PrefetchedEpoch-" + myID 
						+ " key is not allocated yet. Allocate it now");
				try {
					cloudBlockSnapshotStore.put("PrefetchedEpoch-" + myID
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

	public static void printStaticVariables() {
		DebugLog.log("myIP=" + myIP);
		DebugLog.log("myPort=" + myPort);
		DebugLog.log("myID=" + myID);
		DebugLog.log("coordinatorID=" + coordinatorID);
		DebugLog.log("debugPrintoutFlag=" + debugPrintoutFlag);
		DebugLog.log("epochEa=" + epochEa);
		DebugLog.log("epochEp=" + epochEp);
		DebugLog.log("hasAllCommitteeNodes=" + hasAllCommitteeNodes);
		DebugLog.log("hasCloudFailed=" + hasCloudFailed);
		DebugLog.log("noCloudFailureRecoveryFlag=" + noCloudFailureRecoveryFlag);
		DebugLog.log("handleNoCloudFailureRecoveryCount=" + handleNoCloudFailureRecoveryCount);
		DebugLog.log("recoveryCommittee=" + recoveryCommittee.toString());
		DebugLog.log("backendStorage=" + backendStorage);
		DebugLog.log("myInstanceNo=" + myInstanceNo);
		DebugLog.log("exportName=" + exportName);
	}
	
	public static void initialize() {
		// initialize required static variables
		myIP = "127.0.0.1";
		myPort = "17890";
		myID = myIP + ":" + myPort;
		coordinatorID = "127.0.0.1:17890";
		debugPrintoutFlag = false;
		epochEa = -1;
		epochEp = -1;
		hasAllCommitteeNodes = false;
		hasCloudFailed = false;
		noCloudFailureRecoveryFlag = false;
		handleNoCloudFailureRecoveryCount = 0;
		recoveryCommittee = new ArrayList<String>();
		backendStorage = BackendStorageType.DynamoDBLocal;
		myInstanceNo = 0;
		exportName = "testingRecovery";
		System.out.println("Initialization of variables is done.");	
	}
	
	public static void main(String[] args) {

		initialize();
		
		//update variables using config if given
		if (args.length < 2) {
			System.out.println("no config file is given.");
		} else if (args.length >= 2) {
			System.out.println("given config file=" + args[1]);
			parseRecoveryConfig(args[1]);
		}
		printStaticVariables();
		
		Coordinator server = new Coordinator(myID);
		// The following triggers recovery procedure on coordinator
		if (!hasCloudFailed) { //  when there is no cloud failure
			if (recoveryCommittee.size() == 0) { // when the coordinator is the only participant
				server.startNoCloudFailureRecoveryWorker();
				noCloudFailureRecoveryFlag = true;
			} else {
				server.runServer();
			}
		} else {
			// TODO: implement the setup for the three phase protocol
		}
	}

}
