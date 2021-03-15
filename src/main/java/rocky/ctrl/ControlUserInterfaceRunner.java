package rocky.ctrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;

import rocky.ctrl.RockyStorage.CloudFlusher;
import rocky.ctrl.RockyStorage.Prefetcher;
import rocky.ctrl.cloud.ValueStorageDynamoDB;

public class ControlUserInterfaceRunner implements Runnable {

	String loggerID = "ControlUserInterface";
	boolean quitFlag = false;
	
	// command constants
	private final int CMD_QUIT = -1;
	private final int CMD_ROLE_SWITCH = 2;
	private final int CMD_CLEAN = 3;
	private final int CMD_PERF_EVAL = 4;
	private final int CMD_FLUSH_CLOUD = 5;
	private final int CMD_PREFETCH = 6;
	private final int CMD_RESET_EPOCH = 7;
	private final int CMD_MS_STAT = 8;
	
	Thread roleSwitcherThread;
	
	public RockyStorage rockyStorage;
	
	public ControlUserInterfaceRunner (Thread rsThread) {
		roleSwitcherThread = rsThread;
	}
	
	protected void cmdMutationSnapStat() {
		System.out.println("Mutation Snapshot Stat..");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				System.in));
		System.out.println("[" + loggerID + "] What do you want?\n"
				+ "[1] Print stats\n"
				+ "[2] Reset stats\n");
		String input = null;
		try {
			input = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		switch(Integer.parseInt(input)) {
		case 1:
			System.out.println("Number of requested block writes=" + RockyStorage.numBlockWrites);
			System.out.println("Number of blocks for a Mutation Snapshot=" + rockyStorage.writeMap.size());
			System.out.println("Number of past epochs prefetched=" + RockyStorage.numPastEpochsPrefetched);
			System.out.println("Number of dirty blocks for past epochs=" + RockyStorage.numBlockWrittenForPastEpochs);
			System.out.println("Number of blocks in merged snapshot=" + RockyStorage.numBlocksMergedSnapshot);
			break;
		case 2:
			RockyStorage.numBlockWrites = 0;
			RockyStorage.numPastEpochsPrefetched = 0;
			RockyStorage.numBlockWrittenForPastEpochs = 0;
			RockyStorage.numBlocksMergedSnapshot = 0;
			break;
		default:
			break;
		}
	}
	
	protected void cmdResetEpoch() {
		RockyStorage.epochCnt = 0;
		RockyStorage.prefetchedEpoch = 0;
	}
	
	protected void cmdPrefetch() {
		try {
			rockyStorage.prefetch();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void cmdFlushCloud() {
		try {
			RockyStorage.flusherTimer.cancel();
		} catch (IllegalStateException ise) {
			System.out.println("flusherTimer is cancled already.");
		}
		RockyStorage.flusherTimer = new Timer();
		RockyStorage.nextFlusherTask = rockyStorage.new CloudFlusher();
		RockyStorage.flusherTimer.schedule(RockyStorage.nextFlusherTask, 1);
		System.out.println("cmdFlushCloud rescheduled flusher task to begin in 1 ms");
		
		//RockyStorage.nextFlusherTask = new TimerTask();
		//RockyStorage.flusherTimer.schedule(RockyStorage.nextFlusherTask, 3);
		//System.out.println("cmdFlushCloud rescheduled flusher task to begin in 3 ms");
	}
	
	protected void cmdPerfEval() {
		System.out.println("Performance evaluation..");
		BufferedReader br = new BufferedReader(new InputStreamReader(
				System.in));
		System.out.println("[" + loggerID + "] Which setting "
				+ "do you want to evaluate?\n"
				+ "[1] Full Local\n"
				+ "[2] Half Local\n"
				+ "[3] Full Remote\n");
		String input = null;
		try {
			input = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		invokeSetupPerfEval(input);
	}
	
	public void invokeSetupPerfEval(String input) {
		switch(Integer.parseInt(input)) {
		case 1:
			RockyStorage.presenceBitmap.set(0, RockyStorage.numBlock);
			break;
		case 2:
			for (int i = 0; i < RockyStorage.numBlock; i++) {
				if (i % 2 == 0) {
					RockyStorage.presenceBitmap.set(i);
				} else {
					RockyStorage.presenceBitmap.clear(i);
				}
			}
			break;
		case 3:
			RockyStorage.presenceBitmap.clear();
			break;
		default:
			break;
		}
	}
	
	protected void cmdClean() {
//		GenericKeyValueStore cloudEpochBitmaps = null;
//		GenericKeyValueStore localEpochBitmaps = null;
//		GenericKeyValueStore cloudBlockSnapshotStore = null;
//		GenericKeyValueStore versionMap = null;
//		GenericKeyValueStore localBlockSnapshotStore = null;
		
		RockyStorage.cloudEpochBitmaps.clean();
		RockyStorage.cloudBlockSnapshotStore.remove("EpochCount");
		RockyStorage.cloudBlockSnapshotStore.clean();
		RockyStorage.localEpochBitmaps.clean();
		RockyStorage.localBlockSnapshotStore.clean();
		RockyStorage.versionMap.clean();
		try {
			RockyStorage.cloudEpochBitmaps.finish();
			RockyStorage.cloudBlockSnapshotStore.finish();
			RockyStorage.localEpochBitmaps.finish();
			RockyStorage.localBlockSnapshotStore.finish();
			RockyStorage.versionMap.finish();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		String cloudEpochBitmapsTableName = "cloudEpochBitmapsTable";
//		String localEpochBitmapsTableName = "localEpochBitmapsTable";
//		String cloudBlockSnapshotStoreTableName = "cloudBlockSnapshotStoreTable";
//		String versionMapTableName = "versionMapTable";
//		String localBlockSnapshotStoreTableName = "localBlockSnapshotStoreTable";
//		
		if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDBLocal)) {
			RockyStorage.cloudEpochBitmaps = new ValueStorageDynamoDB(RockyStorage.cloudEpochBitmapsTableName, true);
			RockyStorage.cloudBlockSnapshotStore = new ValueStorageDynamoDB(RockyStorage.cloudBlockSnapshotStoreTableName, true);
		} else if (RockyController.backendStorage.equals(RockyController.BackendStorageType.DynamoDB)) {
			RockyStorage.cloudEpochBitmaps = new ValueStorageDynamoDB(RockyStorage.cloudEpochBitmapsTableName, false);
			RockyStorage.cloudBlockSnapshotStore = new ValueStorageDynamoDB(RockyStorage.cloudBlockSnapshotStoreTableName, false);
		}
		try {
			RockyStorage.localEpochBitmaps = new ValueStorageLevelDB(RockyStorage.localEpochBitmapsTableName);
			RockyStorage.localBlockSnapshotStore = new ValueStorageLevelDB(RockyStorage.localBlockSnapshotStoreTableName);
			RockyStorage.versionMap = new ValueStorageLevelDB(RockyStorage.versionMapTableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	protected void cmdRoleSwitch() {
		RockyController.RockyControllerRoleType curRole = null;
		synchronized(RockyController.role) {
			curRole = RockyController.role;
		}
		System.out.println("role switching.. current role=" + curRole);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				System.in));
		System.out.println("[" + loggerID + "] To which role, "
				+ "do you want to switch to?\n"
				+ "[1] None\n"
				+ "[2] NonOwner\n"
				+ "[3] Owner\n");
		String input = null;
		try {
			input = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		invokeRoleSwitching(input);
	}
	
	public void invokeRoleSwitching(String input) {
		RockyController.RockyControllerRoleType newRole = null;
		switch(Integer.parseInt(input)) {
		case 1:
			newRole = RockyController.RockyControllerRoleType.None;
			break;
		case 2:
			newRole = RockyController.RockyControllerRoleType.NonOwner;
			break;
		case 3:
			newRole = RockyController.RockyControllerRoleType.Owner;
			break;
		default:
			break;
		}
		
		// Check assertion
		RockyController.RockyControllerRoleType prevRole = null;
		synchronized(roleSwitcherThread) {
			prevRole = RockyController.role;
		}
		boolean fromNoneToOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.None)
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
		boolean fromNoneOwnerToOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner);
		boolean fromOwnerToNoneOwner =
				prevRole.equals(RockyController.RockyControllerRoleType.Owner) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
		boolean fromNoneOwnerToNone = 
				prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.None);
		if (!(fromNoneToOwner || fromNoneOwnerToOwner 
				|| fromOwnerToNoneOwner || fromNoneOwnerToNone)) {
			System.err.println("ASSERT: unallowed role switching scenario");
			System.err.println("From=" + prevRole.toString() + " To=" + newRole.toString());
			System.err.println("We will ignore the role switching request");
		} else {
			//synchronized(RockyController.role) {
			synchronized(roleSwitcherThread) {
				RockyController.role = newRole;
				roleSwitcherThread.notify();
			}	
		}
	}
	
	@Override
	public void run() {
		try {
			System.out.println("[RoleSwitcher] entered run");
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			String input;
			System.out
					.println("[" + loggerID + "] What do you want to do? "
							+ "[" + CMD_QUIT + "] quit"
							+ "[" + CMD_ROLE_SWITCH + "] role switch "
							+ "[" + CMD_CLEAN + "] clean persistent state in dbs "
							+ "[" + CMD_PERF_EVAL + "] performance evaluation "
							+ "[" + CMD_FLUSH_CLOUD + "] flush to cloud "
							+ "[" + CMD_PREFETCH + "] prefetch "
							+ "[" + CMD_RESET_EPOCH + "] Set epoch counts "
							+ "[" + CMD_MS_STAT + "] Get Mutation Snapshot Stats");
			while (!quitFlag && ((input = br.readLine()) != null)) {
				try {
					int cmd = Integer.valueOf(input);
					System.out.println("[" + loggerID + "] cmd=" + cmd); 
					switch (cmd) {
					case CMD_QUIT:
						quitFlag = true;
						break;
					case CMD_ROLE_SWITCH:
						cmdRoleSwitch();
						break;
					case CMD_CLEAN:
						cmdClean();
						break;
					case CMD_PERF_EVAL:
						cmdPerfEval();
						break;
					case CMD_FLUSH_CLOUD:
						cmdFlushCloud();
						break;
					case CMD_PREFETCH:
						cmdPrefetch();
						break;
					case CMD_RESET_EPOCH:
						cmdResetEpoch();
						break;
					case CMD_MS_STAT:
						cmdMutationSnapStat();
						break;
					default:
						break;
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
				if (!quitFlag) {
					System.out
					.println("[" + loggerID + "] What do you want to do? "
							+ "[" + CMD_QUIT + "] quit"
							+ "[" + CMD_ROLE_SWITCH + "] role switch "
							+ "[" + CMD_CLEAN + "] clean persistent state in dbs "
							+ "[" + CMD_PERF_EVAL + "] performance evaluation "
							+ "[" + CMD_FLUSH_CLOUD + "] flush to cloud "
							+ "[" + CMD_PREFETCH + "] prefetch "
							+ "[" + CMD_RESET_EPOCH + "] Set epoch counts "
							+ "[" + CMD_MS_STAT + "] Get Mutation Snapshot Stats");
				}
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			System.err.println("[" + loggerID + "] JavaClient: " + exception);
		}

		System.out.println("[" + loggerID + "] The main function is Done now...");
		System.out.println("[" + loggerID + "] Goodbye!!!");

	}

	
}
