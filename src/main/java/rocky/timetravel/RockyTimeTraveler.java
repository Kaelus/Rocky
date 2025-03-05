package rocky.timetravel;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import rocky.ctrl.RockyController;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.ValueStorageLevelDB;
import rocky.ctrl.utils.ByteUtils;
import rocky.recovery.RecoveryController;

public class RockyTimeTraveler {

	static Thread roleSwitcherThread;

	public static RockyStorage rockyStorage;

	public RockyTimeTraveler(Thread rsThread) {
		roleSwitcherThread = rsThread;
	}

	private static final Logger logger = Logger.getLogger(RockyTimeTraveler.class.getName());

	private Server server;

	public void start() throws IOException {
		/* The port on which the server should run */
		int port = 50051;
		server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()).addService(new TimeTravelerImpl())
				.build().start();
		logger.info("RockyTimeTraveler Server started, listening on " + port);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// Use stderr here since the logger may have been reset by its JVM shutdown
				// hook.
				System.err.println("*** shutting down gRPC server since JVM is shutting down");
				try {
					RockyTimeTraveler.this.stop();
				} catch (InterruptedException e) {
					e.printStackTrace(System.err);
				}
				System.err.println("*** server shut down");
			}
		});
	}

	public void stop() throws InterruptedException {
		logger.info("Stopping RockyTimeTraveler Server");
		if (server != null) {
			server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
		}
		logger.info("RockyTimeTraveler Server stopped");
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	public void blockUntilShutdown() throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

	static class TimeTravelerImpl extends TimeTravelerGrpc.TimeTravelerImplBase {

		@Override
		public void getStatus(GetStatusRequest req, StreamObserver<GetStatusReply> responseObserver) {
			System.out.println("Entered getStatus");
			System.out.println("req msg has no field for this RPC");
			GetStatusReply reply = GetStatusReply.newBuilder().setAckMsg("GetStatusReply=" + "Ack")
					.setRole(RockyController.role.ordinal())
					.setEpoch(RockyStorage.epochCnt)
					.build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void switchRole(SwitchRoleRequest req, StreamObserver<SwitchRoleReply> responseObserver) {
			System.out.println("Entered switchRole");
			System.out.println("req msg for this RPC: roleFrom=" + req.getRoleFrom() + " roleTo=" + req.getRoleTo());
			System.out.println("current role is=" + RockyController.role);

			// convert provided fields into proper enum RockyController.RockyControllerRoleType values
			RockyController.RockyControllerRoleType reqRoleTypeFrom = null;
			switch(req.getRoleFrom()) {
			case 0:
				reqRoleTypeFrom = RockyController.RockyControllerRoleType.Owner;
				break;
			case 1:
				reqRoleTypeFrom = RockyController.RockyControllerRoleType.NonOwner;
				break;
			case 2:
				reqRoleTypeFrom = RockyController.RockyControllerRoleType.None;
				break;
			default:
				break;
			}
			RockyController.RockyControllerRoleType reqRoleTypeTo = null;
			switch(req.getRoleTo()) {
			case 0:
				reqRoleTypeTo = RockyController.RockyControllerRoleType.Owner;
				break;
			case 1:
				reqRoleTypeTo = RockyController.RockyControllerRoleType.NonOwner;
				break;
			case 2:
				reqRoleTypeTo = RockyController.RockyControllerRoleType.None;
				break;
			default:
				break;
			}

			SwitchRoleReply reply = null;
			String nackMsg = null;
			if (reqRoleTypeFrom == null) {								// assert 1
				// reqRoleTypeFrom is not recognizable.
				nackMsg = "ASSERT: requested role (From) cannot be recognized. req.getRoleFrom()=" + req.getRoleFrom();
				logger.severe(nackMsg);
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack:" + nackMsg)
						.build();
			} else if (reqRoleTypeTo == null) {							// assert 2
				// reqRoleTypeTo is not recognizable.
				nackMsg = "ASSERT: requested role (To) cannot be recognized. req.getRoleTo()=" + req.getRoleTo();
				logger.severe(nackMsg);
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack:" + nackMsg)
						.build();
			} else if (!RockyController.role.equals(reqRoleTypeFrom)) {	// assert 3
				nackMsg = "ASSERT: requested role (From) is different from the current role of the Rocky!";
				logger.severe(nackMsg);
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack:" + nackMsg)
						.build();
			} else {													// Ok. Proceeds.
				invokeRoleSwitching(reqRoleTypeTo);
				if (!RockyController.role.equals(reqRoleTypeTo)) {			// assert 4
					nackMsg = "ASSERT: requested role (To) is different from the new role of the Rocky!";
					logger.severe(nackMsg);
					reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack:" + nackMsg)
							.build();
					invokeRoleSwitching(reqRoleTypeFrom); // rollback the role switch
				} else {
					reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Ack")
							.setRoleNew(RockyController.role.ordinal())
							.build();
				}
			}
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void rewind(RewindRequest req, StreamObserver<RewindReply> responseObserver) {
			System.out.println("Entered rewind");
			System.out.println("req msg for this RPC: epochFrom=" + req.getEpochFrom() + " epochTo=" + req.getEpochTo());

			RewindReply reply = null;
			String nackMsg = null;
					
			// ASSERT: Execute rewind only if the node is NonOwner
			if (!RockyController.role.equals(RockyController.RockyControllerRoleType.NonOwner)) {
				nackMsg = "ASSERT: Execute rewind only if the node is NonOwner";
				System.err.println(nackMsg);
				reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();
			} else if (req.getEpochFrom() <= req.getEpochTo()) { // ASSERT: epochFrom > epochTo should be true
				nackMsg = "ASSERT: epochFrom > epochTo should be true";
				System.err.println(nackMsg);
				reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();			
			} else if (req.getEpochFrom() != RockyStorage.epochCnt) { // ASSERT: epochFrom should be equal to current epoch
				nackMsg = "ASSERT: epochFrom should be equal to current epoch";
				System.err.println(nackMsg);
				reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();			
			} else if (req.getEpochTo() < 1) { // ASSERT: epochTo should be equal to or greater than 1 
				nackMsg = "ASSERT: epochTo should be equal to or greater than 1";
				System.err.println(nackMsg);
				reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();			
			} else {
			
				long epochTo = req.getEpochTo();
				
				// initialize RecoveryController to reset states below
				RecoveryController.epochEa = -1;
				RecoveryController.hasCloudFailed = false;
				RecoveryController.localBlockResetBitmap = new BitSet(RockyStorage.presenceBitmap.length());
				try {
					RecoveryController.localBlockResetEpochAndBlockIDPairStore = new ValueStorageLevelDB(RockyStorage.prefixPathForLocalStorage + "-localBlockResetEpochAndBlockIDPairStoreTable");
					RecoveryController.localBlockResetEpochAndBlockIDPairStore.clean(); // clean the effect from the previous run 
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// reset VersionMap state back to epochTo
				RecoveryController.recoverVersionMap(1, epochTo);
								
				// reset RockyStorage state back to epochTo
				RecoveryController.recoverRockyStorage(1, epochTo);
				
				// presence bitmap should be reset to 1 for all bits
				RockyStorage.presenceBitmap.set(0, RockyStorage.numBlock);
				
				// reset epochCnt properly
				RockyStorage.epochCnt = epochTo;
				
				reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Ack")
						.setEpochNew(RockyStorage.epochCnt)
						.build();
			}
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void replay(ReplayRequest req, StreamObserver<ReplayReply> responseObserver) {
			System.out.println("Entered replay");
			System.out.println("req msg for this RPC: epochFrom=" + req.getEpochFrom() + " epochTo=" + req.getEpochTo());

			ReplayReply reply = null;
			String nackMsg = null;
			long latestEpochOnCloud = rockyStorage.getEpoch();
			
			// Assert: Execute replay only if the node is NonOwner
			if (!RockyController.role.equals(RockyController.RockyControllerRoleType.NonOwner)) {
				System.err.println("ASSERT: Execute replay only if the node is NonOwner");
				reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Nack")
						.setEpochNew(RockyStorage.epochCnt)
						.build();
				
			} else if (req.getEpochFrom() >= req.getEpochTo()) { // ASSERT: epochFrom < epochTo should be true
				nackMsg = "ASSERT: epochFrom < epochTo should be true";
				System.err.println(nackMsg);
				reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();			
			} else if (req.getEpochFrom() != RockyStorage.epochCnt) { // ASSERT: epochFrom should be equal to current epoch
				nackMsg = "ASSERT: epochFrom should be equal to current epoch";
				System.err.println(nackMsg);
				reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();
			} else if (req.getEpochTo() > latestEpochOnCloud) { // ASSERT: epochTo should not exceed latest epoch on the cloud
				nackMsg = "ASSERT: epochTo should not exceed latest epoch on the cloud";
				System.err.println(nackMsg);
				reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Nack:" + nackMsg)
						.setEpochNew(RockyStorage.epochCnt)
						.build();
				
			} else {
				
				long epochFrom = req.getEpochFrom();
				long epochTo = req.getEpochTo();
				
				// incrementally replay up to epochTo
				incReplay(epochFrom, epochTo);
				
				// advance epochCnt properly
				RockyStorage.epochCnt = epochTo;
				
				reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Ack")
						.setEpochNew(RockyStorage.epochCnt)
						.build();
			}
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}

		private void invokeRoleSwitching(RockyController.RockyControllerRoleType newRole) {
			
			// Check assertion
			RockyController.RockyControllerRoleType prevRole = null;
			synchronized(roleSwitcherThread) {
				prevRole = RockyController.role;
			}
			boolean fromNoneToOwner = 
					prevRole.equals(RockyController.RockyControllerRoleType.None)
					&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
			boolean fromNonOwnerToOwner = 
					prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
					&& newRole.equals(RockyController.RockyControllerRoleType.Owner);
			boolean fromOwnerToNonOwner =
					prevRole.equals(RockyController.RockyControllerRoleType.Owner) 
					&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
			boolean fromNonOwnerToNone = 
					prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
					&& newRole.equals(RockyController.RockyControllerRoleType.None);
			if (!(fromNoneToOwner || fromNonOwnerToOwner 
					|| fromOwnerToNonOwner || fromNonOwnerToNone)) {
				System.err.println("ASSERT: unallowed role switching scenario");
				System.err.println("From=" + prevRole.toString() + " To=" + newRole.toString());
				System.err.println("We will ignore the role switching request");
			} else {
				//synchronized(RockyController.role) {
				synchronized(roleSwitcherThread) {
					if (fromNonOwnerToOwner) {
						rockyStorage.takeOwnership();
					} else if (fromOwnerToNonOwner) {
						rockyStorage.renounceOwnership();
					}
					RockyController.role = newRole;
					roleSwitcherThread.notify();
				}	
			}
			System.out.println("current owner recorded on cloud is=" + rockyStorage.getOwner());
		}
		
		private void incReplay(long epochFrom, long epochTo) {
			// Get all epoch bitmaps
			List<BitSet> epochBitmapList = rockyStorage.fetchNextEpochBitmaps(epochTo, epochFrom);
			
			// Get a list of blockIDs to prefetch
			HashSet<Integer> blockIDList = rockyStorage.getPrefetchBlockIDList(epochBitmapList);
			
			// Logically, we have to reset bits to 0 for blocks that are updated/dirtied.
			// Yet, we will (1) fetch all dirty blocks below; (2) set bits for all fetched blocks
			// Thus, we skip resetting bits to 0 that are going to be set to 1 again right below.
			
			if (blockIDList != null) { // if blockIDList is null, we don't need to prefetch anything
				// Prefetch loop
				rockyStorage.prefetchBlocks(rockyStorage, blockIDList);
			}
		}
		
	}

}
