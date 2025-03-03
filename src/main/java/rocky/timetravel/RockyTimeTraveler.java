package rocky.timetravel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import rocky.ctrl.RockyController;
import rocky.ctrl.RockyStorage;

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
			if (reqRoleTypeFrom == null) {								// assert 1
				// reqRoleTypeFrom is not recognizable.
				logger.severe("ASSERT: requested role (From) cannot be recognized. req.getRoleFrom()=" + req.getRoleFrom());
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack")
						.build();
			} else if (reqRoleTypeTo == null) {							// assert 2
				// reqRoleTypeTo is not recognizable.
				logger.severe("ASSERT: requested role (To) cannot be recognized. req.getRoleTo()=" + req.getRoleTo());
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack")
						.build();
			} else if (!RockyController.role.equals(reqRoleTypeFrom)) {	// assert 3
				logger.severe("ASSERT: requested role (From) is different from the current role of the Rocky!");
				reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack")
						.build();
			} else {													// Ok. Proceeds.
				invokeRoleSwitching(reqRoleTypeTo);
				if (!RockyController.role.equals(reqRoleTypeTo)) {			// assert 4
					logger.severe("ASSERT: requested role (To) is different from the new role of the Rocky!");
					reply = SwitchRoleReply.newBuilder().setAckMsg("SwitchRoleReply=" + "Nack")
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
			// Assert: Execute rewind only if the node is NonOwner 
			
			RewindReply reply = RewindReply.newBuilder().setAckMsg("RewindReply=" + "Ack")
					.setEpochNew(RockyStorage.epochCnt)
					.build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void replay(ReplayRequest req, StreamObserver<ReplayReply> responseObserver) {
			System.out.println("Entered replay");
			System.out.println("req msg for this RPC: epochFrom=" + req.getEpochFrom() + " epochTo=" + req.getEpochTo());
			// Assert: Execute rewind only if the node is NonOwner
			
			ReplayReply reply = ReplayReply.newBuilder().setAckMsg("ReplayReply=" + "Ack")
					.setEpochNew(RockyStorage.epochCnt)
					.build();
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
		
	}

}
