package rocky.timetravel;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import rocky.ctrl.RockyStorage;

public class RockyTimeTraveler {

	Thread roleSwitcherThread;

	public RockyStorage rockyStorage;

	public RockyTimeTraveler(Thread rsThread) {
		roleSwitcherThread = rsThread;
	}

	private static final Logger logger = Logger.getLogger(RockyTimeTraveler.class.getName());

	private Server server;

	private void start() throws IOException {
		/* The port on which the server should run */
		int port = 50051;
		server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create()).addService(new TimeTravelerImpl())
				.build().start();
		logger.info("Server started, listening on " + port);
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

	private void stop() throws InterruptedException {
		if (server != null) {
			server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
		}
	}

	/**
	 * Await termination on the main thread since the grpc library uses daemon
	 * threads.
	 */
	private void blockUntilShutdown() throws InterruptedException {
		if (server != null) {
			server.awaitTermination();
		}
	}

	static class TimeTravelerImpl extends TimeTravelerGrpc.TimeTravelerImplBase {

		@Override
		public void getStatus(GetStatusRequest req, StreamObserver<GetStatusReply> responseObserver) {
			System.out.println("Entered sayHello");
			GetStatusReply reply = GetStatusReply.newBuilder().setMessage("Hello " + req.getName()).build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void switchRole(SwitchRoleRequest req, StreamObserver<SwitchRoleReply> responseObserver) {
			System.out.println("Entered sayHello");
			SwitchRoleReply reply = SwitchRoleReply.newBuilder().setMessage("Hello " + req.getName()).build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void rewind(RewindRequest req, StreamObserver<RewindReply> responseObserver) {
			System.out.println("Entered sayHello");
			RewindReply reply = RewindReply.newBuilder().setMessage("Hello " + req.getName()).build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}
		
		@Override
		public void replay(ReplayRequest req, StreamObserver<ReplayReply> responseObserver) {
			System.out.println("Entered sayHello");
			ReplayReply reply = ReplayReply.newBuilder().setMessage("Hello " + req.getName()).build();
			responseObserver.onNext(reply);
			responseObserver.onCompleted();
		}

	}

}
