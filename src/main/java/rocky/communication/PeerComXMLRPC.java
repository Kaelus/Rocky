package rocky.communication;

import java.io.IOException;
import java.util.ArrayList;

import rocky.ctrl.RockyController;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.utils.DebugLog;
import rocky.ctrl.utils.ObjectSerializer;

public class PeerComXMLRPC implements PeerCommunication {

	public String loggerID;
	
	private ArrayList<SenderXMLRPC> peerSenderList;
	private ReceiverXMLRPC receiver;
	
	Thread roleSwitcherThread;
	RockyStorage rockyStorage;
	
	public static int myInstanceNo;
	
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
	public PeerComXMLRPC() {
		if (loggerID == null) {
			this.loggerID = RockyController.nodeID + (myInstanceNo++);
		}
		DebugLog.log("PeerComXMLRPC constructor each worker thread");
	}
	
	/**
	 * This constructor is to be used to start up the WebServer. This is meant to be
	 * called only once. Also, this should be the only constructor used by other
	 * than WebServer.
	 * 
	 * @param srvName
	 * @throws Exception
	 */
	public PeerComXMLRPC(String peerComXMLRPCName) {
		
		loggerID = peerComXMLRPCName + "-logger";
		
		// initialize sender side stuffs
		peerSenderList = new ArrayList<SenderXMLRPC>();
		String peerXMLRPCRecvIP = null;
		String peerXMLRPCRecvID = null;
		String peerXMLRPCRecvReqHandler = "PeerComXMLRPC.handlePeerRequest";
		SenderXMLRPC sender = null;
		
		for (String peerAddrStr : RockyController.peerAddressList) {
			peerXMLRPCRecvIP = "http://" + peerAddrStr + "/xmlrpc";
			peerXMLRPCRecvID = peerAddrStr;
			sender = new SenderXMLRPC(peerXMLRPCRecvIP, 
					peerXMLRPCRecvID, peerXMLRPCRecvReqHandler);
			sender.loggerID = this.loggerID + "-" + peerAddrStr;
		}
		
		// initialize receiver side stuffs		
		myInstanceNo = 0;
		runServer();
	}
	
	public void setRoleSwitcher(Thread rsThread) {
		roleSwitcherThread = rsThread;
	}
	
	public void setRockyStorage(RockyStorage rs) {
		rockyStorage = rs;
	}
	
	@Override
	public Message sendPeerRequest(String pID, PeerRequestType prt) {
		Message reqMsg = new Message();
		if (prt.equals(PeerRequestType.OWNERSHIP_REQUEST)) {
			reqMsg.msgType = MessageType.PEER_REQ_T_OWNERSHIP;
		} else {
			DebugLog.elog("Unknown PeerRequestType is given=" + prt.toString());
		}
		reqMsg.senderID = RockyController.nodeID;
		Message srvMsg = null;
		SenderXMLRPC sender = null;
		Message ackMsg = null;
		for (SenderXMLRPC s : peerSenderList) {
			if (s.serverName.equals(pID)) {
				sender = s;
				break;
			}
		}
		
		try {
			srvMsg = sender.send(reqMsg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (srvMsg.msgType == MessageType.MSG_T_ACK) {
			DebugLog.log("server responds with ACK.", this.loggerID);
			ackMsg = (Message) srvMsg;
		} else {
			// Server NACK...
			// For now, we just warn.
			DebugLog.elog("WARN: Server NACK. For now, we just warn.", this.loggerID);
		}	

		return ackMsg;
	}
	
	@Override
	public Object handlePeerRequest(byte[] req) {
		Object retObj = null;
		Message pMsg = null;
		Message ackMsg = null;
		
		try {
			pMsg = (Message) ObjectSerializer.deserialize(req);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DebugLog.log(pMsg.toString());
		DebugLog.log("handlePeerRequest. Thread ID: " + Thread.currentThread().getId());
		DebugLog.log("received a request in handlePeerRequest", this.loggerID);

		switch (pMsg.msgType) {
		case MessageType.PEER_REQ_T_OWNERSHIP:
			retObj = handleOwnershipRequest(pMsg);
			ackMsg = new Message();
			ackMsg.msgType = MessageType.MSG_T_ACK;
			ackMsg.msgContent = retObj;
			break;
		default:
			break;
		}
		
		try {
			retObj = ObjectSerializer.serialize(ackMsg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DebugLog.log("Server is responding to a client request", this.loggerID);
		
		return retObj;
	}
	
	public void runServer() {
		receiver = new ReceiverXMLRPC(Integer.valueOf(RockyController.myPort));
	}

	public void stopServer() {
		receiver.webServer.shutdown();
	}

	public Object handleOwnershipRequest(Message pMsg) {
		Object retObj = null;
		
		RockyController.RockyControllerRoleType newRole = RockyController.RockyControllerRoleType.Owner;
		RockyController.RockyControllerRoleType prevRole = null;
		if (roleSwitcherThread == null) {
			DebugLog.elog("ASSERT: rsThread is not initialized yet.");
			System.exit(1);
		}
		synchronized(roleSwitcherThread) {
			prevRole = RockyController.role;
		}
		boolean fromNoneOwnerToOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner);
		if (!fromNoneOwnerToOwner) {
			System.err.println("ASSERT: unallowed role switching scenario");
			System.err.println("From=" + prevRole.toString() + " To=" + newRole.toString());
			System.err.println("We will ignore the role switching request");
		} else {
			//synchronized(RockyController.role) {
			synchronized(roleSwitcherThread) {
				if (rockyStorage == null) {
					System.err.println("ASSERT: rockyStorage is null.");
					System.exit(1);					
				}
				rockyStorage.stopWorkload();
				rockyStorage.renounceOwnership();
				RockyController.role = newRole;
				roleSwitcherThread.notify();
			}	
		}
		
		return retObj;
	}
	
}
