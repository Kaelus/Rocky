package rocky.communication;

import java.io.IOException;
import java.util.ArrayList;

import rocky.ctrl.RockyController;
import rocky.ctrl.RockyStorage;
import rocky.ctrl.utils.DebugLog;
import rocky.ctrl.utils.ObjectSerializer;
import rocky.recovery.RecoveryController;

public class PeerComXMLRPC implements PeerCommunication {

	public String loggerID;
	
	private ArrayList<SenderXMLRPC> peerSenderList;
	private ReceiverXMLRPC receiver;
	
	public static Thread roleSwitcherThread;
	public static RockyStorage rockyStorage;
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
			peerSenderList.add(sender);
			DebugLog.log("Inside constructor, add a sender to peerSenderList: " + peerAddrStr);
		}
		DebugLog.log("Added all peer senders now.");
		
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
		} else if (prt.equals(PeerRequestType.CLOUD_FAILURE_RECOVERY_PREP_REQUEST)) {
			reqMsg.msgType = MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_PREP;
		} else if (prt.equals(PeerRequestType.CLOUD_FAILURE_RECOVERY_IP_REQUEST)) {
			reqMsg.msgType = MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_IP;
			String e1e2 = RecoveryController.latestOwnerEpoch + ";" + RecoveryController.latestPrefetchEpoch;
			reqMsg.msgContent = e1e2;
		} else if (prt.equals(PeerRequestType.CLOUD_FAILURE_RECOVERY_IRP_REQUEST)) {
			reqMsg.msgType = MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_IRP;
		} else if (prt.equals(PeerRequestType.CLOUD_FAILURE_RECOVERY_FRP_REQUEST)) {
			reqMsg.msgType = MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_FRP;
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
		if (sender == null) {
			DebugLog.elog("ASSERT: could not find proper sender object for ID=" + pID);
			System.exit(1);
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
			if (retObj != null) {
				ackMsg.msgType = MessageType.MSG_T_ACK;
			} else {
				ackMsg.msgType = MessageType.MSG_T_NACK;
			}
			ackMsg.msgContent = retObj;
			break;
		case MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_PREP:
			retObj = handleDeadCloudRecoveryPrep(pMsg);
			ackMsg = new Message();
			if (retObj != null) {
				ackMsg.msgType = MessageType.MSG_T_ACK;
			} else {
				ackMsg.msgType = MessageType.MSG_T_NACK;
			}
			ackMsg.msgContent = retObj;
			break;
		case MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_IP:
			retObj = handleDeadCloudRecoveryIP(pMsg);
			ackMsg = new Message();
			if (retObj != null) {
				ackMsg.msgType = MessageType.MSG_T_ACK;
			} else {
				ackMsg.msgType = MessageType.MSG_T_NACK;
			}
			ackMsg.msgContent = retObj;
			break;
		case MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_IRP:
			retObj = handleDeadCloudRecoveryIRP(pMsg);
			ackMsg = new Message();
			if (retObj != null) {
				ackMsg.msgType = MessageType.MSG_T_ACK;
			} else {
				ackMsg.msgType = MessageType.MSG_T_NACK;
			}
			ackMsg.msgContent = retObj;
			break;
		case MessageType.PEER_REQ_T_CLOUD_FAILURE_RECOVERY_FRP:
			retObj = handleDeadCloudRecoveryFRP(pMsg);
			ackMsg = new Message();
			if (retObj != null) {
				ackMsg.msgType = MessageType.MSG_T_ACK;
			} else {
				ackMsg.msgType = MessageType.MSG_T_NACK;
			}
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
		receiver = new ReceiverXMLRPC(Integer.valueOf(RockyController.pComPort));
	}

	public void stopServer() {
		receiver.webServer.shutdown();
	}

	public Object handleOwnershipRequest(Message pMsg) {
		Object retObj = null;
		
		RockyController.RockyControllerRoleType newRole = RockyController.RockyControllerRoleType.NonOwner;
		RockyController.RockyControllerRoleType prevRole = null;
		if (roleSwitcherThread == null) {
			DebugLog.elog("ASSERT: roleSwitcherThread is not initialized yet.");
			System.exit(1);
		}
		synchronized(roleSwitcherThread) {
			prevRole = RockyController.role;
		}
		boolean fromOwnerToNonOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.Owner)
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
		if (!fromOwnerToNonOwner) {
			System.err.println("ASSERT: unallowed role switching scenario for ownership request handling");
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
			retObj = "Succeed.";
		}
		
		System.out.println("current owner recorded on cloud is=" + rockyStorage.getOwner());
		
		return retObj;
	}
	
	public Object handleDeadCloudRecoveryPrep(Message pMsg) {
		Object retObj = null;
		String nonCoordinatorID = (String) pMsg.senderID;
		if (RockyController.peerAddressList.contains(nonCoordinatorID)) {
			synchronized(RecoveryController.nonCoordinatorWaitingList) {
				RecoveryController.nonCoordinatorWaitingList.add(nonCoordinatorID);
				RecoveryController.nonCoordinatorWaitingList.notify();
			}
			synchronized(RecoveryController.canSendResponse) {
				try {
					RecoveryController.canSendResponse.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			retObj = RecoveryController.hasCloudFailed + ";" + RecoveryController.epochEa;
		}
		return retObj;
	}
	
	public Object handleDeadCloudRecoveryIP(Message pMsg) {
		Object retObj = null;
		String nonCoordinatorID = (String) pMsg.senderID;
		String e1e2 = (String) pMsg.msgContent;
		Long ownerEpoch = Long.parseLong(e1e2.split(";")[0]);
		Long prefetchEpoch = Long.parseLong(e1e2.split(";")[1]);
		DebugLog.log("nonCoordinatorID=" + nonCoordinatorID + " ownerEpoch=" + ownerEpoch + " prefetchEpoch=" + prefetchEpoch);
		synchronized(RecoveryController.epochLeader) {
			if (ownerEpoch > RecoveryController.latestOwnerEpoch) {
				DebugLog.log("Updating latestOwnerEpoch and epochLeader:");
				DebugLog.log("Previously, latestOwnerEpoch=" + RecoveryController.latestOwnerEpoch + " epochLeader=" + RecoveryController.epochLeader);
				RecoveryController.latestOwnerEpoch = ownerEpoch;
				RecoveryController.epochLeader = nonCoordinatorID;
				DebugLog.log("After updating, latestOwnerEpoch=" + RecoveryController.latestOwnerEpoch + " epochLeader=" + RecoveryController.epochLeader);
			}
		}
		synchronized(RecoveryController.prefetchLeader) {
			if (prefetchEpoch > RecoveryController.latestPrefetchEpoch) {
				DebugLog.log("Updating latestPrefetchEpoch and prefetchLeader:");
				DebugLog.log("Previously, latestPrefetchEpoch=" + RecoveryController.latestPrefetchEpoch + " prefetchLeader=" + RecoveryController.prefetchLeader);
				RecoveryController.latestPrefetchEpoch = prefetchEpoch;
				RecoveryController.prefetchLeader = nonCoordinatorID;
				DebugLog.log("After updating, latestPrefetchEpoch=" + RecoveryController.latestPrefetchEpoch + " prefetchLeader=" + RecoveryController.prefetchLeader);

			}
		}
		if (RockyController.peerAddressList.contains(nonCoordinatorID)) {
			synchronized(RecoveryController.nonCoordinatorWaitingList) {
				RecoveryController.nonCoordinatorWaitingList.add(nonCoordinatorID);
				RecoveryController.nonCoordinatorWaitingList.notify();
			}
			synchronized(RecoveryController.canSendResponse) {
				try {
					RecoveryController.canSendResponse.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			retObj = RecoveryController.latestOwnerEpoch + ";" + RecoveryController.epochLeader + ";"
					+ RecoveryController.latestPrefetchEpoch + ";" + RecoveryController.prefetchLeader;
		}

		return retObj;
	}
	
	public Object handleDeadCloudRecoveryIRP(Message pMsg) {
		Object retObj = null;
		
		String nonCoordinatorID = (String) pMsg.senderID;
		if (RockyController.peerAddressList.contains(nonCoordinatorID)) {
			synchronized(RecoveryController.nonCoordinatorWaitingList) {
				RecoveryController.nonCoordinatorWaitingList.add(nonCoordinatorID);
				RecoveryController.nonCoordinatorWaitingList.notify();
			}
			synchronized(RecoveryController.canSendResponse) {
				try {
					RecoveryController.canSendResponse.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			retObj = RecoveryController.contiguousEpochListL;
		}
		
		return retObj;
	}
	
	public Object handleDeadCloudRecoveryFRP(Message pMsg) {
		Object retObj = null;
		return retObj;
	}
	
}
