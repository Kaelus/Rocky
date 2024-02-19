package rocky.communication;

public interface PeerCommunication {

	public enum PeerRequestType {OWNERSHIP_REQUEST, CLOUD_FAILURE_RECOVERY_PREP_REQUEST, 
		CLOUD_FAILURE_RECOVERY_IP_REQUEST, CLOUD_FAILURE_RECOVERY_IRP_REQUEST, 
		CLOUD_FAILURE_RECOVERY_FRP_REQUEST, UNKNOWN};
	
	public Message sendPeerRequest(String pID, PeerRequestType prt);
	public Object handlePeerRequest(byte[] req);
	
}
