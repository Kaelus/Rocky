package rocky.communication;

public interface PeerCommunication {

	public enum PeerRequestType {OWNERSHIP_REQUEST, UNKNOWN};
	
	public Message sendPeerRequest(String pID, PeerRequestType prt);
	public Object handlePeerRequest(byte[] req);
	
}
