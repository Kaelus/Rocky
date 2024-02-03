package rocky.communication;

public class Message  implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public int msgType;
	public Object msgContent;
	public byte[] authCode; //authentication code
	public String senderID;
	
	public Message() {
		//initialization
		msgType = 0;
		msgContent = null;
		authCode = null;
	}
	
	public String toString() {
		String messageString = "";
		messageString += "=== Message Description BEGIN ===\n";
		messageString += "msgType: " + msgType + "\n";
		messageString += "authCode: " + authCode + "\n";
		messageString += "=== Message Description END ===\n\n";
		
		return messageString;
	}
	
}
