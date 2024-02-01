package rocky.recovery.rpc;

public class MessageType {

	// Message Type Constants.
	public static final int MSG_T_NACK = -1;
	public static final int MSG_T_ACK = 0;
	public static final int MSG_T_REGISTER_ENDPOINT = 1;
	public static final int MSG_T_NO_CLOUD_FAILURE_RECOVERY = 2;
	public static final int MSG_T_GET_CMD = 3;
	public static final int MSG_T_SEND_CMD = 4;
	
	// Client to Phone message type constants
	public static final int CM_T_DW_F = 0;
	public static final int CM_T_CL_F = 1;
	public static final int CM_T_CL_J1 = 2;
	public static final int CM_T_CL_J2 = 3;
	
	// Update Type Constants. HB=heartbeat, CU=client update, WU=watcher update
	public static final int UP_T_HB = 0;
	public static final int UP_T_CU = 1;
	public static final int UP_T_WU = 2;
	
	// notification
	public static final String GCM_NOTIFICATION = "notificationType";
	
	
}

