package rocky.ctrl;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ControlUserInterfaceRunner implements Runnable {

	String loggerID = "ControlUserInterface";
	boolean quitFlag = false;
	
	// command constants
	private final int CMD_ROLE_SWITCH = 1;
	private final int CMD_QUIT = 2;
	
	public ControlUserInterfaceRunner () {
		
	}

	protected void roleSwitch() {
		RockyController.RockyControllerRoleType curRole = null;
		synchronized(RockyController.role) {
			curRole = RockyController.role;
		}
		System.out.println("role switching.. current role=" + curRole);
		RockyController.RockyControllerRoleType switchTo = null;
		if (curRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				|| curRole.equals(RockyController.RockyControllerRoleType.None)) {
			switchTo = RockyController.RockyControllerRoleType.Owner; 
		} else {
			switchTo = RockyController.RockyControllerRoleType.NonOwner;
		}
		synchronized(RockyController.role) {
			RockyController.role = switchTo;
		}
	}
	
	public void run() {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			String input;
			System.out
					.println("[" + loggerID + "] What do you want to do? "
							+ "[" + CMD_ROLE_SWITCH + "] role switch "
							+ "[" + CMD_QUIT + "] quit \n");
			while (!quitFlag && ((input = br.readLine()) != null)) {
				try {
					int cmd = Integer.valueOf(input);
					System.out.println("[" + loggerID + "] cmd=" + cmd); 
					switch (cmd) {
					case CMD_ROLE_SWITCH:
						roleSwitch();
						break;
					case CMD_QUIT:
						quitFlag = true;
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
							+ "[" + CMD_ROLE_SWITCH + "] role switch "
							+ "[" + CMD_QUIT + "] quit \n");
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
