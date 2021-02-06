package rocky.ctrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ControlUserInterfaceRunner implements Runnable {

	String loggerID = "ControlUserInterface";
	boolean quitFlag = false;
	
	// command constants
	private final int CMD_ROLE_SWITCH = 1;
	private final int CMD_QUIT = 2;
	
	Thread roleSwitcherThread;
	
	public ControlUserInterfaceRunner (Thread rsThread) {
		roleSwitcherThread = rsThread;
	}

	protected void cmdRoleSwitch() {
		RockyController.RockyControllerRoleType curRole = null;
		synchronized(RockyController.role) {
			curRole = RockyController.role;
		}
		System.out.println("role switching.. current role=" + curRole);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				System.in));
		System.out.println("[" + loggerID + "] To which role, "
				+ "do you want to switch to?\n"
				+ "[1] None\n"
				+ "[2] NonOwner\n"
				+ "[3] Owner\n");
		String input = null;
		try {
			input = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		invokeRoleSwitching(input);
	}
	
	public void invokeRoleSwitching(String input) {
		RockyController.RockyControllerRoleType newRole = null;
		switch(Integer.parseInt(input)) {
		case 1:
			newRole = RockyController.RockyControllerRoleType.None;
			break;
		case 2:
			newRole = RockyController.RockyControllerRoleType.NonOwner;
			break;
		case 3:
			newRole = RockyController.RockyControllerRoleType.Owner;
			break;
		default:
			break;
		}
		
		// Check assertion
		RockyController.RockyControllerRoleType prevRole = null;
		synchronized(roleSwitcherThread) {
			prevRole = RockyController.role;
		}
		boolean fromNoneToOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.None)
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
		boolean fromNoneOwnerToOwner = 
				prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.Owner);
		boolean fromOwnerToNoneOwner =
				prevRole.equals(RockyController.RockyControllerRoleType.Owner) 
				&& newRole.equals(RockyController.RockyControllerRoleType.NonOwner);
		boolean fromNoneOwnerToNone = 
				prevRole.equals(RockyController.RockyControllerRoleType.NonOwner)
				&& newRole.equals(RockyController.RockyControllerRoleType.None);
		if (!(fromNoneToOwner || fromNoneOwnerToOwner 
				|| fromOwnerToNoneOwner || fromNoneOwnerToNone)) {
			System.err.println("ASSERT: unallowed role switching scenario");
			System.err.println("From=" + prevRole.toString() + " To=" + newRole.toString());
			System.err.println("We will ignore the role switching request");
		} else {
			//synchronized(RockyController.role) {
			synchronized(roleSwitcherThread) {
				RockyController.role = newRole;
				roleSwitcherThread.notify();
			}	
		}
	}
	
	@Override
	public void run() {
		try {
			System.out.println("[RoleSwitcher] entered run");
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
						cmdRoleSwitch();
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
