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
		RockyController.RockyControllerRoleType switchTo = null;
		switch(Integer.parseInt(input)) {
		case 1:
			switchTo = RockyController.RockyControllerRoleType.None;
			break;
		case 2:
			switchTo = RockyController.RockyControllerRoleType.NonOwner;
			break;
		case 3:
			switchTo = RockyController.RockyControllerRoleType.Owner;
			break;
		default:
			break;
		}
		//synchronized(RockyController.role) {
		synchronized(roleSwitcherThread) {
			RockyController.role = switchTo;
			roleSwitcherThread.notify();
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
