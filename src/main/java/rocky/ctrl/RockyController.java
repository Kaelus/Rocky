package rocky.ctrl;

import static rocky.ctrl.NBD.INIT_PASSWD;
import static rocky.ctrl.NBD.NBD_FLAG_HAS_FLAGS;
import static rocky.ctrl.NBD.NBD_OPT_EXPORT_NAME;
import static rocky.ctrl.NBD.OPTS_MAGIC_BYTES;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Charsets;

import rocky.recovery.RecoveryController;


//import nbdfdb.NBDVolumeServer;

public class RockyController {

	private static Logger log = Logger.getLogger("NBD");
	
	public static final Integer defaultPort = 10810;
	public static String myIP;
	public static Integer myPort;

	public static String nodeID;
	
	public static String lcvdName;
	
	public static String workingDir;
	
	public enum RockyModeType {Origin, Rocky, Recovery, Unknown};
	public static RockyModeType rockyMode; 
	
	public enum BackendStorageType {DynamoDBLocal, DynamoDB_SEOUL, 
		DynamoDB_LONDON, DynamoDB_OHIO, Unknown};
	public static BackendStorageType backendStorage;
	
	public enum RockyControllerRoleType {Owner, NonOwner, None};
	public static RockyControllerRoleType role;
	
	public static int epochPeriod;
	public static int prefetchPeriod;
		
	public static void main (String args[]) throws IOException {
		System.out.println("Hello, Rocky");
		//default variable settings
		myIP = "127.0.0.1";
		myPort = defaultPort;
		rockyMode = RockyModeType.Rocky;
		backendStorage = BackendStorageType.DynamoDBLocal;
		role = RockyControllerRoleType.None;
		epochPeriod = 30000; // 30sec
		prefetchPeriod = 300000; // 5min
		workingDir = ".";
		
		//update variables using config if given
		if (args.length < 2) {
			System.out.println("no config file is given.");
		} else if (args.length >= 2) {
			System.out.println("given config file=" + args[1]);
			parseRockyControllerConfig(args[1]);
		}
		//nodeID = "node" + getProcID();
		nodeID = myIP + ":" + myPort;
		
		//print out variable settings
		System.out.println("myIP=" + myIP);
		System.out.println("myPort=" + myPort);
		System.out.println("nodeID=" + nodeID);
		System.out.println("rockyMode=" + rockyMode);
		System.out.println("backendStorageType=" + backendStorage);
		System.out.println("workingDir=" + workingDir);

		// start 'recovery' mode
		if (rockyMode.equals(RockyModeType.Recovery)) {
			RecoveryController.runRecovery(args);
		} else { // start 'rocky' storage mode
			ExecutorService es = Executors.newCachedThreadPool();
		    log.info("Listening for nbd-client connections");
		    ServerSocket ss = new ServerSocket(myPort);
		    while (true) {
		      Socket accept = ss.accept();
		      es.submit(() -> {
		        try {
		          InetSocketAddress remoteSocketAddress = (InetSocketAddress) accept.getRemoteSocketAddress();
		          log.info("Client connected from: " + remoteSocketAddress.getAddress().getHostAddress());
		          DataInputStream in = new DataInputStream(accept.getInputStream());
		          DataOutputStream out = new DataOutputStream(new BufferedOutputStream(accept.getOutputStream()));
	
		          out.write(INIT_PASSWD);
		          out.write(OPTS_MAGIC_BYTES);
		          out.writeShort(NBD_FLAG_HAS_FLAGS);
		          out.flush();
	
		          // TODO: interpret the client flags.
		          int clientFlags = in.readInt();
		          long magic = in.readLong();
		          System.out.println("BK: magic=" + magic);
		          System.out.println("BK: magic(hex)=" + Long.toHexString(magic));
		          int opt = in.readInt();
		          System.out.println("BK: opt=" + opt);
		          if (opt != NBD_OPT_EXPORT_NAME) {
		            throw new RuntimeException("We support only EXPORT options");
		          }
		          int length = in.readInt();
		          byte[] bytes = new byte[length];
		          in.readFully(bytes);
		          String exportName = new String(bytes, Charsets.UTF_8);
		          //exportName = getUniqueExportName();
		          if (lcvdName != null) {
		        	  if (!exportName.equals(lcvdName)) {
			        	  System.err.println("exportName and lcvdName do not match!");
			        	  System.err.println("exportName=" + exportName);
			        	  System.err.println("lcvdName=" + lcvdName);
			        	  System.exit(1);
			          }
		          }
		          log.info("Connecting client to exportName=" + exportName);
		          NBDVolumeServer nbdVolumeServer = new NBDVolumeServer(exportName, in, out);
		          log.info("Volume mounted");
		          nbdVolumeServer.run();
		        } catch (Throwable e) {
		          log.log(Level.SEVERE, "Failed to connect", e);
		          try {
		            accept.close();
		          } catch (IOException e1) {
		            e1.printStackTrace();
		          }
		        }
		      });
		    }
		}
	}

	private static void parseRockyControllerConfig(String configFile) {
		File confFile = new File(configFile);
		if (!confFile.exists()) {
			if (!confFile.mkdir()) {
				System.err.println("Unable to find " + confFile);
	            System.exit(1);
	        }
		}
		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (line.startsWith("ip")) {
					String[] tokens = line.split("=");
					String ipPortStr = tokens[1];
					String[] ipPortStrArr = ipPortStr.split(":");
					myIP = ipPortStrArr[0];
					myPort = Integer.parseInt(ipPortStrArr[1]);
					nodeID = myIP + ":" + myPort;					
				} else if (line.startsWith("lcvdName")) {
		    	   String[] tokens = line.split("=");
		    	   lcvdName = tokens[1];
		    	   System.out.println("lcvdName=" + lcvdName);
		       } else if (line.startsWith("rockyMode")) {
		    	   String[] tokens = line.split("=");
		    	   String rockyModeTypeStr = tokens[1];
		    	   if (rockyModeTypeStr.equals("origin")) {
		    		   rockyMode = RockyModeType.Origin;
		    	   } else if (rockyModeTypeStr.equals("rocky")) {
		    		   rockyMode = RockyModeType.Rocky;
		    	   } else if (rockyModeTypeStr.equals("recovery")) {
		    		   rockyMode = RockyModeType.Recovery;
		    	   } else {
		    		   rockyMode = RockyModeType.Unknown;
		    	   }
		    	   System.out.println("rockyModeType=" + rockyMode);
		    	   if (rockyMode.equals(RockyModeType.Unknown)) {
		    		   System.err.println("Error: Cannot support Unknown RockyModeType");
		    		   System.exit(1);
		    	   }
		       } else if (line.startsWith("backendStorageType")) {
		    	   String[] tokens = line.split("=");
		    	   String backendStorageTypeStr = tokens[1];
		    	   if (backendStorageTypeStr.equals("dynamoDBLocal")) {
		    		   backendStorage = BackendStorageType.DynamoDBLocal;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBSeoul")) {
		    		   backendStorage = BackendStorageType.DynamoDB_SEOUL;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBLondon")) {
		    		   backendStorage = BackendStorageType.DynamoDB_LONDON;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBOhio")) {
		    		   backendStorage = BackendStorageType.DynamoDB_OHIO;
		    	   } else {
		    		   backendStorage = BackendStorageType.Unknown;
		    	   }
		    	   System.out.println("backendStorageType=" + backendStorage);
		    	   if (backendStorage.equals(BackendStorageType.Unknown)) {
		    		   System.err.println("Error: Cannot support Unknown backendStorageType");
		    		   System.exit(1);
		    	   }
		       } else if (line.startsWith("epochPeriod")) {
		    	   String[] tokens = line.split("=");
		    	   String epochPeriodStr = tokens[1];
		    	   epochPeriod = Integer.parseInt(epochPeriodStr);
		       } else if (line.startsWith("prefetchPeriod")) {
		    	   String[] tokens = line.split("=");
		    	   String prefetchPeriodStr = tokens[1];
		    	   prefetchPeriod = Integer.parseInt(prefetchPeriodStr);
		       } else if (line.startsWith("workingDir")) {
		    	   String[] tokens = line.split("=");
		    	   workingDir = tokens[1];
		       } else if (line.startsWith("debugFlag")) {
		    	   String[] tokens = line.split("=");
		    	   RockyStorage.debugPrintoutFlag = Boolean.parseBoolean(tokens[1]);
		       }
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static String getUniqueExportName() {
		InetAddress ip = null;
		try {
			ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			System.err.println("UnknownHostException..");
			e.printStackTrace();
		}
		String ipStr = ip.toString().split("/")[1];
		System.out.println("IP address is " + ipStr);
		String vmName = ManagementFactory.getRuntimeMXBean().getName();
        int p = vmName.indexOf("@");
        String pid = vmName.substring(0, p);
        System.out.println(pid);
		return pid + "_" + ipStr;
	}
	
	private static String getProcID() {
		String vmName = ManagementFactory.getRuntimeMXBean().getName();
        int p = vmName.indexOf("@");
        String pid = vmName.substring(0, p);
        //System.out.println(pid);
        return pid;
	}
	
}
