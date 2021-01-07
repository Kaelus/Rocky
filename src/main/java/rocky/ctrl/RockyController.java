package rocky.ctrl;

import static rocky.ctrl.NBD.INIT_PASSWD;
import static rocky.ctrl.NBD.NBD_FLAG_HAS_FLAGS;
import static rocky.ctrl.NBD.NBD_OPT_EXPORT_NAME;
import static rocky.ctrl.NBD.OPTS_MAGIC_BYTES;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

//import nbdfdb.NBDVolumeServer;

public class RockyController {

	private static Logger log = Logger.getLogger("NBD");
	
	private static Integer port = 10809;

	// HARD-CODED for now
	private static String nodeID = "node0";
	
	public static void main (String args[]) throws IOException {
		System.out.println("Hello, Rocky");
		
		ExecutorService es = Executors.newCachedThreadPool();
	    log.info("Listening for nbd-client connections");
	    ServerSocket ss = new ServerSocket(port);
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
	          int opt = in.readInt();
	          if (opt != NBD_OPT_EXPORT_NAME) {
	            throw new RuntimeException("We support only EXPORT options");
	          }
	          int length = in.readInt();
	          byte[] bytes = new byte[length];
	          in.readFully(bytes);
	          String exportName = new String(bytes, Charsets.UTF_8);
	          //exportName = getUniqueExportName();
	          log.info("Connecting client to " + exportName);
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
}
