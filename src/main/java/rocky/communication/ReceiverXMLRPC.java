package rocky.communication;

import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;

import rocky.ctrl.utils.DebugLog;

public class ReceiverXMLRPC {
	public WebServer webServer;

	//logging info
	public String loggerID = "ReceiverXMLRPC";
	
	public ReceiverXMLRPC(int port) {

		try {
			//DebugLog.log("Attempting to start XML-RPC Server...port="+port, loggerID);

			webServer = new WebServer(port);

			XmlRpcServer xmlRpcServer = webServer.getXmlRpcServer();

			PropertyHandlerMapping phm = new PropertyHandlerMapping();
			/*
			 * Load handler definitions from a property file. The property file
			 * might look like: Calculator=org.apache.xmlrpc.demo.Calculator
			 * org.
			 * apache.xmlrpc.demo.proxy.Adder=org.apache.xmlrpc.demo.proxy.AdderImpl
			 */
			// phm.load(Thread.currentThread().getContextClassLoader(),
			// "XmlRpcServlet.properties");
			
			phm.addHandler("PeerComXMLRPC", rocky.communication.PeerComXMLRPC.class);

			/*
			 * You may also provide the handler classes directly, like this:
			 * phm.addHandler("Calculator",
			 * org.apache.xmlrpc.demo.Calculator.class);
			 * phm.addHandler(org.apache
			 * .xmlrpc.demo.proxy.Adder.class.getName(),
			 * org.apache.xmlrpc.demo.proxy.AdderImpl.class);
			 */
			xmlRpcServer.setHandlerMapping(phm);

			XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) xmlRpcServer
					.getConfig();
			serverConfig.setEnabledForExtensions(true);
			serverConfig.setContentLengthOptional(false);

			webServer.start();

			/*
			 * WebServer server = new WebServer(80); server.addHandler("sample",
			 * new JavaServer()); server.start();
			 */
			DebugLog.log("Started successfully.", loggerID);
			DebugLog.log("Accepting requests. (Halt program to stop.)", loggerID);
			
		} catch (Exception exception) {
			DebugLog.log("JavaServer: " + exception + " port: " + port, loggerID);
			System.exit(1);
		}

	}	

}
