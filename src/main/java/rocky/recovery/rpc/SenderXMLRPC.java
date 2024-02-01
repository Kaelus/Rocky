package rocky.recovery.rpc;

import java.net.URL;
import java.util.Vector;

import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

import rocky.ctrl.utils.DebugLog;
import rocky.ctrl.utils.ObjectSerializer;

public class SenderXMLRPC {

	// RPC related
	private static final String SVR_URL = "http://127.0.0.1:8090/xmlrpc"; //Hard coded
	private XmlRpcClientConfigImpl config;
	public XmlRpcClient client;
	//private Vector<Object> commitParams;
	public String reqHandlerName;
	public String serverName;
	
	public String loggerID = null;
	
	public SenderXMLRPC() {
		try {
			
			//RPC related
			config = new XmlRpcClientConfigImpl();
			config.setEnabledForExtensions(true);
			config.setServerURL(new URL(SVR_URL));
			client = new XmlRpcClient();
			client.setConfig(config);
			
			reqHandlerName = "Coordinator.handleClientRequest";
			
		} catch(Exception exception) {
			DebugLog.elog("JavaClient.init: " + exception, loggerID);
		}
		
	}
	
	public SenderXMLRPC(String srvURL, String srvName, String reqHN) {
		try {
			
			//RPC related
			config = new XmlRpcClientConfigImpl();
			config.setEnabledForExtensions(true);
			config.setServerURL(new URL(srvURL));
			client = new XmlRpcClient();
			client.setConfig(config);
	
			serverName = srvName;
			
			reqHandlerName = reqHN;
			
		} catch(Exception exception) {
			DebugLog.log("JavaClient.init: " + exception, loggerID);
			System.exit(1);
		}
		
	}
	
	public static String getSrvURL(String ip, String port){
		return "http://" + ip + ":" + port + "/xmlrpc";
	}
	
/*	// RPC REQUEST; receive RESPONSE on return of execute
	public Response request(Request request) {
		try {

			
			 * // REQUEST Vector<Integer> params = new Vector<Integer>();
			 * params.addElement(new Integer(17)); params.addElement(new
			 * Integer(16)); Object result = client.execute("UnityServer.sum",
			 * params); System.out.println("request response: " + result);
			 
			// Try to send Update Request Msg
			Vector<Object> requestParams;
			requestParams = new Vector<Object>();
			requestParams.addElement(ObjectSerializer.serialize(request));
			System.out.println("Client REQUEST seqNo: " + request.seqNo);
			Object result = client
					.execute("UnityServer.request", requestParams);
			Response response = (Response) ObjectSerializer
					.deserialize((byte[]) result);

			System.out.println("Server RESPONSE: ");
			System.out.println(response.toString());

			return response;

		} catch (Exception exception) {
			System.err.println("JavaClient.request: " + exception);
		}

		return null;
	}

	// RPC COMMIT; receive ACK on return of execute
	public Ack commit(Update update) {
		try {

			
			 * //COMMIT params = new Vector<Integer>(); params.addElement(new
			 * Integer(19)); params.addElement(new Integer(21)); Object result =
			 * client.execute("UnityServer.commit", params);
			 * System.out.println("commit response: " + result);
			 

			// COMMIT
			commitParams = new Vector<Object>();
			Commit ci = new Commit(update);
			commitParams.addElement(ObjectSerializer.serialize(ci));
			Ack result = (Ack) client.execute("UnityServer.commit",
					commitParams);
			System.out.println("commit response: " + result.ackFlag);

			return result;

		} catch (Exception exception) {
			System.err.println("JavaClient.commit: " + exception);
		}

		return null;
	}*/
	
	// send the given msg and return what it gets from the receiver
	public int sum(int i1, int i2) throws Exception {
		Vector<Object> requestParams;
		requestParams = new Vector<Object>();
		requestParams.addElement(new Integer(i1));
		requestParams.addElement(new Integer(i2));
		Object result = client.execute(serverName + ".sum",
				requestParams);
		int srvResp = ((Integer) result).intValue();

		//DebugLog.log("srvResp: " + srvResp);

		return srvResp;

	}
	
	public void finish() throws Exception {
		Vector<Object> requestParams = new Vector<Object>();
		client.execute(reqHandlerName + "Finish", requestParams);
	}
	
/*	public int sendValues(Vector<GSKeyValue> values) throws Exception {
		Vector<Object> requestParams;
		requestParams = new Vector<Object>();
		requestParams.addElement(ObjectSerializer.serialize(values));
		Object result = client
				.execute(reqHandlerName, requestParams);
		
		return ((Integer)result).intValue();
		
	}*/
	
	
	//send the given msg and return what it gets from the receiver
	public Message send(Message msg) throws Exception {
		Vector<Object> requestParams;
		requestParams = new Vector<Object>();
		
		//DebugLog.log("bytes of msg to Send using classmexer is=" + MemoryUtil.deepMemoryUsageOf(msg,VisibilityFilter.ALL));
		
		byte[] bytesToSend = ObjectSerializer.serialize(msg);
		//DebugLog.log("bytesToSend is=" + bytesToSend.length);
		
		//ByteArrayOutputStream baos = new ByteArrayOutputStream();
		//new ObjectOutputStream( baos ).writeObject(bytesToSend);
		//System.out.println("base64 bytes length=" + Base64.encode(baos.toByteArray()).length());
		//System.out.println("base64 bytes length=" + Base64.encode(bytesToSend).length());
		
		//DebugLog.log("bytes To Send using classmexer is=" + MemoryUtil.deepMemoryUsageOf(bytesToSend, VisibilityFilter.ALL));
		
		requestParams.addElement(bytesToSend);
		//requestParams.addElement((Object)msg);
		
		//DebugLog.log("bytes of requestParams to Send using classmexer is=" + MemoryUtil.deepMemoryUsageOf(requestParams, VisibilityFilter.ALL));
		
		
		Object result = client
				.execute(reqHandlerName, requestParams);
		
		//DebugLog.log("bytes To Recv before deserialization is=" + ((byte[])result).length);
		
		Message srvResp = (Message) ObjectSerializer
				.deserialize((byte[]) result);
		
		//System.out.println("[UnitySenderXMLRPC.send] srvResp:\n" + srvResp.toString());
		
		/*Message srvResp = (Message) result;
		System.out.println("srvResp" + srvResp.toString());
		return srvResp;
		*/
		//System.out.println("result " + ((Message) ObjectSerializer.deserialize((byte[])result)).toString());
		return srvResp;//new Message();
		
	}
	
	//send the given msg and return what it gets from the receiver
	public Object serializedSend(Object serializedMsg) throws Exception {
		Vector<Object> requestParams;
		requestParams = new Vector<Object>();
		requestParams.addElement(serializedMsg);
		Object result = client
				.execute(reqHandlerName, requestParams);
		return result;
		
	}
}
