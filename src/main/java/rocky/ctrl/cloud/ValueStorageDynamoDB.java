package rocky.ctrl.cloud;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * This class represent generic key-value storage that is a wrapper of DynamoDB.
 *   
 * @author ben
 *
 */

public class ValueStorageDynamoDB implements GenericKeyValueStore {

	AmazonDynamoDB client;
	DynamoDB dynamoDB;
	String tableName;
    Table table;
    public Boolean consistHSReads = false;
	
	public ValueStorageDynamoDB(String filename, boolean localMode) {
		if (localMode) {
			// To use the local version dynamodb for development
			client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
					.build();
		} else {
			// To use the actual AWS
			client = AmazonDynamoDBClientBuilder.defaultClient();
		}
		
		dynamoDB = new DynamoDB(client);

		// we use filename given as the table name
		tableName = filename;
		
		// check if the given tableName already exists in the database
		ListTablesResult tableLists = client.listTables();
		if (tableLists.getTableNames().contains(tableName)) {
			System.out.println("The table name " + tableName + " already exists.");
			table = dynamoDB.getTable(tableName);
		} else {
			createTable(tableName);
		}
	}
	
	private void createTable(String tableName) {
        try {
            System.out.println("Attempting to create table; please wait...");
    		if (tableName.contentEquals("historykeyspace")) {
    			table = dynamoDB.createTable(tableName,
    					Arrays.asList(new KeySchemaElement("keyspace", KeyType.HASH), // Partition
    							                                                      // key
    							new KeySchemaElement("timestamp", KeyType.RANGE)), // Sort key
    					Arrays.asList(new AttributeDefinition("keyspace", ScalarAttributeType.S),
    							new AttributeDefinition("timestamp", ScalarAttributeType.S)),
    					new ProvisionedThroughput(10L, 10L));
    		} else if (tableName.contentEquals("datakeyspace")) {
    			table = dynamoDB.createTable(tableName,
    					Arrays.asList(new KeySchemaElement("key", KeyType.HASH)), // Partition
					                                                            // key
    					Arrays.asList(new AttributeDefinition("key", ScalarAttributeType.S)),
    					new ProvisionedThroughput(10L, 10L));
    		} else {
    			System.err.println("Unknown tableName. Pick either historykeyspace or datakeyspace");
    			System.exit(1);
    		}
    		table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        }
        catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }		
	}
	
	@Override
	public void finish() throws IOException {
		// TODO Auto-generated method stub
		client.shutdown();
		dynamoDB.shutdown();
	}

	@Override
	public byte[] get(String key) throws IOException {
		// ASSERT: should not be called for historykeyspace table 
		if (tableName.contentEquals("historykeyspace")) {
			System.err.println("ASSERT: should not be called for historykeyspace table");
			System.exit(1);
		}
		
		byte[] retValue = null;
		
		GetItemSpec spec = new GetItemSpec().withPrimaryKey("key", key);

        try {
            System.out.println("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            System.out.println("GetItem succeeded: " + outcome);
            Object outcomeValue = outcome.get("value");
            retValue = (byte[]) outcomeValue;
        }
        catch (Exception e) {
            System.err.println("Unable to read item: " + key);
            System.err.println(e.getMessage());
        }
        
		return retValue;
	}

	@Override
	public SortedMap<String, byte[]> get(String start, String end) throws IOException, ParseException {
		// ASSERT: should be called only for historykeyspace table 
		if (!tableName.contentEquals("historykeyspace")) {
			System.err.println("ASSERT: should not be called for historykeyspace table");
			System.exit(1);
		}

		SortedMap<String, byte[]> histSeg = null;
		
		Map<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#ks", "keyspace");
		nameMap.put("#ts", "timestamp");

        Map<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":ksn", tableName);
        valueMap.put(":start", start);
        valueMap.put(":end", end);
        
		QuerySpec querySpec = new QuerySpec()
				.withKeyConditionExpression("#ks = :ksn and #ts between :start and :end")
				.withNameMap(nameMap)
				.withValueMap(valueMap);
		querySpec = querySpec.withConsistentRead(consistHSReads);
       
		ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
		
		//try {
			System.out.println("Operations from " + start + " and " + end);
			items = table.query(querySpec);
			iterator = items.iterator();
			histSeg = new TreeMap<String, byte[]>();
			while (iterator.hasNext()) {
				item = iterator.next();
				histSeg.put(item.getString("timestamp"), item.getBinary("operation"));
			}
		//}
		//catch (Exception e) {
		//	System.err.println("Unable to query movies from " + start + " and " + end);
		//	System.err.println(e.getMessage());
		//}

		return histSeg;
	}

	@Override
	public void put(String key, byte[] value) throws IOException {
		try {
            System.out.println("Adding a new item...");
            PutItemOutcome outcome = null;
    		if (tableName.contentEquals("historykeyspace")) {
    			System.out.println("Adding to historykeyspace");
    			outcome = table.putItem(new Item().withPrimaryKey("keyspace", tableName, 
    					"timestamp", key).withBinary("operation", value));
    		} else if (tableName.contentEquals("datakeyspace")) {
    			System.out.println("Adding to datakeyspace");
				outcome = table.putItem(new Item().withPrimaryKey("key", key).withBinary(
						"value", value));
    		} else {
    			System.err.println("Unknown tableName. Pick either historykeyspace or datakeyspace");
    			System.exit(1);
    		}
            
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

		} catch (Exception e) {
			System.err.println("Unable to add item: " + key);
			System.err.println(e.getMessage());
		}

	}

	@Override
	public void remove(String key) {
		if (tableName.contentEquals("historykeyspace")) {
			DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
	                .withPrimaryKey(new PrimaryKey("keyspace", tableName, "timestamp", key));

	        try {
	            System.out.println("Attempting a delete...");
	            table.deleteItem(deleteItemSpec);
	            System.out.println("DeleteItem succeeded");
	        }
	        catch (Exception e) {
	            System.err.println("Unable to delete item: " + key);
	            System.err.println(e.getMessage());
	        }

		} else if (tableName.contentEquals("datakeyspace")) {
			DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
	                .withPrimaryKey(new PrimaryKey("key", key));

	        try {
	            System.out.println("Attempting a delete...");
	            table.deleteItem(deleteItemSpec);
	            System.out.println("DeleteItem succeeded");
	        }
	        catch (Exception e) {
	            System.err.println("Unable to delete item: " + key);
	            System.err.println(e.getMessage());
	        }

		} else {
			System.err.println("Unknown tableName. Pick either historykeyspace or datakeyspace");
			System.exit(1);
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		ValueStorageDynamoDB historyKeyspace = new ValueStorageDynamoDB("historykeyspace", true);
		ValueStorageDynamoDB dataKeyspace = new ValueStorageDynamoDB("datakeyspace", true);
		
		System.out.println("-----------------------------------------");
		System.out.println("datakeyspace test");
		System.out.println("-----------------------------------------");
		
		byte[] buf = dataKeyspace.get("hello");
		if (buf == null)
			System.out.println("There is no such key=hello in datakeyspace.");
		else
			System.out.println("For key=hello, datakeyspace returned value=" + new String(buf));

		dataKeyspace.put("hello", "world".getBytes());
		buf = dataKeyspace.get("hello");
		System.out.println("For key=hello, datakeyspace returned value=" + new String(buf));
		
		dataKeyspace.put("goodbye", "cruel world".getBytes());
		buf = dataKeyspace.get("goodbye");
		System.out.println("For key=goodbye, datakeyspace returned value=" + new String(buf));
		
		dataKeyspace.remove("hello");
		buf = dataKeyspace.get("hello");
		if (buf != null) {
			System.out.println("For key=hello, datakeyspace returned value=" + new String(buf));
		} else {
			System.out.println("There is no such key=hello in datakeyspace.");
		}
		
		System.out.println("-----------------------------------------");
		System.out.println("historykeyspace test");
		System.out.println("-----------------------------------------");

		SortedMap<String, byte[]> histSeg = null;
		historyKeyspace.put("hello", "world".getBytes());
		try {
			histSeg = historyKeyspace.get("h", "i");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String elemKey : histSeg.keySet()) {
			byte[] elemValue = histSeg.get(elemKey);
			System.out.println("For key=" + elemKey + ", returned value=" + new String(elemValue));
		}
		
		historyKeyspace.put("goodbye", "cruel world".getBytes());
		try {
			histSeg = historyKeyspace.get("g", "h");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String elemKey : histSeg.keySet()) {
			byte[] elemValue = histSeg.get(elemKey);
			System.out.println("For key=" + elemKey + ", returned value=" + new String(elemValue));
		}
		try {
			histSeg = historyKeyspace.get("h", "i");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String elemKey : histSeg.keySet()) {
			byte[] elemValue = histSeg.get(elemKey);
			System.out.println("For key=" + elemKey + ", returned value=" + new String(elemValue));
		}
		try {
			histSeg = historyKeyspace.get("g", "i");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String elemKey : histSeg.keySet()) {
			byte[] elemValue = histSeg.get(elemKey);
			System.out.println("For key=" + elemKey + ", returned value=" + new String(elemValue));
		}
		
		historyKeyspace.remove("hello");
		historyKeyspace.remove("goodbye");
		try {
			histSeg = historyKeyspace.get("g", "i");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (histSeg == null || histSeg.isEmpty()) {
			System.out.println("There is no such key=hello and key=goodbye exist currently.");
		} else {
			for (String elemKey : histSeg.keySet()) {
				byte[] elemValue = histSeg.get(elemKey);
				System.out.println("For key=" + elemKey + ", returned value=" + new String(elemValue));
			}
		}
	}

}