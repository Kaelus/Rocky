package rocky.ctrl.cloud;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import rocky.ctrl.cloud.ValueStorageDynamoDB;

public class ValueStorageDynamoDBTest {
	
	@Test
	public void testPutAndGet() {
		ValueStorageDynamoDB testStorage = new ValueStorageDynamoDB("testStorage", true);
		byte[] buf = null;
		try {
			testStorage.put("" + 777, "hello world".getBytes());
			buf = testStorage.get("" + 777);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		testStorage.deleteTable("testStorage");
		Assert.assertArrayEquals("hello world".getBytes(), buf);
	}

	@Test
	public void testDeleteAndGetNonExistKey() {
		ValueStorageDynamoDB testStorage = new ValueStorageDynamoDB("testStorage", true);
		testStorage.remove("" + 777);
		byte[] buf = null;
		try {
			buf = testStorage.get("" + 777);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		testStorage.deleteTable("testStorage");
		Assert.assertNull(buf);
	}

}
