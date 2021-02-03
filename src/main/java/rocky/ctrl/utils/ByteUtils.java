package rocky.ctrl.utils;

import java.nio.ByteBuffer;

public class ByteUtils {
	
    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);    

    public static byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
    	//System.out.println("buffer capacity=" + buffer.capacity());
    	//System.out.println("buffer limit=" + buffer.limit());
    	//System.out.println("bytes length=" + bytes.length);
        //System.out.println("buffer remaining=" + buffer.remaining());
        buffer.position(0);
    	buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip 
        return buffer.getLong();
    }
	
}
