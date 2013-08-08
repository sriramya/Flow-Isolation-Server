package flume;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class Utility {

	static String getBinaryString(String data_in, int maskLen) throws UnknownHostException
    {
        InetAddress bar = InetAddress.getByName(data_in);
        int value = ByteBuffer.wrap(bar.getAddress()).getInt();
        String data_out = String.format("%32s", Integer.toBinaryString(value)).replace(" ", "0");
        if (maskLen == 0)
            return data_out;
//        System.out.println();
        return data_out.substring(0,maskLen);
       
     }
	public final static boolean ValidateIPAddress( String  ipAddress )
	{
	    String[] parts = ipAddress.split( "\\." );

	    if ( parts.length != 3 )
	    {
	        return false;
	    }

	    for ( String s : parts )
	    {
	        int i = Integer.parseInt( s );

	        if ( (i < 0) || (i > 255) )
	        {
	            return false;
	        }
	    }

	    return true;
	}
	
}

