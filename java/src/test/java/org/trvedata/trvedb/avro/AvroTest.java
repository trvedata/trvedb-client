package org.trvedata.trvedb.avro;

import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import org.junit.Test;
import org.trvedata.trvedb.client.websocket.AvroTrveDbCodec;

public class AvroTest {
	
	@Test
	public void testEncode() {
		ChannelID channelID = new AvroTrveDbCodec().channelID("12345678901234567890123456789012");
		SubscribeToChannel subscribeToChannel = new SubscribeToChannel(channelID, 0L);
		ClientToServer clientToServer = new ClientToServer(subscribeToChannel);
		ByteBuffer buf = new AvroTrveDbCodec().toByteBuffer(clientToServer);
		System.out.println(DatatypeConverter.printHexBinary(buf.array()));
	}
}
