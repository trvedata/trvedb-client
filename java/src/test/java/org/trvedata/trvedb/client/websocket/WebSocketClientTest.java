package org.trvedata.trvedb.client.websocket;

import java.net.URI;

import org.junit.Before;
import org.junit.Test;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.ReceiveMessage;
import org.trvedata.trvedb.avro.SendMessageError;

public class WebSocketClientTest {
	
	private String host = "localhost";
	private int port = 8080;
	private TrveDbMessageHandler simpleMessageHandler;
	
	@Before
	public void setUp() {
		simpleMessageHandler = new TrveDbMessageHandler() {
			@Override
			public void processSendMessageError(SendMessageError message) {
				System.err.println("Error sending message");
			}
			
			@Override
			public void processReceivedMessage(ReceiveMessage message) {
				System.err.println("Message received. Size: " + message.getPayload().remaining());
			}
		};
	}
	
	private String buildUrl() {
		return "ws://" + host + ":" + port + "/events";
	}
	
	@Test
	public void testWebSocketClient() throws Exception {
		URI serverUri = new URI(buildUrl() + "?peer_id=17a42e475338a2012906afa99510ccfd9e757a0d1129aa40706113c6f7e005e4");
		WebSocketClient client = new WebSocketClient();
		client.connect(serverUri);
	}
	
	@Test
	public void testTrveDbWebSocketClient() throws Exception {
		URI serverUri = new URI(buildUrl());
		TrveDbWebSocketClient client = new TrveDbWebSocketClient(simpleMessageHandler);
		client.connect(serverUri, "17a42e475338a2012906afa99510ccfd9e757a0d1129aa40706113c6f7e005e4");
	}
	
	@Test
	public void testSendSubscriptionRequest() throws Exception {
		URI serverUri = new URI(buildUrl());
		TrveDbWebSocketClient client = new TrveDbWebSocketClient(simpleMessageHandler);
		client.connect(serverUri, "17a42e475338a2012906afa99510ccfd9e757a0d1129aa40706113c6f7e005e4");
		ChannelID channelID = new AvroTrveDbCodec().channelID("c9caf6e952b7a5307542046bff66ff94");
		client.sendSubscriptionRequest(channelID, 0);
		Thread.sleep(5000);
	}
}
