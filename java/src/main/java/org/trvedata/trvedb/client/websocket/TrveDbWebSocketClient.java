package org.trvedata.trvedb.client.websocket;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import javax.websocket.DeploymentException;

import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.ClientToServer;
import org.trvedata.trvedb.avro.SendMessage;
import org.trvedata.trvedb.avro.ServerToClient;
import org.trvedata.trvedb.avro.SubscribeToChannel;
import org.trvedata.trvedb.client.websocket.WebSocketClient.BinaryMessageHandler;

public class TrveDbWebSocketClient {

	private final WebSocketClient webSocketClient;
	private final ServerToClientMessageHandler serverToClientMessageHandler;

	public TrveDbWebSocketClient(TrveDbMessageHandler messageHandler) {
		this.webSocketClient = new WebSocketClient();
		this.webSocketClient.setMessageHandler(new BinaryMessageHandler() {
			
			@Override
			public void handleMessage(byte[] binaryMsg) {
				TrveDbWebSocketClient.this.handleBinaryMessage(binaryMsg);
			}
		});
		
		this.serverToClientMessageHandler = new ServerToClientMessageHandler(messageHandler);
	}

	void handleBinaryMessage(byte[] binaryMsg) {
		ServerToClient serverToClientMsg = new AvroTrveDbCodec().toServerToClientMessage(binaryMsg);
		processServerToClientMessage(serverToClientMsg);
	}

	private void processServerToClientMessage(ServerToClient message) {
		serverToClientMessageHandler.processServerToClientMessage(message);
	}

	public void connect(URI serverUri, String peerId) throws IOException {
		try {
			String query = "peer_id=" + peerId;
			URI fullUri = new URI(
				serverUri.getScheme(), 
				serverUri.getAuthority(), 
				serverUri.getPath(), 
				query,
				serverUri.getFragment());
			webSocketClient.connect(fullUri);
		} catch (DeploymentException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}
	
	public void sendSubscriptionRequest(ChannelID channelID, long startOffset) throws IOException {
		SubscribeToChannel subscribeToChannel = new SubscribeToChannel(channelID, startOffset);
		ClientToServer clientToServer = new ClientToServer(subscribeToChannel);
		webSocketClient.sendData(new AvroTrveDbCodec().toByteBuffer(clientToServer));
	}
	
	public void sendMessage(SendMessage message) throws IOException {
		webSocketClient.sendData(new AvroTrveDbCodec().toByteBuffer(new ClientToServer(message)));
	}
	
	public void close() throws IOException {
		webSocketClient.close();
	}
}
