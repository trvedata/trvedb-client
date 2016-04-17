package org.trvedata.trvedb.client.websocket;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

import javax.websocket.ClientEndpoint;
import javax.websocket.CloseReason;
import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.WebSocketContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClientEndpoint
public class WebSocketClient {
	private static final Logger log = LoggerFactory.getLogger(WebSocketClient.class);
	
	private Session session;
	private BinaryMessageHandler messageHandler;

	public interface BinaryMessageHandler {
		public void handleMessage(byte[] binaryMessage);
	}
	
	public WebSocketClient() {
	}
	
	public void connect(URI serverUri) throws DeploymentException, IOException {
		WebSocketContainer container = ContainerProvider.getWebSocketContainer();
		container.connectToServer(this, serverUri);
	}

	@OnOpen
	public void onOpen(Session userSession) {
		this.session = userSession;
		log.debug("Session opened: {}", this.session);
	}

	@OnClose
	public void onClose(Session userSession, CloseReason reason) {
		this.session = null;
	}

	@OnMessage
	public void onMessage(byte[] binaryMsg) {
		log.debug("Binary message received. Size: {}", binaryMsg.length);
		if (this.messageHandler != null)
			this.messageHandler.handleMessage(binaryMsg);
	}
	
	public void setMessageHandler(BinaryMessageHandler msgHandler) {
		this.messageHandler = msgHandler;
	}

	public void sendDataAsync(ByteBuffer data) {
		this.session.getAsyncRemote().sendBinary(data);
	}
	
	public void sendData(ByteBuffer data) throws IOException {
		this.session.getBasicRemote().sendBinary(data, true);
	}

	public void close() throws IOException {
		this.session.close();
	}
}