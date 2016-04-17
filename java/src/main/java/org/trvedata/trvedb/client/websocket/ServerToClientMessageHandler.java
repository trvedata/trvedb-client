package org.trvedata.trvedb.client.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trvedata.trvedb.avro.ReceiveMessage;
import org.trvedata.trvedb.avro.SendMessageError;
import org.trvedata.trvedb.avro.ServerToClient;

public class ServerToClientMessageHandler {
	private static final Logger log = LoggerFactory.getLogger(ServerToClientMessageHandler.class);

	private final TrveDbMessageHandler messageHandler;
	
	public ServerToClientMessageHandler(TrveDbMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	public void processServerToClientMessage(ServerToClient serverToClientMsg) {
		Object message = serverToClientMsg.getMessage();
		log.debug("Message of type {} received.", message.getClass().getName());
		log.trace("Message received: {}", message);
		processMessageContent(message);
	}

	protected void processMessageContent(Object message) {
		if (message instanceof ReceiveMessage) {
			processReceiveMessage((ReceiveMessage) message);
		} else if (message instanceof SendMessageError) {
			processSendMessageError((SendMessageError)message);
		} else {
			throw new RuntimeException("Unexpected message type: " + message.getClass().getName());
		}
	}

	protected void processSendMessageError(SendMessageError message) {
		log.error("Error sending message for channel ID {}. Last known sequence number: {}", message.getChannelID(), message.getLastKnownSeqNo());
		messageHandler.processSendMessageError(message);
	}

	protected void processReceiveMessage(ReceiveMessage message) {
		messageHandler.processReceivedMessage(message);
	}
}
