package org.trvedata.trvedb.client.websocket;

import org.trvedata.trvedb.avro.ReceiveMessage;
import org.trvedata.trvedb.avro.SendMessageError;

public interface TrveDbMessageHandler {
	void processSendMessageError(SendMessageError message);
	void processReceivedMessage(ReceiveMessage message);
}
