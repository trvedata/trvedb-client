package org.trvedata.trvedb.client.websocket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.trvedata.trvedb.avro.ChannelID;
import org.trvedata.trvedb.avro.ClientToServer;
import org.trvedata.trvedb.avro.PeerID;
import org.trvedata.trvedb.avro.SendMessage;
import org.trvedata.trvedb.avro.ServerToClient;

public class AvroTrveDbCodec {
    
    public ChannelID channelID(String hexChannelID) {
        if (hexChannelID.length() != 32) {
            throw new IllegalArgumentException("ChannelID must be 128-bit hex");
        }
        return new ChannelID(DatatypeConverter.parseHexBinary(hexChannelID));
    }

    public String channelID(ChannelID channelID) {
        return DatatypeConverter.printHexBinary(channelID.bytes()).toLowerCase();
    }

    public PeerID peerID(String hexPeerID) {
        if (hexPeerID.length() != 64) {
            throw new IllegalArgumentException("PeerID must be 256-bit hex");
        }
        return new PeerID(DatatypeConverter.parseHexBinary(hexPeerID));
    }

    public String peerID(PeerID peerID) {
        return DatatypeConverter.printHexBinary(peerID.bytes()).toLowerCase();
    }
    
    public SendMessage sendMessage(ChannelID channelID, long senderSeqNo, ByteBuffer payload) {
    	return new SendMessage(channelID, senderSeqNo, payload);
    }
    
    public ByteBuffer toByteBuffer(ClientToServer clientToServer) {
    	return new AvroByteWriter<ClientToServer>(ClientToServer.class).toByteBuffer(clientToServer);
    }
    
    static class AvroByteWriter<T> {
    	private SpecificDatumWriter<T> datumWriter;

		public AvroByteWriter(Class<T> cl) {
			this.datumWriter = new SpecificDatumWriter<T>(cl);
		}
    	
    	ByteBuffer toByteBuffer(T data) {
    		ByteArrayOutputStream stream = new ByteArrayOutputStream();
        	BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
            try {
            	datumWriter.write(data, encoder);
                encoder.flush();
                return ByteBuffer.wrap(stream.toByteArray());
            } catch (IOException e) {
            	throw new RuntimeException("Exception while encoding", e);
    		}
    	}
    }
    
    public ServerToClient toServerToClientMessage(byte[] buf) {
    	return new AvroByteReader<ServerToClient>(ServerToClient.class).fromBytes(buf);
    }
    
    static class AvroByteReader<T> {
    	private SpecificDatumReader<T> datumReader;

		public AvroByteReader(Class<T> cl) {
			this.datumReader = new SpecificDatumReader<T>(cl);
		}
    	
    	T fromBytes(byte[] buf) {
        	BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buf, null);
            try {
            	return datumReader.read(null, decoder);
            } catch (IOException e) {
            	throw new RuntimeException("Exception while encoding", e);
    		}
    	}
    }

}
