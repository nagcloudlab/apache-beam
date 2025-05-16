package day8.coder;

import day8.event.UserEvent;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public  class UserEventCoder extends CustomCoder<UserEvent> {
    @Override
    public void encode(UserEvent event, OutputStream outStream) throws IOException {
        writeString(outStream, event.userId);
        writeString(outStream, event.eventType);
        VarInt.encode(event.timestamp, outStream);
    }

    @Override
    public UserEvent decode(InputStream inStream) throws IOException {
        String userId = readString(inStream);
        String eventType = readString(inStream);
        long timestamp = VarInt.decodeLong(inStream);
        return new UserEvent(userId, eventType, timestamp);
    }

    private void writeString(OutputStream out, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        VarInt.encode(bytes.length, out);
        out.write(bytes);
    }

    private String readString(InputStream in) throws IOException {
        int len = VarInt.decodeInt(in);
        byte[] bytes = new byte[len];
        in.read(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
