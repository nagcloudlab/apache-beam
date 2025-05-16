package com.example.chapter2.event;


import com.example.chapter2.coder.UserEventCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(UserEventCoder.class)
public class UserEvent {
    public String userId;
    public String eventType;
    public long timestamp;

    public UserEvent() {
    }

    public UserEvent(String userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return userId + " - " + eventType + " @ " + timestamp;
    }
}
