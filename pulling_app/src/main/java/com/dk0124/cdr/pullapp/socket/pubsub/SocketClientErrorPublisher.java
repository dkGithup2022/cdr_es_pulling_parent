package com.dk0124.cdr.pullapp.socket.pubsub;

public interface SocketClientErrorPublisher {
    void setSubscriber(SocketClientErrorSubscriber subscriber);
    void notifySubscriber(String key);
}
