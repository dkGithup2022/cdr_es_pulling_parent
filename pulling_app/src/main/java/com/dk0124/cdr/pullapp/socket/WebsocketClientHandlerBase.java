package com.dk0124.cdr.pullapp.socket;


import com.dk0124.cdr.pullapp.socket.pubsub.SocketClientErrorPublisher;
import org.springframework.web.socket.WebSocketHandler;

public abstract class WebsocketClientBase implements WebSocketHandler , SocketClientErrorPublisher {
}
