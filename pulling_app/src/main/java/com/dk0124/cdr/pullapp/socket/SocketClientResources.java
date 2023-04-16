package com.dk0124.cdr.pullapp.socket;

import com.dk0124.cdr.constants.Uri;
import org.springframework.web.socket.TextMessage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

public class ConnectionResourceUtil {
    public static TextMessage getUpbitTickMessage(String payload) {
        return new TextMessage(
                "[{\"ticket\":\"give_me_ticks_" + UUID.randomUUID() + "\"}," +
                        "{\"type\":\"trade\"," +
                        "\"codes\":[" + payload + "]}]"
        );
    }

    public static TextMessage getUpbitOrderbookMessage(String payload) {
        return new TextMessage(
                "[{\"ticket\":\"give_me_orderbook_" + UUID.randomUUID() + "\"}," +
                        "{\"type\":\"orderbook\"," +
                        "\"codes\":[" + payload + "]}]"
        );
    }

    public static TextMessage getBithumbTickMessage(String payload) {
        return new TextMessage(
                "{\"type\":\"transaction\"," +
                        "\"symbols\":[" + payload + "]}"
        );
    }

    public static TextMessage getBithumbOrderbookMessage(String payload) {
        return new TextMessage(
                "{\"type\":\"orderbookdepth\"," +
                        "\"symbols\":[" + payload + "]}"
        );
    }

    public static URI getUpbitSocketURI() throws URISyntaxException {
        return new URI(Uri.UPBIT_SOCKET_URI.getAddress());
    }

    public static URI getBithumbSocketURI() throws URISyntaxException {
        return new URI(Uri.BITHUMB_SOCKET_URI.getAddress());
    }
}
