package com.dk0124.cdr.pullapp.socket;

import com.dk0124.cdr.constants.coinCode.CoinCode;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.constants.task.TaskType;
import com.dk0124.cdr.constants.vendor.VendorType;
import com.dk0124.cdr.pullapp.socket.pubsub.SocketClientErrorSubscriber;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dk0124.cdr.constants.vendor.VendorType.BITHUMB;
import static com.dk0124.cdr.constants.vendor.VendorType.UPBIT;

@Slf4j
public abstract class ClientBase implements SocketClientErrorSubscriber {

    private final TaskType taskType;
    private final VendorType vendorType;
    private final List<CoinCode> codes;
    private WebsocketClientHandlerBase webSocketHandler;
    private WebSocketSession clientSession;

    public ClientBase(TaskType taskType, VendorType vendorType, WebsocketClientHandlerBase webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
        this.taskType = taskType;
        this.vendorType = vendorType;

        if (vendorType == BITHUMB)
            codes = Stream.of(BithumbCoinCode.values()).collect(Collectors.toList());
        else if (vendorType == UPBIT)
            codes = Stream.of(UpbitCoinCode.values()).collect(Collectors.toList());
        else
            throw new RuntimeException("INVALID COIN CODE WHILE INITIALIZING SOCKET CLIENT : " + vendorType);

    }

    public void closeConnection() throws IOException {
        if (clientSession != null && clientSession.isOpen())
            clientSession.close();
    }

    public void startConnection() throws URISyntaxException {
        registerHandler();
        ListenableFuture<WebSocketSession> listenableFuture = openNewClientSession();
        configureConnection(listenableFuture);
    }

    public WebSocketSession getClientSession() {
        return this.clientSession;
    }

    public TaskType getTaskType() {
        return this.taskType;
    }

    private void registerHandler() {
        this.webSocketHandler.setSubscriber(this);
    }

    private ListenableFuture<WebSocketSession> openNewClientSession() throws URISyntaxException {
        return new StandardWebSocketClient().doHandshake(webSocketHandler, null, createUri());
    }

    private void configureConnection(ListenableFuture<WebSocketSession> listenableFuture) {
        listenableFuture.addCallback(result -> {
            clientSession = result;
            sendInitialMessage(result);
        }, ex -> {
            log.error("socket connection fail : {}", ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        });
    }

    private void sendInitialMessage(WebSocketSession session) {
        try {
            log.info("send initial message : {}", getSocketClientInitialMessage().getPayload());
            session.sendMessage(getSocketClientInitialMessage());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private TextMessage getSocketClientInitialMessage() {
        String payload = "";
        for (CoinCode coin : codes) {
            payload += "\"" + coin.toString() + "\",";
        }
        payload = payload.substring(0, payload.length() - 1);

        switch (taskType) {
            case UPBIT_TICK:
                return SocketClientResources.getUpbitTickMessage(payload);
            case UPBIT_ORDERBOOK:
                return SocketClientResources.getUpbitOrderbookMessage(payload);
            case BITHUMB_TICK:
                return SocketClientResources.getBithumbTickMessage(payload);
            case BITHUMB_ORDERBOOK:
                return SocketClientResources.getBithumbOrderbookMessage(payload);
            default:
                throw new RuntimeException("invalid taskType");
        }
    }

    private URI createUri() throws URISyntaxException {
        switch (vendorType) {
            case UPBIT:
                return SocketClientResources.getUpbitSocketURI();
            case BITHUMB:
                return SocketClientResources.getBithumbSocketURI();
            default:
                throw new RuntimeException("invalid vendor");
        }
    }
}

