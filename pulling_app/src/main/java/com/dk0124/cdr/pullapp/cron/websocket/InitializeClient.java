package com.dk0124.cdr.pullapp.cron.websocket;

import com.dk0124.cdr.constants.task.TaskType;
import com.dk0124.cdr.constants.vendor.VendorType;
import com.dk0124.cdr.es.dao.upbit.UpbitTickRepository;
import com.dk0124.cdr.pullapp.socket.client.UpbitTickSocketClient;
import com.dk0124.cdr.pullapp.socket.handler.UpbitTickClientHandler;
import com.dk0124.cdr.pullapp.socketClientManager.SocketClientManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.URISyntaxException;

@Component
@RequiredArgsConstructor
public class InitializeClient implements ApplicationRunner {
    private final SocketClientManager manager;
    private final UpbitTickRepository upbitTickRepository;
    private final ObjectMapper objectMapper;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        initializeUpbitTickSocket();
    }

    public void initializeUpbitTickSocket() throws URISyntaxException {
        UpbitTickClientHandler handler = new UpbitTickClientHandler(upbitTickRepository, objectMapper);
        UpbitTickSocketClient client = new UpbitTickSocketClient(TaskType.UPBIT_TICK, VendorType.UPBIT, handler);
        client.startConnection();
        manager.registerClient(client);
    }

}
