package com.dk0124.cdr.pullapp.cron.websocket;


import com.dk0124.cdr.constants.task.TaskType;
import com.dk0124.cdr.pullapp.socket.ClientBase;
import com.dk0124.cdr.pullapp.socketClientManager.SocketClientManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.net.URISyntaxException;

@Component
@RequiredArgsConstructor
@Slf4j
public class RestartSocket {
    private final SocketClientManager manager;
    private static final int MAX_RETRY_COUNT = 3;

    @Scheduled(cron = "00 * * * * * ")
    public void restartSocket() throws IOException, InterruptedException, URISyntaxException {
        TaskType[] tasks = TaskType.values();
        log.info("RECONNECT SOCKETS ");
        for (TaskType task : tasks) {
            if (!manager.getClientMap().containsKey(task))
                continue;
            manager.getSession(task).closeConnection();
            Thread.sleep(1000);
            connectWithRetry(manager.getSession(task), 0);
        }
    }

    /**
     * @param client
     * @param retryCount
     * @throws URISyntaxException
     * @throws InterruptedException
     *
     * 429 에러 발생 시, 2초 대기 후 재시도. ( 될 때까지 )
     * 429 이외 에러 발생시, 2초 대기 후 재시도 ( 최대 3번 )
     */

    private void connectWithRetry(ClientBase client, int retryCount) throws URISyntaxException, InterruptedException {
        try {
            client.startConnection();
        } catch (HttpClientErrorException e) {
            if (e.getRawStatusCode() == 429 && retryCount < MAX_RETRY_COUNT) {
                log.error("Failed to connect to socket client (429 TOO MANY REQUESTS), retry attempt #{} \n{}", retryCount + 1, e);
                Thread.sleep(2000);
                connectWithRetry(client, retryCount + 1);
            } else if (retryCount < MAX_RETRY_COUNT) {
                log.error("Failed to connect to socket client, retry attempt #{} \n{}", retryCount + 1, e);
                Thread.sleep(2000);
                connectWithRetry(client, retryCount + 1);
            } else {
                throw new RuntimeException("Could not connect after maximum number of retries", e);
            }
        } catch (Exception e) {
            log.error("Failed to connect to socket client, unknown error \n{}", e);
            throw new RuntimeException("Could not connect after maximum number of retries", e);
        }
    }
}
