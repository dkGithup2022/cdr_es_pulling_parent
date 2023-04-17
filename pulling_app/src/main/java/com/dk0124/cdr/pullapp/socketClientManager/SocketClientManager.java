package com.dk0124.cdr.pullapp.socketClientManager;


import com.dk0124.cdr.constants.task.TaskType;
import com.dk0124.cdr.pullapp.socket.ClientBase;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
@Getter
public class SocketClientManager {

    private final Map<TaskType, ClientBase> clientMap = new ConcurrentHashMap<>();

    public ClientBase registerClient(ClientBase clientBase) {
        clientMap.put(clientBase.getTaskType(), clientBase);
        return clientBase;
    }

    public ClientBase getSession(TaskType taskType){ return clientMap.get(taskType);}

    public void closeConnection(TaskType taskType) throws IOException {
        clientMap.get(taskType).closeConnection();
    }
    public void openConnection(TaskType taskType) throws URISyntaxException {
        clientMap.get(taskType).startConnection();
    }

}