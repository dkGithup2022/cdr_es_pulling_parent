package com.dk0124.cdr.pullapp.socket.client;

import com.dk0124.cdr.constants.task.TaskType;
import com.dk0124.cdr.constants.vendor.VendorType;
import com.dk0124.cdr.pullapp.socket.ClientBase;
import com.dk0124.cdr.pullapp.socket.WebsocketClientHandlerBase;


public class UpbitTickSocketClient extends ClientBase {

    public UpbitTickSocketClient(TaskType taskType, VendorType vendorType, WebsocketClientHandlerBase webSocketHandler) {
        super(taskType, vendorType, webSocketHandler);
    }

    @Override
    public void notified(String key) {

    }
}
