package com.dk0124.cdr.pullapp.socket.handler;

import com.dk0124.cdr.es.dao.upbit.UpbitTickRepository;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.pullapp.socket.WebsocketClientHandlerBase;
import com.dk0124.cdr.pullapp.util.UpbitDocUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
@Slf4j
public class UpbitTickClientHandler extends WebsocketClientHandlerBase {
    private final UpbitTickRepository repository;

    private final ObjectMapper objectMapper;

    @Override
    public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
        String s = StandardCharsets.UTF_8.decode((ByteBuffer) message.getPayload()).toString();
        UpbitTickDoc tick = objectMapper.readValue(s, UpbitTickDoc.class);
        repository.index(UpbitDocUtil.generateTickIndex(tick), UpbitDocUtil.generateTickId(tick), tick);
    }
}
