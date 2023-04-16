package com.dk0124.cdr.pullapp.cron.polling;

import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;

import java.util.List;

@Slf4j
public abstract class BithumbCronBase<T, R extends ElasticsearchRepository> {

    protected final ObjectMapper objectMapper;
    private final R respository;

    private final JavaType docType;
    protected String type;

    public BithumbCronBase(ObjectMapper objectMapper, R respository, T dummy) {
        this.objectMapper = objectMapper;
        this.respository = respository;
        this.docType = objectMapper.getTypeFactory().constructType(dummy.getClass());
    }

    public void run() throws InterruptedException, JsonProcessingException {
        Long currentTimeMillis = System.currentTimeMillis();
        for (BithumbCoinCode code : BithumbCoinCode.values()) {
            String url = getApiUrl(code, currentTimeMillis);
            ResponseEntity<String> response = reqApi(url, code);
            List<T> list = parseResponse(response, code);
            saveAll(list);

        }
    }


    protected void saveAll(List<T> list) {
        for (T doc : list) {
            respository.index(getIndex(doc), generateId(doc), doc);
        }
    }

    protected abstract String generateId(T doc);

    protected abstract String getIndex(T doc);

    protected abstract String getApiUrl(BithumbCoinCode code, Long currentTimeMillis);

    protected abstract ResponseEntity<String> reqApi(String url, BithumbCoinCode code) throws InterruptedException;

    protected abstract List<T> parseResponse(ResponseEntity<String> response, BithumbCoinCode code) throws JsonProcessingException;

}
