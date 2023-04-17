package com.dk0124.cdr.pullapp.cron.polling;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;



@Slf4j
public abstract class UpbitCronBase<T, R extends ElasticsearchRepository> {
    protected final ObjectMapper objectMapper;
    private final R respository;

    private final JavaType docType;
    protected String type;

    public UpbitCronBase(ObjectMapper objectMapper, R respository, T dummy) {
        this.objectMapper = objectMapper;
        this.respository = respository;
        this.docType = objectMapper.getTypeFactory().constructType(dummy.getClass());
    }

    public void run() throws InterruptedException {
        Long currentTimeMillis = System.currentTimeMillis();
        for (UpbitCoinCode code : UpbitCoinCode.values()) {
            String url = getApiUrl(code, currentTimeMillis);
            List<T> list = reqApi(url);
            saveAll(list);
        }
    }

    public List<T> reqApi(String url) throws InterruptedException {
        RestTemplate restTemplate = new RestTemplate();
        // API request retry logic , 429 에러에 3번까지 재호출
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
                return parseResponse(response);
            } catch (HttpClientErrorException e) {
                if (e.getRawStatusCode() == 429) {
                    Thread.sleep(500);
                } else {
                    throw e;
                }
            } catch (JsonProcessingException e) {
                log.error("Invalid response body: {}", e.getMessage());
            }
        }

        return Collections.emptyList();
    }


    protected List<T> parseResponse(ResponseEntity<String> response) throws JsonProcessingException {
        return objectMapper.readValue(response.getBody(), objectMapper.getTypeFactory().constructCollectionType(List.class, docType));
    }



    public void saveAll(List<T> list) {
        for (T doc : list) {
            log.info(doc.toString());
            respository.index(getIndex(doc), generateId(doc), doc);
        }
    }


    abstract protected String getApiUrl(UpbitCoinCode code, Long currentTimeMillis);

    abstract protected String getIndex(T doc);

    abstract protected String generateId(T t);

}
