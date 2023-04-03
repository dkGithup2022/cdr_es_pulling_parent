package com.dk0124.cdr.pullapp.cron;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.vendor.VendorType;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;


@Component
@Slf4j
public abstract class UpbitCronBase<T, R extends ElasticsearchRepository> {
    protected final ObjectMapper objectMapper;
    private final R respository;

    private final JavaType docType;
    private final JavaType docListType;

    protected final String vendor = VendorType.UPBIT.name;
    protected String type;


    public UpbitCronBase(ObjectMapper objectMapper, R respository, T dummy) {
        this.objectMapper = objectMapper;
        this.respository = respository;

        //Generic to JavaType
        this.docType = objectMapper.getTypeFactory().constructType(dummy.getClass());
        this.docListType = objectMapper.getTypeFactory().
                constructCollectionType(
                        ArrayList.class,
                        dummy.getClass());
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

        // api 요청 최대 3회 // 429 에러 대응
        List<T> list = null;
        for (int i = 0; i < 3; i++) { // 최대 3번 재시도
            try {
                ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
                list = objectMapper.readValue(response.getBody(), objectMapper.getTypeFactory().constructCollectionType(List.class, docType));
                break;
            } catch (HttpClientErrorException e) {
                if (e.getRawStatusCode() == 429) {
                    Thread.sleep(500);
                } else {
                    throw e;
                }
            } catch (JsonProcessingException e) {
                log.error("Invalid res body: JsonProcessingException ");
                log.error(e.getMessage());
            }
        }

        return list;
    }

    public void saveAll(List<T> list) {
        for (T doc : list) {
            respository.index(getIndex(doc), generateId(doc), doc);
        }
    }


    abstract protected String getApiUrl(UpbitCoinCode code, Long currentTimeMillis);

    abstract protected String getIndex(T doc);

    abstract protected String generateId(T t);

}
