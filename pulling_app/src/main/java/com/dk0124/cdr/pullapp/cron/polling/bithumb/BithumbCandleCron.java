package com.dk0124.cdr.pullapp.cron.polling.bithumb;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.bithumbCoinCode.BithumbCoinCode;
import com.dk0124.cdr.es.dao.bithumb.BithumbCandleRespository;
import com.dk0124.cdr.es.document.bithumb.BithumbCandleDoc;
import com.dk0124.cdr.pullapp.cron.polling.BithumbCronBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@Slf4j
@Component
public class BithumbCandleCron extends BithumbCronBase<BithumbCandleDoc, BithumbCandleRespository> {

    private final String BITHUMB_CANDLE_INDEX_PREFIX = "bithumb_candle";
    private final String TYPE = "candle";

    public BithumbCandleCron(ObjectMapper objectMapper, BithumbCandleRespository respository) {
        super(objectMapper, respository, new BithumbCandleDoc());
        type = TYPE;
    }

    @Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException, JsonProcessingException {
        run();
    }

    @Override
    protected String generateId(BithumbCandleDoc doc) {
        return doc.getCode() + "_" + doc.getTimestamp();
    }

    @Override
    protected String getIndex(BithumbCandleDoc doc) {
        String code = doc.getCode();
        if (BithumbCoinCode.fromString(code) == null)
            throw new RuntimeException("INVALID CODE");
        String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
        return BITHUMB_CANDLE_INDEX_PREFIX + "_" + String.join("_", splitted);
    }

    @Override
    protected String getApiUrl(BithumbCoinCode code, Long currentTimeMillis) {
        return Uri.BITHUMB_REST_CANDLE_URI.getAddress() + "/" + code.toString() + "/1m";
    }

    @Override
    protected ResponseEntity<String> reqApi(String url, BithumbCoinCode code) throws InterruptedException {
        RestTemplate restTemplate = new RestTemplate();
        // API request retry logic , 429 에러에 3번까지 재호출
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return restTemplate.getForEntity(url, String.class);
            } catch (HttpClientErrorException e) {
                if (e.getRawStatusCode() == 429) {
                    Thread.sleep(500);
                } else {
                    throw e;
                }
            }
        }
        throw new RuntimeException("429 3회 초과  , url : " + url);
    }

    @Override
    protected List<BithumbCandleDoc> parseResponse(ResponseEntity<String> response, BithumbCoinCode code) throws JsonProcessingException {
        BithumbRestCandleMessage brcm = objectMapper.readValue(response.getBody(), BithumbRestCandleMessage.class);
        Object[] candleData = brcm.getData();
        ArrayList<BithumbCandleDoc> bithumbCandles = new ArrayList<BithumbCandleDoc>();
        for (Object data : candleData) {
            ArrayList<Object> numbericData = (ArrayList<Object>) data;
            BithumbCandleDoc c = new BithumbCandleDoc(
                    code.toString()
                    , Long.valueOf(String.valueOf(numbericData.get(0)))
                    , Double.valueOf(String.valueOf(numbericData.get(1)))
                    , Double.valueOf(String.valueOf(numbericData.get(2)))
                    , Double.valueOf(String.valueOf(numbericData.get(3)))
                    , Double.valueOf(String.valueOf(numbericData.get(4)))
                    , Double.valueOf(String.valueOf(numbericData.get(5)))
            );
            bithumbCandles.add(c);
        }
        return bithumbCandles;
    }


    @Getter
    @Setter
    @AllArgsConstructor
    private static class BithumbRestCandleMessage {

        @JsonProperty("status")
        private String status;

        @JsonProperty("data")
        private Object[] data;

    }
}
