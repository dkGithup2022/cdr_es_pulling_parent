package com.dk0124.cdr.pullapp.cron;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Component
@Slf4j
public class UpbitOrderbookCron extends UpbitCronBase<UpbitOrderbookDoc, UpbitOrderbookRepository> {

    public UpbitOrderbookCron(ObjectMapper objectMapper, UpbitOrderbookRepository respository, UpbitOrderbookDoc dummy) {
        super(objectMapper, respository, dummy);
        type = "orderbook";
    }

    @Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException {
        run();
    }

    @Override
    protected String getIndex(UpbitOrderbookDoc doc) {
        String UpbitCandlePrefix = "upbit_orderbook";
        if (UpbitCoinCode.fromString(doc.getCode()) == null)
            throw new RuntimeException("INVALID CODE");
        String[] splitted = doc.getCode().toLowerCase(Locale.ROOT).split("-");
        return UpbitCandlePrefix + "_" + String.join("_", splitted);
    }

    @Override
    protected String generateId(UpbitOrderbookDoc upbitOrderbookDoc) {
        return upbitOrderbookDoc.getCode().toLowerCase(Locale.ROOT) + "_" + upbitOrderbookDoc.getTimestamp();
    }

    @Override
    protected String getApiUrl(UpbitCoinCode code, Long currentTimeStamp) {
        String baseUrl = Uri.UPBIT_REST_ORDERBOOK_URI.getAddress();
        String marketParam = "?markets=" + code.toString();
        return baseUrl + marketParam;
    }
}
