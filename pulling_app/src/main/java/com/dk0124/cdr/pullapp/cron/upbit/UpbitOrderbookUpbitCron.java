package com.dk0124.cdr.pullapp.cron.upbit;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.pullapp.cron.UpbitCronBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Locale;


@Component
public class UpbitOrderbookUpbitCron extends UpbitCronBase<UpbitOrderbookDoc, UpbitOrderbookRepository> {
    private final String UPBIT_ORDERBOOK_INDEX_PREFIX = "upbit_orderbook";
    private final String TYPE = "orderbook";

    public UpbitOrderbookUpbitCron(ObjectMapper objectMapper, UpbitOrderbookRepository respository) {
        super(objectMapper, respository, new UpbitOrderbookDoc());
        type = TYPE;
    }

    //@Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException {
        run();
    }


    @Override
    protected String getIndex(UpbitOrderbookDoc doc) {

        if (UpbitCoinCode.fromString(doc.getCode()) == null)
            throw new RuntimeException("INVALID CODE");
        String[] splitted = doc.getCode().toLowerCase(Locale.ROOT).split("-");
        return UPBIT_ORDERBOOK_INDEX_PREFIX + "_" + String.join("_", splitted);
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


