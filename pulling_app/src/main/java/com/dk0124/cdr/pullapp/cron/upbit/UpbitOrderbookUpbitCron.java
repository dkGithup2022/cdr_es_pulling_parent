package com.dk0124.cdr.pullapp.cron.upbit;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.pullapp.cron.UpbitCronBase;
import com.dk0124.cdr.pullapp.util.UpbitDocUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;


@Component
public class UpbitOrderbookUpbitCron extends UpbitCronBase<UpbitOrderbookDoc, UpbitOrderbookRepository> {
    public UpbitOrderbookUpbitCron(ObjectMapper objectMapper, UpbitOrderbookRepository respository) {
        super(objectMapper, respository, new UpbitOrderbookDoc());
    }

    //@Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException {
        run();
    }

    @Override
    protected String getIndex(UpbitOrderbookDoc doc) {
        return UpbitDocUtil.generateOrderbookIndex(doc);
    }

    @Override
    protected String generateId(UpbitOrderbookDoc upbitOrderbookDoc) {
        return UpbitDocUtil.generateOrderbookId(upbitOrderbookDoc);
    }

    @Override
    protected String getApiUrl(UpbitCoinCode code, Long currentTimeStamp) {
        String baseUrl = Uri.UPBIT_REST_ORDERBOOK_URI.getAddress();
        String marketParam = "?markets=" + code.toString();
        return baseUrl + marketParam;
    }
}


