package com.dk0124.cdr.pullapp.cron.polling.upbit;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.pullapp.cron.polling.UpbitCronBase;
import com.dk0124.cdr.pullapp.util.UpbitDocUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;


@Component
@Slf4j
public class UpbitCandleUpbitCron extends UpbitCronBase<UpbitCandleDoc, UpbitCandleRepository> {
    public UpbitCandleUpbitCron(ObjectMapper objectMapper, UpbitCandleRepository respository) {
        super(objectMapper, respository, new UpbitCandleDoc());
    }

    //@Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException {
        run();
    }

    @Override
    protected String getApiUrl(UpbitCoinCode code, Long currentTimeMillis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String baseUrl = Uri.UPBIT_REST_CANDLE_MINUTES_URI.getAddress();
        String marketParam = "?market=" + code.toString();
        String to = "&to=" + sdf.format(new Date(currentTimeMillis));
        String suffix = "&count=10";
        return baseUrl + marketParam + to + suffix;
    }

    @Override
    protected String getIndex(UpbitCandleDoc doc) {
        return UpbitDocUtil.generateCandleIndex(doc);
    }

    @Override
    protected String generateId(UpbitCandleDoc doc) {
        return UpbitDocUtil.generateCandleId(doc);
    }

}
