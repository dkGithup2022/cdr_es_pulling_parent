package com.dk0124.cdr.pullapp.cron;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.constants.vendor.VendorType;
import com.dk0124.cdr.es.dao.ElasticsearchRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;


@Component
@Slf4j
public class UpbitCandleCron extends UpbitCronBase<UpbitCandleDoc, UpbitCandleRepository> {
    public UpbitCandleCron(ObjectMapper objectMapper, UpbitCandleRepository respository) {
        super(objectMapper, respository, new UpbitCandleDoc());
        type = "candle";
    }

    @Scheduled(cron = "00 * * * * *")
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
        String UpbitCandlePrefix = "upbit_candle";
        if (UpbitCoinCode.fromString(doc.getMarket()) == null)
            throw new RuntimeException("INVALID CODE");
        String[] splitted = doc.getMarket().toLowerCase(Locale.ROOT).split("-");
        return UpbitCandlePrefix + "_" + String.join("_", splitted);
    }

    @Override
    protected String generateId(UpbitCandleDoc candle) {
        return candle.getMarket() + "_" + candle.getTimestamp();
    }


}
