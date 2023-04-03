package com.dk0124.cdr.pullapp.cron.upbit;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitTickRespository;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;


@Component
@Slf4j
public class UpbitTickCron extends UpbitCronBase<UpbitTickDoc, UpbitTickRespository> {
    private final String UPBIT_TICK_INDEX_PREFIX = "upbit_tick";
    private final String TYPE = "tick";

    public UpbitTickCron(ObjectMapper objectMapper, UpbitTickRespository repository) {
        super(objectMapper, repository, new UpbitTickDoc());
        type = TYPE;
    }

    @Scheduled(cron = "00 */2 * * * *")
    public void cron() throws InterruptedException {
        run();
    }


    @Override
    protected String getApiUrl(UpbitCoinCode code, Long currentTimeMillis) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        String baseUrl = Uri.UPBIT_REST_TICK_URI.getAddress();
        String marketParam = "?market=" + code.toString();
        String to = "&to=" + sdf.format(new Date(currentTimeMillis));
        String cnt = "&count=1000";
        return baseUrl + marketParam + to + cnt;
    }

    @Override
    protected String getIndex(UpbitTickDoc doc) {
        String UpbitCandlePrefix = UPBIT_TICK_INDEX_PREFIX;
        if (UpbitCoinCode.fromString(doc.getCode()) == null)
            throw new RuntimeException("INVALID CODE");
        String[] splitted = doc.getCode().toLowerCase(Locale.ROOT).split("-");
        return UpbitCandlePrefix + "_" + String.join("_", splitted);

    }

    @Override
    protected String generateId(UpbitTickDoc upbitTickDoc) {
        return upbitTickDoc.getCode().toLowerCase(Locale.ROOT) + "_" + upbitTickDoc.getTimestamp();
    }
}
