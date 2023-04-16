package com.dk0124.cdr.pullapp.cron.upbit;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitTickRepository;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.pullapp.cron.UpbitCronBase;
import com.dk0124.cdr.pullapp.util.UpbitDocUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;


@Component
@Slf4j
public class UpbitTickUpbitCron extends UpbitCronBase<UpbitTickDoc, UpbitTickRepository> {
    public UpbitTickUpbitCron(ObjectMapper objectMapper, UpbitTickRepository repository) {
        super(objectMapper, repository, new UpbitTickDoc());
    }

    //@Scheduled(cron = "00 * * * * *")
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
        return UpbitDocUtil.generateTickIndex(doc);
    }

    @Override
    protected String generateId(UpbitTickDoc upbitTickDoc) {
        return UpbitDocUtil.generateTickId(upbitTickDoc);
    }
}
