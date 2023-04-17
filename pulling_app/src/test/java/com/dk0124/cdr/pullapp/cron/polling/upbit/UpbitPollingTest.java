package com.dk0124.cdr.pullapp.cron.polling.upbit;

import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.dao.upbit.UpbitCandleRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitOrderbookRepository;
import com.dk0124.cdr.es.dao.upbit.UpbitTickRepository;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import com.dk0124.cdr.pullapp.cron.ElasticTestContainer;
import com.dk0124.cdr.pullapp.cron.EsIndexOps;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;


@Testcontainers
@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class UpbitPollingTest {

    @Autowired
    UpbitTickCron upbitTickCron;

    @Autowired
    UpbitOrderbookCron upbitOrderbookCron;

    @Autowired
    UpbitCandleCron upbitCandleCron;

    @Container
    private static ElasticTestContainer esContainer = new ElasticTestContainer();

    private static EsIndexOps esIndexOps = new EsIndexOps();

    @BeforeAll
    static void setUp() {
        esContainer.start();
    }

    static String[][] PAIR = new String[][]{{"bithumb", "candle"}, {"bithumb", "tick"}, {"bithumb", "orderbook"}, {"upbit", "candle"}, {"upbit", "tick"}, {"upbit", "orderbook"}};

    @BeforeAll
    public static void re_create_index() throws IOException {
        for (String[] pair : PAIR) {
            String vendor = pair[0];
            String type = pair[1];
            String sp = "elastic/" + vendor + "/" + type + "_setting.json";
            String mp = "elastic/" + vendor + "/" + type + "_mapping.json";
            String prefix = vendor + "_" + type + "_";

            for (UpbitCoinCode code : UpbitCoinCode.values()) {
                String[] splitted = code.toString().toLowerCase(Locale.ROOT).split("-");
                String idx = prefix + String.join("_", splitted);
                esIndexOps.deleteIndex(idx);
                esIndexOps.forceMergeAll();
                esIndexOps.createIndexWithMappingAndSetting(idx, mp, sp);
                esIndexOps.forceMerge(idx);
            }
        }
    }


    @Autowired
    UpbitCandleRepository candleRepository;

    @Autowired
    UpbitTickRepository tickRepository;

    @Autowired
    UpbitOrderbookRepository orderbookRepository;

    @Test
    @DisplayName("의존성 확인")
    public void empty() {
        assertNotNull(upbitTickCron);
        assertNotNull(upbitCandleCron);
        assertNotNull(upbitOrderbookCron);
    }

    @Test
    void testCronCandleJob() throws Exception {
        // Run the cron job manually
        UpbitCandleCron cron = upbitCandleCron;
        cron.run();

        // Wait for Elasticsearch indexing to complete
        Thread.sleep(3000);

        // Verify that documents were indexed
        Page<UpbitCandleDoc> page = candleRepository.findAll("upbit_candle_krw_btc", PageRequest.of(0, 1000));
        assertTrue(page.getContent().isEmpty(), "No documents were indexed");


        // Verify the contents of the documents
        for (UpbitCandleDoc candle : page.getContent()) {
            assertNotNull(candle.getMarket(), "Market is null");
            assertNotNull(candle.getTimestamp(), "Timestamp is null");
            assertNotNull(candle.getCandleDateTimeUtc(), "CandleDateTimeUtc is null");
            assertNotNull(candle.getCandleDateTimeKst(), "CandleDateTimeKst is null");
            assertNotNull(candle.getLowPrice(), "Low Price is null");
            assertNotNull(candle.getHighPrice(), "High Price is null");
        }
    }

    /***
     * 현재 tick api 호출 시 long 으로 오고 있음. DATELOCAL 로 변경 필요. 별도의 DTO 필요할 수도 있음 .
     */


    @Test
    void testCronTickJob() throws Exception {
        // Run the cron job manually
        UpbitTickCron cron = upbitTickCron;
        cron.run();

        // Wait for Elasticsearch indexing to complete
        Thread.sleep(3000);

        // Verify that documents were indexed
        Page<UpbitTickDoc> page = tickRepository.findAll("upbit_tick_krw_btc", PageRequest.of(0, 1000));
        assertFalse(page.getContent().isEmpty(), "No documents were indexed");


        // Verify the contents of the documents
        for (UpbitTickDoc tick : page.getContent()) {
            assertNotNull(tick.getCode(), "Code is null");
            assertNotNull(tick.getTimestamp(), "Timestamp is null");
            assertNotNull(tick.getSequentialId(), "seq id is null");
            assertNotNull(tick.getTradeVolume(), "Trade Volume is null");
            assertNotNull(tick.getTradePrice(), "Trade Price is null");
            assertNotNull(tick.getTradeDateUtc(), "Trade Date  is null");
        }
    }



    @Test
    void testCronOrderbookJob() throws Exception {
        // Run the cron job manually
        UpbitOrderbookCron cron = upbitOrderbookCron;
        cron.run();

        // Wait for Elasticsearch indexing to complete
        Thread.sleep(3000);

        // Verify that documents were indexed
        Page<UpbitOrderbookDoc> page = orderbookRepository.findAll("upbit_orderbook_krw_btc", PageRequest.of(0, 1000));
        assertTrue(page.getContent().isEmpty(), "No documents were indexed");


        // Verify the contents of the documents
        for (UpbitOrderbookDoc doc : page.getContent()) {
            assertNotNull(doc.getCode(), "Code is null");
            assertNotNull(doc.getTimestamp(), "Timestamp is null");
            assertNotNull(doc.getTotalAskSize(), "total ask size is null");
            assertNotNull(doc.getTotalBidSize(), "total bid size is null");
            assertNotNull(doc.getOrderBookUnits().get(0).getBidSize(), "unit bid size is null");
            assertNotNull(doc.getOrderBookUnits().get(0).getBidPrice(), "unit bid price is null");
        }
    }


}