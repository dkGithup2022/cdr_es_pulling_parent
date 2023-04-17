package com.dk0124.cdr.pullapp.util;

import com.dk0124.cdr.constants.Uri;
import com.dk0124.cdr.constants.coinCode.UpbitCoinCode.UpbitCoinCode;
import com.dk0124.cdr.es.document.upbit.UpbitCandleDoc;
import com.dk0124.cdr.es.document.upbit.UpbitOrderbookDoc;
import com.dk0124.cdr.es.document.upbit.UpbitTickDoc;
import org.springframework.stereotype.Component;

import java.util.Locale;

public class UpbitDocUtil {

    private static final String TICK_INDEX_PREFIX = "upbit_tick";
    private static final String CANDLE_INDEX_PREFIX = "upbit_candle";
    private static final String ORDERBOOK_INDEX_PREFIX = "upbit_orderbook";

    // 틱 인덱스 생성
    public static String generateTickIndex(UpbitTickDoc tickDoc) {
        if (UpbitCoinCode.fromString(tickDoc.getCode().toUpperCase(Locale.ROOT)) == null)
            throw new IllegalArgumentException("Invalid market: " +  tickDoc.toString());

        String[] splitted =  tickDoc.getCode().toLowerCase(Locale.ROOT).split("-");
        return TICK_INDEX_PREFIX + "_" + String.join("_", splitted);
    }

    // 틱 ID 생성
    public static String generateTickId(UpbitTickDoc tickDoc) {
        String code = tickDoc.getCode().toLowerCase(Locale.ROOT);
        long timestamp = tickDoc.getTimestamp();
        return code + "_" + timestamp;
    }

    // 캔들 인덱스 생성
    public static String generateCandleIndex(UpbitCandleDoc candleDoc) {
        if (UpbitCoinCode.fromString(candleDoc.getMarket().toUpperCase(Locale.ROOT)) == null)
            throw new IllegalArgumentException("Invalid market: " + candleDoc.toString());

        String[] splitted = candleDoc.getMarket().toLowerCase(Locale.ROOT).split("-");
        return CANDLE_INDEX_PREFIX + "_" + String.join("_", splitted);
    }

    // 캔들 ID 생성
    public static String generateCandleId(UpbitCandleDoc candleDoc) {
        String market = candleDoc.getMarket().toLowerCase(Locale.ROOT);
        long timestamp = candleDoc.getTimestamp();
        return market + "_" + timestamp;
    }

    // 오더북 인덱스 생성
    public static String generateOrderbookIndex(UpbitOrderbookDoc orderbookDoc) {

        if (UpbitCoinCode.fromString(orderbookDoc.getCode().toUpperCase(Locale.ROOT)) == null)
            throw new IllegalArgumentException("Invalid market: " +  orderbookDoc.toString());

        String[] splitted =  orderbookDoc.getCode().toLowerCase(Locale.ROOT).split("-");
        return ORDERBOOK_INDEX_PREFIX + "_" + String.join("_", splitted);
    }

    // 오더북 ID 생성
    public static String generateOrderbookId(UpbitOrderbookDoc orderbookDoc) {
        String code = orderbookDoc.getCode().toLowerCase(Locale.ROOT);
        long timestamp = orderbookDoc.getTimestamp();
        return code + "_" + timestamp;
    }
}