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
        String prefix = TICK_INDEX_PREFIX;
        String code = tickDoc.getCode().toLowerCase(Locale.ROOT);
        if (UpbitCoinCode.fromString(code) == null) {
            throw new IllegalArgumentException("Invalid code: " + code);
        }
        String[] splitted = code.split("-");
        return prefix + "_" + String.join("_", splitted);
    }

    // 틱 ID 생성
    public static String generateTickId(UpbitTickDoc tickDoc) {
        String code = tickDoc.getCode().toLowerCase(Locale.ROOT);
        long timestamp = tickDoc.getTimestamp();
        return code + "_" + timestamp;
    }

    // 캔들 인덱스 생성
    public static String generateCandleIndex(UpbitCandleDoc candleDoc) {
        String prefix = CANDLE_INDEX_PREFIX;
        String market = candleDoc.getMarket().toLowerCase(Locale.ROOT);
        if (UpbitCoinCode.fromString(market) == null) {
            throw new IllegalArgumentException("Invalid market: " + market);
        }
        String[] splitted = market.split("-");
        return prefix + "_" + String.join("_", splitted);
    }

    // 캔들 ID 생성
    public static String generateCandleId(UpbitCandleDoc candleDoc) {
        String market = candleDoc.getMarket().toLowerCase(Locale.ROOT);
        long timestamp = candleDoc.getTimestamp();
        return market + "_" + timestamp;
    }

    // 오더북 인덱스 생성
    public static String generateOrderbookIndex(UpbitOrderbookDoc orderbookDoc) {
        String prefix = ORDERBOOK_INDEX_PREFIX;
        String code = orderbookDoc.getCode().toLowerCase(Locale.ROOT);
        if (UpbitCoinCode.fromString(code) == null) {
            throw new IllegalArgumentException("Invalid code: " + code);
        }
        String[] splitted = code.split("-");
        return prefix + "_" + String.join("_", splitted);
    }

    // 오더북 ID 생성
    public static String generateOrderbookId(UpbitOrderbookDoc orderbookDoc) {
        String code = orderbookDoc.getCode().toLowerCase(Locale.ROOT);
        long timestamp = orderbookDoc.getTimestamp();
        return code + "_" + timestamp;
    }
}