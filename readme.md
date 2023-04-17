
## Pulling server

long pulling 으로 가상화폐  거래 내역, 가격 데이터 수집.  

</br>

---

### 수집 데이터 
아래의 코인에 대해 tick, orderbooks, candle 데이터 수집.

- tick : 거래 내역 전부 수집
- orderbook : 주기 1 분
- candle : 주기 1 분 

1. Upbit
```
    KRW_BTC("KRW-BTC"),
    KRW_XRP("KRW-XRP"),
    KRW_ETH("KRW-ETH"),
    KRW_STX("KRW-STX"),
    KRW_SOL("KRW-SOL"),
    KRW_ADA("KRW-ADA"),
    KRW_DOT("KRW-DOT"),
    KRW_BCH("KRW-BCH"),
    KRW_BAT("KRW-BAT"),
    KRW_AVAX("KRW-AVAX"),
    KRW_ETC("KRW-ETC"),
    KRW_AXS("KRW-AXS"),
    KRW_PLA("KRW-PLA"),
    KRW_SAND("KRW-SAND"),
    KRW_SRM("KRW-SRM"),
    KRW_DOGE("KRW-DOGE"),
    KRW_MANA("KRW-MANA"),
    KRW_FLOW("KRW-FLOW"),
    KRW_BTG("KRW-BTG"),
    KRW_ATOM("KRW-ATOM"),
    KRW_MATIC("KRW-MATIC"),
    KRW_ENJ("KRW-ENJ"),
    KRW_CHZ("KRW-CHZ");
```

</br>

2. Bithumb

```
    KRW_BTC("BTC_KRW"),
    KRW_XRP("XRP_KRW"),
    KRW_ETH("ETH_KRW"),
    KRW_SOL("SOL_KRW"),
    KRW_ADA("ADA_KRW"),
    KRW_DOT("DOT_KRW"),
    KRW_BCH("BCH_KRW"),
    KRW_BAT("BAT_KRW"),
    KRW_AVAX("AVAX_KRW"),
    KRW_ETC("ETC_KRW"),
    KRW_AXS("AXS_KRW"),
    KRW_PLA("PLA_KRW"),
    KRW_SAND("SAND_KRW"),
    KRW_SRM("SRM_KRW"),
    KRW_DOGE("DOGE_KRW"),
    KRW_MANA("MANA_KRW"),
    KRW_BTG("BTG_KRW"),
    KRW_ATOM("ATOM_KRW"),
    KRW_MATIC("MATIC_KRW"),
    KRW_ENJ("ENJ_KRW"),
    KRW_CHZ("CHZ_KRW");
```

</br>

---

### Cron 

데이터 수집은 Cron으로 수집함, spring boot 의 @Scheduled 어노테이션을 사용한다.


</br>

1. 구현체 코드  : UpbitTickCron

```java
    @Scheduled(cron = "00 * * * * *")
    public void cron() throws InterruptedException {
        run();
    }
```


</br>


2. cron 추상 클래스 

```java
@Slf4j
public abstract class UpbitCronBase<T, R extends ElasticsearchRepository> {
    protected final ObjectMapper objectMapper;
    private final R respository;

    private final JavaType docType;
    protected String type;

    public UpbitCronBase(ObjectMapper objectMapper, R respository, T dummy) {
        this.objectMapper = objectMapper;
        this.respository = respository;
        this.docType = objectMapper.getTypeFactory().constructType(dummy.getClass());
    }

    public void run() throws InterruptedException {
        Long currentTimeMillis = System.currentTimeMillis();
        for (UpbitCoinCode code : UpbitCoinCode.values()) {
            String url = getApiUrl(code, currentTimeMillis);
            List<T> list = reqApi(url);
            saveAll(list);
        }
    }

    public List<T> reqApi(String url) throws InterruptedException {
        RestTemplate restTemplate = new RestTemplate();

        // API request retry logic , 429 에러에 3번까지 재호출
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
                return objectMapper.readValue(response.getBody(), objectMapper.getTypeFactory().constructCollectionType(List.class, docType));
            } catch (HttpClientErrorException e) {
                if (e.getRawStatusCode() == 429) {
                    Thread.sleep(500);
                } else {
                    throw e;
                }
            } catch (JsonProcessingException e) {
                log.error("Invalid response body: {}", e.getMessage());
            }
        }

        return Collections.emptyList();
    }



    public void saveAll(List<T> list) {
        for (T doc : list) {
            respository.index(getIndex(doc), generateId(doc), doc);
        }
    }


    abstract protected String getApiUrl(UpbitCoinCode code, Long currentTimeMillis);

    abstract protected String getIndex(T doc);

    abstract protected String generateId(T t);

}

```


</br>


---


### Socket client base 

```java


@Slf4j
public abstract class ClientBase implements SocketClientErrorSubscriber {

    private final TaskType taskType;
    private final VendorType vendorType;
    private final List<CoinCode> codes;
    private WebsocketClientHandlerBase webSocketHandler;
    private WebSocketSession clientSession;

    public ClientBase(TaskType taskType, VendorType vendorType, WebsocketClientHandlerBase webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
        this.taskType = taskType;
        this.vendorType = vendorType;

        if (vendorType == BITHUMB)
            codes = Stream.of(BithumbCoinCode.values()).collect(Collectors.toList());
        else if (vendorType == UPBIT)
            codes = Stream.of(UpbitCoinCode.values()).collect(Collectors.toList());
        else
            throw new RuntimeException("INVALID COIN CODE WHILE INITIALIZING SOCKET CLIENT : " + vendorType);

    }

    public void closeConnection() throws IOException {
        if (clientSession != null && clientSession.isOpen())
            clientSession.close();
    }

    public void startConnection() throws URISyntaxException {
        registerHandler();
        ListenableFuture<WebSocketSession> listenableFuture = openNewClientSession();
        configureConnection(listenableFuture);
    }

    public WebSocketSession getClientSession() {
        return this.clientSession;
    }

    public TaskType getTaskType() {
        return this.taskType;
    }

    private void registerHandler() {
        this.webSocketHandler.setSubscriber(this);
    }

    private ListenableFuture<WebSocketSession> openNewClientSession() throws URISyntaxException {
        return new StandardWebSocketClient().doHandshake(webSocketHandler, null, createUri());
    }

    private void configureConnection(ListenableFuture<WebSocketSession> listenableFuture) {
        listenableFuture.addCallback(result -> {
            clientSession = result;
            sendInitialMessage(result);
        }, ex -> {
            log.error("socket connection fail : {}", ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        });
    }
    .....





```



</br>


---

### 테스트 

통합 테스트만 실행. 

- 환경 : TestContainer elasticsearch 7.9.1
- 인덱스 정보 : /resources/elastic/

코드 

```java
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
```

</br>

---

### 앞으로 남은 것들

1. 현재 통합 테스트 중, 도큐먼트 인덱싱이 바로 되지 않아 테스트 결과가 올바르지 않게 나오는 경우가 있음 .

-  resources/elastic/settings 의 세팅을 바꿔도  testcontianer의 es 가 너무 느려서 제때 동작하지 않음 .

- 현재는 settings 에서 refresh interval 을 immediate 로 변경 후. timeout 을 길게 두고 진행함.

</br>

2. cdr_elasticsearch 에 bulk insert 기능 넣고 크론에 쓰는  함수 바꾸기 .

</br> 

3. upbit tradetimeutc -> LocalDate 로 변경 하기 .





