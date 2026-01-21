package com.kabu.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.*;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TrendScreener {

    // ===== 可调参数 =====
    static final double MIN_PRICE = 500.0;                   // 最低股价(円)
    static final double MIN_AVG_TURNOVER_JPY = 5e8;          // 旧阈值（保留但不再用于硬卡）
    static final double MAX_DIST_52W = 0.20;                 // 距52周高≤20%
    static final double MAX_DIST_52W_EARLY = 0.35; // 早期趋势：允许离高点稍远一些
    static final double BREAKOUT_VOL_MULTIPLIER = 1.5;       // 放量突破阈值
    static final long   RATE_LIMIT_MS = 200;                 // （旧）单线程节流，已被并发节流替代
    static final int    MAX_RETRY = 4;                       // 429/5xx 重试次数
    static final ZoneId JP = ZoneId.of("Asia/Tokyo");

    // ===== 趋势过滤参数 =====
    static final int ADX_PERIOD = 14;
    static final double ADX_TREND_MIN  = 25.0; // 确认趋势所需的最小ADX（软约束方案里仍可参考）

    static final int MACD_FAST = 12, MACD_SLOW = 26, MACD_SIGNAL = 9;

    static final int MA200_SLOPE_LOOKBACK = 10; // 200日线向上判定的回看天数

    // ===== 成交量/OBV 过滤参数 =====
    static final int VOL_SMA_FAST = 5, VOL_SMA_SLOW = 20;
    static final double VOL_FAST_UP_MIN_GROWTH = 1.02;   // 5日均量相较3日前至少上涨2%
    static final int VOL_FAST_UP_LOOKBACK = 3;

    static final int OBV_DIVERGENCE_LOOKBACK = 60;       // 检测近N日顶背离
    static final double OBV_DIVERGENCE_TOL = 0.003;      // OBV新高容差（0.3%）

    static final int BREAKOUT_LOOKBACK = 20;             // 突破N日新高
    static final double BREAKOUT_VOL_MULT = 1.5;         // 突破日量能≥1.5×20日均量
    static final int BREAKOUT_HOLD_DAYS = 2;             // 突破后N日内量能不应断崖式萎缩
    static final double VOL_NOT_COLLAPSE = 0.75;         // 断崖阈值：不低于 0.75×20日均量

    // “价升量增/价跌量缩”判据
    static final double RISE_DAY_VOL_FLOOR = 0.90;       // 上涨日成交量≥0.9×20日均量
    static final double FALL_DAY_VOL_CAP = 1.20;         // 下跌日成交量≤1.2×20日均量

    // ===== 流动性参数（新）=====
    static final double LIQ_TURN_MED20_MIN = 2e8;   // 20日成交额“中位数”≥2亿
    static final double LIQ_TURN_AVG20_MIN = 3e8;   // 或 20日均值 ≥3亿
    static final double LIQ_TODAY_PUMP_MULT = 3.0;  // 或 当日成交额 ≥ 中位数的3倍（放量豁免）

    // =========== 新增：趋势模式开关 ===========
    enum TrendMode { HYBRID, MINERVINI, ADX_ONLY }
    static final TrendMode TREND_MODE = TrendMode.HYBRID; // 可切换：HYBRID / MINERVINI / ADX_ONLY

    // 是否严格要求 MACD（建议先设 false 做回归）
    static final boolean REQUIRE_MACD = false;




    // Excel 文件路径
    static final String EXCEL_FILE = "202310.xlsx";
    static final String OUTPUT_CSV = "candidates";

    // JPX 证券代码列表 CSV（未使用时可忽略）
    static final String JPX_CSV_URL = System.getenv().getOrDefault(
            "JPX_CSV_URL",
            "file://./jpx_list.csv"
    );

    static final CookieManager cookieManager =
            new CookieManager(null, CookiePolicy.ACCEPT_ALL);

    static final OkHttpClient http = new OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .callTimeout(20, TimeUnit.SECONDS)
            .build();
    static final String UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    + "(KHTML, like Gecko) Chrome/123.0 Safari/537.36";
    static final ObjectMapper om = new ObjectMapper();
    static long lastCallAt = 0; // 已由并发节流取代，保留兼容

    static volatile boolean USE_QUERY2 = true;

    // ====== Yahoo 并发与节流（新）======
    static final int  YH_MAX_PARALLEL = 6;     // 并发上限（4~8 较稳）
    static final long YH_MIN_GAP_MS   = 150L;  // 任意两次请求的最小全局间隔
    static final java.util.concurrent.Semaphore YH_PERMITS =
            new java.util.concurrent.Semaphore(YH_MAX_PARALLEL);
    static final java.util.concurrent.atomic.AtomicLong YH_LAST_CALL_AT =
            new java.util.concurrent.atomic.AtomicLong(0L);
    static final java.util.concurrent.atomic.AtomicBoolean YH_SESSION_READY =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    // 严格度：STRICT（最严） / NORMAL（默认） / LOOSE（宽松，便于排查管线）
    enum Strictness { STRICT, NORMAL, LOOSE }

    static Strictness STRICTNESS = Strictness.LOOSE;  // 先用 LOOSE 验证能否筛出结果


    static String yahooBase() {
        return USE_QUERY2 ? "https://query2.finance.yahoo.com" : "https://query1.finance.yahoo.com";
    }

    static synchronized void ensureYahooSession() {
        try {
            Request warm = new Request.Builder()
                    .url("https://finance.yahoo.com/quote/7203.T")
                    .header("User-Agent", UA)
                    .build();
            try (Response r = http.newCall(warm).execute()) { /* ignore body */ }
        } catch (IOException ignored) {
        }
    }

    static void ensureYahooSessionOnce() throws Exception {
        if (YH_SESSION_READY.get()) return;
        synchronized (YH_SESSION_READY) {
            if (YH_SESSION_READY.get()) return;
            ensureYahooSession(); // 幂等预热
            YH_SESSION_READY.set(true);
        }
    }

    static void throttleYahoo() throws InterruptedException {
        while (true) {
            long prev = YH_LAST_CALL_AT.get();
            long now = System.currentTimeMillis();
            long wait = YH_MIN_GAP_MS - (now - prev);
            if (wait > 0) { Thread.sleep(Math.min(wait, YH_MIN_GAP_MS)); continue; }
            if (YH_LAST_CALL_AT.compareAndSet(prev, now)) return;
        }
    }

    // ========== Filter Profiling ==========
    enum Reason { TREND, VOL, TURNOVER, FLOW, PATTERN, RS, RISK }
    static final Map<Reason, Integer> DROP = new java.util.EnumMap<>(Reason.class);
    static final java.util.List<String> EX_FAIL = new java.util.ArrayList<>();

    public static void main(String[] args) throws Exception {
        // 1) 从 Excel 文件加载股票代码
        List<String> universe = loadUniverseFromExcel();
        universe = universe.stream().map(TrendScreener::ensureTokyoSymbol).collect(Collectors.toList());
        System.out.println("从 Excel 加载股票数量: " + universe.size());

        // 2) 预筛（并发）：价格/流动性快速过滤
        List<String> tickers = prefilterUsingStooq(universe);
        System.out.println("预筛选后剩余: " + tickers.size() + " 支股票");

        // 3) 并发跑全市场
        java.util.List<CsvRow> rows = scanAllParallel(tickers);

        rows.sort(Comparator
                .comparing((CsvRow r) -> r.signal.isEmpty())
                .thenComparingDouble(r -> r.dist52w));
        List<CsvRow> signaled = rows.stream()
                .filter(r -> r.signal != null && !r.signal.isEmpty()&&!r.signal.equals("SETUP")) // 只要有信号
                .collect(Collectors.toList());
        int topN = Math.min(50, signaled.size());

//        GptRanker.rankOnlySignaled(signaled, topN);
//        try {
//
//            System.out.println("===== GPT 排名 Top " + ranked.size() + " =====");
//            for (GptRanker.Ranked r : ranked) {
//                System.out.printf(Locale.ROOT,
//                        "#%d %-8s score=%.1f  entry[%.2f~%.2f]  stop=%.2f  %s%n",
//                        r.rank, r.symbol, r.score, r.entryLow, r.entryHigh, r.stop, r.reason);
//            }
//            System.out.println("写出: " + GptRanker.RANKED_TSV.toAbsolutePath());
//        } catch (Exception e) {
//            System.err.println("GPT 排名失败: " + e.getMessage());
//        }
        writeCsv(rows, OUTPUT_CSV);
        System.out.println("完成 -> " + OUTPUT_CSV + " (总行数=" + rows.size() + ")");
    }

    // ===== 从 Excel 读取股票代码 =====
    static List<String> loadUniverseFromExcel() {
        final int CODE_COL  = 1;
        final int NAME_COL  = 2;
        final int MKT_COL   = 3;
        final int TOPIX_COL = 9;

        List<String> symbols = new ArrayList<>();
        int total = 0, kept = 0;
        int dropNon4 = 0, dropNotDomestic = 0, dropEtfReit = 0, dropNoTopix = 0;

        try (InputStream is = TrendScreener.class.getClassLoader().getResourceAsStream(EXCEL_FILE)) {
            if (is == null) {
                System.err.println("资源 202310.xls 未找到（请放到 src/main/resources/ 下）");
                return Arrays.asList("5830","1893","8616","5079","6857","5334","5832","8306","6855","5851","8593","1926","8058","6590","4507");
            }
            try (Workbook wb = WorkbookFactory.create(is)) {
                Sheet sheet = wb.getSheetAt(0);

                for (int i = 1; i <= sheet.getLastRowNum(); i++) { // 跳过表头
                    Row row = sheet.getRow(i);
                    if (row == null) continue;
                    total++;

                    String code = getCellAsString(row.getCell(CODE_COL)).trim();
                    String name = getCellAsString(row.getCell(NAME_COL)).trim();
                    String mkt  = getCellAsString(row.getCell(MKT_COL)).trim();
                    String topix= getCellAsString(row.getCell(TOPIX_COL)).trim();

//                    if (!code.matches("\\d{4}")) { dropNon4++; continue; }
                    if (!mkt.contains("内国株式")) { dropNotDomestic++; continue; }

                    String nLower = name.toLowerCase(Locale.ROOT);
                    if (nLower.contains("etf") || name.contains("上場投信") || name.contains("投資法人") || name.contains("リート")
                            || nLower.contains("reit") || name.contains("優先") || name.contains("受益証券")) {
                        dropEtfReit++; continue;
                    }

                    //if (topix.isEmpty() || "-".equals(topix)) { dropNoTopix++; continue; }

                    symbols.add(code);
                    kept++;
                }
            }

            symbols = symbols.stream().distinct().sorted().collect(Collectors.toList());

            System.out.printf(
                    Locale.ROOT,
                    "Excel 总行=%d, 选中=%d, 丢弃: 非4位=%d, 非内国株=%d, ETF/REIT等=%d, 无TOPIX=%d%n",
                    total, kept, dropNon4, dropNotDomestic, dropEtfReit, dropNoTopix
            );

        } catch (Exception e) {
            System.err.println("读取 Excel 文件出错: " + e.getMessage());
            e.printStackTrace();
            return Arrays.asList("6758","8035","7203","9432","9984","6861","4502");
        }
        return symbols;
    }

    private static String getCellAsString(Cell cell) {
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING:  return cell.getStringCellValue();
            case NUMERIC: return String.valueOf((int)cell.getNumericCellValue());
            case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
            default:      return "";
        }
    }


    static Chart fetchChart1yDaily(String symbol) throws Exception {
        try { return fetchChartFromYahoo(symbol); }
        catch (IOException e) { return fetchChartFromStooq(symbol); }
    }

    // ===== 并发预筛（价格+流动性粗筛）=====
    static List<String> prefilterUsingStooq(List<String> symbols) throws IOException, InterruptedException {
        java.util.List<String> keep = Collections.synchronizedList(new ArrayList<>());

        int threads = Math.min(YH_MAX_PARALLEL, Math.max(2, Runtime.getRuntime().availableProcessors()));
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CompletionService<Void> ecs = new java.util.concurrent.ExecutorCompletionService<>(pool);

        for (String raw : symbols) {
            ecs.submit(() -> {
                String sym = ensureTokyoSymbol(raw);
                try {
                    Chart c = fetchChartFromYahoo(sym);
                    if (c.bars.size() < 30) return null;

                    Bar last = c.bars.get(c.bars.size() - 1);
                    if (last.close < MIN_PRICE) return null;

                    // 流动性（快速口径）：20日成交额中位/均值 + 放量豁免
                    double[] last20 = lastNDailyTurnovers(c.bars, 20);
                    double med20 = median(last20);
                    double avg20 = mean(last20);
                    double today = (last20.length > 0 ? last20[last20.length - 1] : 0.0);

                    boolean liquid = (med20 >= LIQ_TURN_MED20_MIN)
                            || (avg20 >= LIQ_TURN_AVG20_MIN)
                            || (med20 > 0 && today >= LIQ_TODAY_PUMP_MULT * med20);

                    if (liquid) keep.add(sym);
                } catch (Exception e) {
                    System.err.println("预筛选跳过 " + sym + " : " + e.getMessage());
                }
                return null;
            });
        }

        for (int i = 0; i < symbols.size(); i++) {
            try { ecs.take().get(); } catch (Exception ignored) {}
        }
        pool.shutdown();
        return keep;
    }

    static Chart fetchChartFromStooq(String symbol) throws Exception {
        String code = symbol.toLowerCase(Locale.ROOT).replace(".t", ".jp");
        String url  = "https://stooq.com/q/d/l/?s=" + code + "&i=d";

        Request req = new Request.Builder()
                .url(url)
                .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                        + "(KHTML, like Gecko) Chrome/120 Safari/537.36")
                .header("Accept", "text/csv,*/*;q=0.8")
                .header("Accept-Language", "ja,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7")
                .header("Referer", "https://stooq.com/q/a/?s=" + code + "&i=d")
                .build();

        try (Response r = http.newCall(req).execute()) {
            if (!r.isSuccessful()) {
                throw new IOException("stooq HTTP " + r.code());
            }

            byte[] buf = r.body() != null ? r.body().bytes() : new byte[0];
            int bytes = buf.length;
            System.out.printf("stooq GET %-12s -> %3d, bytes=%6d%n", code, r.code(), bytes);

            if (bytes == 0) throw new IOException("stooq empty (zero bytes)");

            String csv = new String(buf, java.nio.charset.StandardCharsets.UTF_8).trim();

            if (!csv.startsWith("Date") || csv.indexOf('\n') < 0 || csv.split("\\R").length <= 1) {
                throw new IOException("stooq empty (header only)");
            }

            Chart c = new Chart(); c.symbol = symbol;
            String[] lines = csv.split("\\R");
            for (int i = 1; i < lines.length; i++) {
                String[] f = lines[i].split(",");
                if (f.length < 6) continue;
                long ts = java.time.LocalDate.parse(f[0])
                        .atStartOfDay(java.time.ZoneId.of("Asia/Tokyo")).toEpochSecond();
                double o = parseD(f[1]), h = parseD(f[2]), l = parseD(f[3]), cl = parseD(f[4]);
                long v = (long) parseD(f[5]);
                if (!Double.isNaN(cl)) c.bars.add(new Bar(ts, o, h, l, cl, v));
            }
            if (c.bars.isEmpty()) throw new IOException("stooq empty (no bars)");
            return c;
        } finally {
            Thread.sleep(220 + ThreadLocalRandom.current().nextInt(120));
        }
    }

    static double parseD(String s){
        try{ return Double.parseDouble(s);}catch(Exception e){return Double.NaN;}
    }

    static Chart fetchChartFromYahoo(String symbol) throws IOException, InterruptedException {
        String url = yahooBase() + "/v8/finance/chart/" + symbol + "?range=1y&interval=1d";
        JsonNode root = getJson(url, symbol);
        JsonNode err = root.path("chart").path("error");
        if (!err.isMissingNode() && !err.isNull()) {
            throw new IOException("chart error");
        }
        JsonNode res = root.path("chart").path("result").get(0);
        JsonNode ts = res.path("timestamp");
        JsonNode q = res.path("indicators").path("quote").get(0);

        Chart c = new Chart();
        c.symbol = symbol;
        for (int i = 0; i < ts.size(); i++) {
            long t = ts.get(i).asLong();
            double o = getNum(q, "open", i);
            double h = getNum(q, "high", i);
            double l = getNum(q, "low", i);
            double cl = getNum(q, "close", i);
            long v = (long) getNum(q, "volume", i);
            if (!Double.isNaN(cl) && v >= 0) {
                c.bars.add(new Bar(t, o, h, l, cl, v));
            }
        }
        return c;
    }

    // ===== 并发安全的 GET JSON（全局节流+重试）=====
    static JsonNode getJson(String url, String symbol) throws IOException, InterruptedException {
        IOException last = null;
        try { ensureYahooSessionOnce(); } catch (Exception e) { throw new IOException(e); }

        for (int i = 0; i <= MAX_RETRY; i++) {
            YH_PERMITS.acquire();
            try {
                throttleYahoo();

                Request req = new Request.Builder()
                        .url(url)
                        .header("User-Agent", UA)
                        .header("Accept", "application/json")
                        .header("Accept-Language", "en-US,en;q=0.9,ja;q=0.8")
                        .header("Referer", "https://finance.yahoo.com/")
                        .build();

                try (Response resp = http.newCall(req).execute()) {
                    int code = resp.code();
                    byte[] buf = (resp.body() != null) ? resp.body().bytes() : new byte[0];
                    System.out.printf("yahoo GET %-8s -> %3d, bytes=%d%n", symbol, code, buf.length);

                    if (code == 200) {
                        if (buf.length == 0) throw new IOException("empty body");
                        String head = new String(buf, 0, Math.min(buf.length, 64), java.nio.charset.StandardCharsets.UTF_8);
                        if (head.startsWith("<") || head.startsWith("<!--")) {
                            cookieManager.getCookieStore().removeAll();
                            USE_QUERY2 = !USE_QUERY2;
                            ensureYahooSession();
                            Thread.sleep(800L * (i + 1));
                            url = url.replace("https://query1.finance.yahoo.com", yahooBase())
                                    .replace("https://query2.finance.yahoo.com", yahooBase());
                            continue;
                        }
                        return om.readTree(buf);
                    }

                    if (code == 401) {
                        cookieManager.getCookieStore().removeAll();
                        USE_QUERY2 = !USE_QUERY2;
                        ensureYahooSession();
                        Thread.sleep(1200L * (i + 1));
                        url = url.replace("https://query1.finance.yahoo.com", yahooBase())
                                .replace("https://query2.finance.yahoo.com", yahooBase());
                        continue;
                    }

                    if (code == 429 || code >= 500) {
                        long backoff = (long)Math.pow(2, i) * 500L + ThreadLocalRandom.current().nextInt(400);
                        Thread.sleep(backoff);
                        continue;
                    }

                    throw new IOException("HTTP " + code);
                } catch (IOException e) {
                    last = e;
                }
            } finally {
                YH_PERMITS.release();
            }
        }
        throw (last != null ? last : new IOException("request failed"));
    }

    // ===== 计算 =====
    static double sma(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd; // -1=最后
        int start = end - n + 1;
        if (start < 0) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = start; i <= end; i++) {
            sum += bars.get(i).close;
        }
        return sum / n;
    }

    static double avgVol(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd;
        int start = end - n + 1;
        if (start < 0) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = start; i <= end; i++) {
            sum += bars.get(i).volume;
        }
        return sum / n;
    }

    static double avgTurnover(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd;
        int start = end - n + 1;
        if (start < 0) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = start; i <= end; i++) {
            sum += bars.get(i).close * bars.get(i).volume;
        }
        return sum / n;
    }

    static double rollingHigh(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd;
        int start = Math.max(0, end - n + 1);
        double hi = Double.NEGATIVE_INFINITY;
        for (int i = start; i <= end; i++) {
            hi = Math.max(hi, bars.get(i).high);
        }
        return hi;
    }

    // 近L内最近两个顶点
    static int[] lastTwoSwingHighs(double[] arr, int lookback) {
        int n = arr.length, last = n - 1, start = Math.max(1, n - lookback);
        List<Integer> pivots = new ArrayList<>();
        for (int i = start; i < last; i++) {
            if (arr[i] > arr[i - 1] && arr[i] >= arr[i + 1]) pivots.add(i);
        }
        if (pivots.size() < 2) {
            int max1 = -1, max2 = -1;
            for (int i = Math.max(0, n - lookback); i <= last; i++) {
                if (max1 == -1 || arr[i] >= arr[max1]) { max2 = max1; max1 = i; }
                else if (max2 == -1 || arr[i] > arr[max2]) { max2 = i; }
            }
            if (max1 != -1 && max2 != -1) {
                int a = Math.min(max1, max2), b = Math.max(max1, max2);
                return new int[]{a, b};
            }
        }
        if (pivots.size() >= 2) {
            int a = pivots.get(pivots.size() - 2);
            int b = pivots.get(pivots.size() - 1);
            return new int[]{a, b};
        }
        return new int[]{-1, -1};
    }

    // OBV 顶背离
    static boolean hasBearishObvDivergence(List<Bar> bars, double[] obv, int lookback, double tol) {
        int n = bars.size(), last = n - 1;
        if (n < Math.max(30, lookback)) return false;

        double[] close = new double[n];
        for (int i = 0; i < n; i++) close[i] = bars.get(i).close;

        int[] piv = lastTwoSwingHighs(close, lookback);
        int i1 = piv[0], i2 = piv[1];
        if (i1 < 0 || i2 < 0 || i2 <= i1) return false;

        boolean priceHigherHigh = close[i2] > close[i1] * (1.0 + 1e-6);
        boolean obvNotHigher    = obv[i2] <= obv[i1] * (1.0 + tol);
        return priceHigherHigh && obvNotHigher;
    }

    static int recentBreakoutIndex(List<Bar> bars, int lookback) {
        int n = bars.size(), last = n - 1;
        for (int i = Math.max(1, n - lookback); i <= last; i++) {
            double priorHigh = Double.NEGATIVE_INFINITY;
            int priorStart = Math.max(0, i - lookback);
            for (int k = priorStart; k < i; k++) priorHigh = Math.max(priorHigh, bars.get(k).high);
            if (bars.get(i).close > priorHigh * 1.0001) return i;
        }
        return -1;
    }

    static boolean priceVolumeHarmony(List<Bar> bars) {
        int n = bars.size(), last = n - 1;
        double vma20 = smaVolume(bars, VOL_SMA_SLOW, -1);

        if (bars.get(last).close > bars.get(last - 1).close) {
            if (!(bars.get(last).volume >= vma20 * RISE_DAY_VOL_FLOOR
                    || bars.get(last).volume >= bars.get(last - 1).volume)) return false;
        } else if (bars.get(last).close < bars.get(last - 1).close) {
            if (!(bars.get(last).volume <= vma20 * FALL_DAY_VOL_CAP
                    || bars.get(last).volume <= bars.get(last - 1).volume)) return false;
        }

        if (last >= 2) {
            double vma20Prev = smaVolume(bars, VOL_SMA_SLOW, -2);
            if (bars.get(last - 1).close > bars.get(last - 2).close) {
                if (!(bars.get(last - 1).volume >= vma20Prev * (RISE_DAY_VOL_FLOOR - 0.05))) return false;
            } else if (bars.get(last - 1).close < bars.get(last - 2).close) {
                if (!(bars.get(last - 1).volume <= vma20Prev * (FALL_DAY_VOL_CAP + 0.05))) return false;
            }
        }
        return true;
    }

    // 量能/OBV过滤
    static boolean passesVolumeFilters(List<Bar> bars) {
        int n = bars.size(), last = n - 1;
        if (n < Math.max(60, BREAKOUT_LOOKBACK + 5)) return false;

        double vma5  = smaVolume(bars, VOL_SMA_FAST, -1);
        double vma20 = smaVolume(bars, VOL_SMA_SLOW, -1);
        double vma5Ago = smaVolume(bars, VOL_SMA_FAST, -1 - VOL_FAST_UP_LOOKBACK);
        boolean volTrend = !Double.isNaN(vma5) && !Double.isNaN(vma20) && vma5 > vma20
                && !Double.isNaN(vma5Ago) && vma5 >= vma5Ago * VOL_FAST_UP_MIN_GROWTH;
        if (!volTrend) return false;

        double[] obvArr = obv(bars);
        if (hasBearishObvDivergence(bars, obvArr, OBV_DIVERGENCE_LOOKBACK, OBV_DIVERGENCE_TOL)) {
            return false;
        }

        int bIdx = recentBreakoutIndex(bars, BREAKOUT_LOOKBACK);
        if (bIdx >= 0) {
            double vma20AtB = smaVolume(bars, VOL_SMA_SLOW, bIdx - (bars.size()));
            if (Double.isNaN(vma20AtB)) vma20AtB = vma20;

            boolean breakoutVolumeOk = bars.get(bIdx).volume >= vma20AtB * BREAKOUT_VOL_MULT;
            if (!breakoutVolumeOk) return false;

            for (int i = bIdx; i <= Math.min(last, bIdx + BREAKOUT_HOLD_DAYS); i++) {
                double vma20i = smaVolume(bars, VOL_SMA_SLOW, i - n);
                if (Double.isNaN(vma20i)) vma20i = vma20;
                if (bars.get(i).volume < vma20i * VOL_NOT_COLLAPSE) return false;
            }
        }

        if (!priceVolumeHarmony(bars)) return false;

        return true;
    }

    // 工具
    static String ensureTokyoSymbol(String raw) {
        String s = raw.trim();
        if (s.contains(".")) {
            return s;
        } else {
            return s + ".T";
        }
    }

    static double getNum(JsonNode q, String field, int i) {
        JsonNode arr = q.path(field);
        if (arr == null || arr.isMissingNode() || i >= arr.size()) {
            return Double.NaN;
        }
        JsonNode v = arr.get(i);
        if (v == null || v.isNull()) {
            return Double.NaN;
        } else {
            return v.asDouble();
        }
    }

    static String fmtDate(long epochSec) {
        return Instant.ofEpochSecond(epochSec).atZone(JP)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    static void writeCsv(List<CsvRow> rows, String path) throws IOException {
        path=path+"_"+ LocalDate.now()+".csv";
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(path), "UTF-8"))) {
            pw.println("Symbol,Name,Date,Close,SMA10,SMA20,SMA50,52WHigh,Dist52W,VolMA20,TurnoverMA20,Trigger20H,TodayVol,Signal");
            for (CsvRow r : rows) {
                pw.printf(Locale.US,
                        "%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.3f,%.0f,%.0f,%.2f,%.0f,%s%n",
                        r.symbol, csvSafe(r.name), r.date, r.close, r.sma10, r.sma20, r.sma50,
                        r.high52w, r.dist52w, r.volMA20, r.turnMA20, r.trigger20h, r.todayVol, r.signal);
            }
        }
    }

    static String csvSafe(String s) {
        if (s == null) {
            return "";
        } else {
            return s.replace(",", " ");
        }
    }



    static double[] wilderAvg(double[] x, int period) {
        int n = x.length;
        double[] out = new double[n];
        java.util.Arrays.fill(out, Double.NaN);
        if (n <= period) return out;

        double sum = 0.0; int cnt = 0;
        for (int i = 1; i <= period; i++) {
            if (!Double.isNaN(x[i])) { sum += x[i]; cnt++; }
        }
        if (cnt == period) out[period] = sum / period;

        for (int i = period + 1; i < n; i++) {
            if (!Double.isNaN(x[i]) && !Double.isNaN(out[i - 1])) {
                out[i] = (out[i - 1] * (period - 1) + x[i]) / period;
            } else {
                out[i] = out[i - 1];
            }
        }
        return out;
    }

    static AdxResult adx(List<Bar> bars, int period) {
        int n = bars.size();
        double[] tr = new double[n];
        double[] plusDM = new double[n];
        double[] minusDM = new double[n];

        for (int i = 1; i < n; i++) {
            double up   = bars.get(i).high - bars.get(i - 1).high;
            double down = bars.get(i - 1).low - bars.get(i).low;
            plusDM[i]  = (up   > down && up   > 0) ? up   : 0.0;
            minusDM[i] = (down > up   && down > 0) ? down : 0.0;

            double h_l  = bars.get(i).high - bars.get(i).low;
            double h_pc = Math.abs(bars.get(i).high - bars.get(i - 1).close);
            double l_pc = Math.abs(bars.get(i).low  - bars.get(i - 1).close);
            tr[i] = Math.max(h_l, Math.max(h_pc, l_pc));
        }

        double[] atr      = wilderAvg(tr,      period);
        double[] plusDMma = wilderAvg(plusDM,  period);
        double[] minusDMma= wilderAvg(minusDM, period);

        double[] diPlus  = new double[n];
        double[] diMinus = new double[n];
        double[] dx      = new double[n];
        java.util.Arrays.fill(diPlus,  Double.NaN);
        java.util.Arrays.fill(diMinus, Double.NaN);
        java.util.Arrays.fill(dx,      Double.NaN);

        for (int i = 0; i < n; i++) {
            if (!Double.isNaN(atr[i]) && atr[i] > 0) {
                diPlus[i]  = 100.0 * (plusDMma[i]  / atr[i]);
                diMinus[i] = 100.0 * (minusDMma[i] / atr[i]);
                double sum = diPlus[i] + diMinus[i];
                if (sum > 0) dx[i] = 100.0 * Math.abs(diPlus[i] - diMinus[i]) / sum;
            }
        }

        double[] adx = new double[n];
        java.util.Arrays.fill(adx, Double.NaN);

        int seedEnd = 2 * period - 1;
        if (n > seedEnd) {
            double s = 0.0; int cnt = 0;
            for (int i = period; i <= seedEnd; i++) {
                if (!Double.isNaN(dx[i])) { s += dx[i]; cnt++; }
            }
            if (cnt == period) {
                adx[seedEnd] = s / period;
                for (int i = seedEnd + 1; i < n; i++) {
                    if (!Double.isNaN(dx[i])) {
                        adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period;
                    } else {
                        adx[i] = adx[i - 1];
                    }
                }
            }
        }

        AdxResult r = new AdxResult();
        r.adx = adx; r.diPlus = diPlus; r.diMinus = diMinus;
        return r;
    }

    static double[] ema(double[] arr, int period) {
        int n = arr.length;
        double[] out = new double[n];
        for (int i = 0; i < n; i++) out[i] = Double.NaN;
        if (n < period) return out;

        double sum = 0.0;
        for (int i = 0; i < period; i++) sum += arr[i];
        out[period - 1] = sum / period;

        double k = 2.0 / (period + 1.0);
        for (int i = period; i < n; i++) {
            out[i] = out[i - 1] + k * (arr[i] - out[i - 1]);
        }
        return out;
    }

    static MacdResult macd(List<Bar> bars, int fast, int slow, int signal) {
        int n = bars.size();
        double[] close = new double[n];
        for (int i = 0; i < n; i++) close[i] = bars.get(i).close;

        double[] emaFast = ema(close, fast);
        double[] emaSlow = ema(close, slow);

        double[] macd = new double[n];
        for (int i = 0; i < n; i++) {
            macd[i] = (Double.isNaN(emaFast[i]) || Double.isNaN(emaSlow[i])) ? Double.NaN : (emaFast[i] - emaSlow[i]);
        }

        double[] signalArr = ema(macd, signal);
        double[] hist = new double[n];
        for (int i = 0; i < n; i++) {
            hist[i] = (Double.isNaN(macd[i]) || Double.isNaN(signalArr[i])) ? Double.NaN : (macd[i] - signalArr[i]);
        }

        MacdResult r = new MacdResult();
        r.macd = macd; r.signal = signalArr; r.hist = hist;
        return r;
    }

    // ===== 趋势过滤（含 macdOk 且带严格度开关）=====
    static boolean passesTrendFilters(List<Bar> bars) {
        int need = Math.max(200, ADX_PERIOD + MACD_SLOW + MACD_SIGNAL + 5);
        if (bars.size() < need) return false;

        int last = bars.size() - 1;
        double price = bars.get(last).close;

        // 均线：允许 3/4 堆叠
        double ma5   = sma(bars,   5, -1);
        double ma20  = sma(bars,  20, -1);
        double ma60  = sma(bars,  60, -1);
        double ma120 = sma(bars, 120, -1);
        double ma200 = sma(bars, 200, -1);
        double ma200Prev = sma(bars, 200, -1 - MA200_SLOPE_LOOKBACK);

        int stackCnt = 0;
        if (price > ma5)  stackCnt++;
        if (ma5 > ma20)   stackCnt++;
        if (ma20 > ma60)  stackCnt++;
        if (ma60 > ma120) stackCnt++;
        boolean stacked     = (stackCnt == 4);
        boolean stacked3of4 = (stackCnt >= 3);

        boolean above200 = price > ma200;
        boolean ma200Up  = !Double.isNaN(ma200Prev) && ma200 > ma200Prev;

        // ADX / DI
        AdxResult a = adx(bars, ADX_PERIOD);
        double lastAdx     = a.adx[a.adx.length - 1];
        double lastDiPlus  = a.diPlus[a.diPlus.length - 1];
        double lastDiMinus = a.diMinus[a.diMinus.length - 1];
        boolean diOk       = lastDiPlus > lastDiMinus;
        boolean adxStrong  = isFinite(lastAdx) && lastAdx >= 25.0;     // Wilder常用阈值
        double  adxSlope5  = linregSlope(a.adx, 5);
        boolean trendOk    = adxStrong || (adxSlope5 > 0 && diOk);

        // 备选模板：无需 MACD
        if (TREND_MODE == TrendMode.MINERVINI) {
            boolean pass = minerviniTemplate(bars);
            System.out.printf(Locale.ROOT,
                    "DBG SYMBOL: mode=MINERVINI stacked=%b 3of4=%b above200=%b ma200Up=%b adx=%.1f diOk=%b pass=%b%n",
                    stacked, stacked3of4, above200, ma200Up, lastAdx, diOk, pass);
            return pass;
        }
        if (TREND_MODE == TrendMode.ADX_ONLY) {
            boolean pass = (stacked3of4 && above200 && (ma200Up || diOk) && trendOk);
            System.out.printf(Locale.ROOT,
                    "DBG SYMBOL: mode=ADX_ONLY stacked=%b 3of4=%b above200=%b ma200Up=%b adx=%.1f diOk=%b pass=%b%n",
                    stacked, stacked3of4, above200, ma200Up, lastAdx, diOk, pass);
            return pass;
        }

        // HYBRID：MACD 用宽松版；强趋势下 MACD 可选
        MacdResult m = macd(bars, MACD_FAST, MACD_SLOW, MACD_SIGNAL);
        boolean macdPass = macdLooseRelax(m);
        boolean macdRequired = REQUIRE_MACD && trendOk; // 只在你显式要求时才硬性卡 MACD

        boolean passStrict   = stacked && above200 && ma200Up && trendOk && (macdRequired ? macdPass : true);
        boolean passFallback = stacked3of4 && above200 && (ma200Up || diOk) && (macdRequired ? macdPass : true);

        // 调试输出（你日志里那行就是下面这句）
        double h0 = m.hist[m.hist.length - 1];
        double h1 = m.hist[m.hist.length - 2];
        System.out.printf(Locale.ROOT,
                "DBG SYMBOL: stacked=%b 3of4=%b above200=%b ma200Up=%b adxStrong=%.1f diOk=%b macdLoose=%b h0=%.6f h1=%.6f%n",
                stacked, stacked3of4, above200, ma200Up, (isFinite(lastAdx)?lastAdx:Double.NaN), diOk, macdPass,
                (isFinite(h0)?h0:Double.NaN), (isFinite(h1)?h1:Double.NaN));

        return passStrict || passFallback;
    }


    // 仅用最近 L 根有效(非NaN)的 |hist| 均值的 0.25 倍作为“近零带”
    static double nearZeroBand(double[] hist, int L) {
        int last = hist.length - 1;
        int i = last, used = 0;
        double sumAbs = 0.0;
        while (i >= 0 && used < L) {
            if (isFinite(hist[i])) { sumAbs += Math.abs(hist[i]); used++; }
            i--;
        }
        double band = (used == 0 ? 0.0 : (sumAbs / used) * 0.25);
        return Math.max(1e-4, band);
    }

    // 忽略 NaN 的线性回归斜率（至少2个有效点才计算）
    static double linregSlope(double[] arr, int N) {
        int last = arr.length - 1;
        int start = Math.max(0, last - (N - 1));
        double sx=0, sy=0, sxx=0, sxy=0; int n=0;
        for (int k = start; k <= last; k++) {
            if (!isFinite(arr[k])) continue;
            double x = n; // 紧密重编号
            double y = arr[k];
            sx += x; sy += y; sxx += x*x; sxy += x*y; n++;
        }
        if (n < 2) return 0.0;
        double den = n * sxx - sx * sx;
        return den == 0 ? 0.0 : (n * sxy - sx * sy) / den;
    }

    // 忽略 NaN：近 N 根里至少 U 次抬头（arr[i] >= arr[i-1]）
    static boolean upInKofN(double[] arr, int U, int N) {
        int last = arr.length - 1;
        if (last < 1) return false;
        int checked = 0, upCnt = 0;
        for (int i = last; i > 0 && checked < N; i--) {
            if (isFinite(arr[i]) && isFinite(arr[i-1])) {
                if (arr[i] >= arr[i-1]) upCnt++;
                checked++;
            }
        }
        if (checked == 0) return false;
        return upCnt >= U;
    }

    // 简单数组 SMA（保证 end 有效且有 p 个可用值）
    static double smaArr(double[] arr, int p, int end) {
        int cnt = 0; double sum = 0.0;
        for (int i = end; i >= 0 && cnt < p; i--) {
            if (isFinite(arr[i])) { sum += arr[i]; cnt++; }
        }
        return (cnt == p) ? (sum / p) : Double.NaN;
    }

    // 可选：有没有“金叉”
    static boolean macdCrossUp(double[] macd, double[] signal) {
        int last = macd.length - 1;
        if (last < 1 || !isFinite(macd[last]) || !isFinite(signal[last]) ||
                !isFinite(macd[last-1]) || !isFinite(signal[last-1])) return false;
        return (macd[last-1] < signal[last-1]) && (macd[last] >= signal[last]);
    }

    static double smaVolume(List<Bar> bars, int period, int endOffset) {
        int n = bars.size();
        int end = n + endOffset;
        if (end < 0) return Double.NaN;
        if (end >= n) end = n - 1;
        int start = end - period + 1;
        if (start < 0) return Double.NaN;
        double sum = 0.0;
        for (int i = start; i <= end; i++) {
            sum += bars.get(i).volume;
        }
        return sum / period;
    }

    static double[] obv(List<Bar> bars) {
        int n = bars.size();
        double[] out = new double[n];
        if (n == 0) return out;
        out[0] = 0.0;
        for (int i = 1; i < n; i++) {
            if (bars.get(i).close > bars.get(i - 1).close) {
                out[i] = out[i - 1] + bars.get(i).volume;
            } else if (bars.get(i).close < bars.get(i - 1).close) {
                out[i] = out[i - 1] - bars.get(i).volume;
            } else {
                out[i] = out[i - 1];
            }
        }
        return out;
    }



    static double macdEps(double price) {
        return Math.max(1e-6, price * 0.0005); // 0.05%
    }



    // =========== 更宽松、NaN安全的 MACD 判定 ===========
    static boolean macdLooseRelax(MacdResult m) {
        int last = m.hist.length - 1;
        if (last < 1) return true; // 数据太短：放过

        double h0 = m.hist[last];
        double h1 = m.hist[last - 1];
        double band = nearZeroBand(m.hist, 34);

        // 1) 最近不变弱：h0 >= h1 - 极小容差
        boolean notWorsening = (isFinite(h0) && isFinite(h1)) ? (h0 + 1e-12 >= h1) : true;

        // 2) 3根SMA在抬头
        double smaNow = smaArr(m.hist, Math.min(3, last + 1), last);
        double smaPrev= smaArr(m.hist, Math.min(3, last + 1), last - 1);
        boolean smaUp = (isFinite(smaNow) && isFinite(smaPrev)) ? (smaNow >= smaPrev) : true;

        // 3) 近3根里≥2次抬升
        boolean up2of3 = upInKofN(m.hist, 2, 3);

        // 4) 金叉
        boolean crossUp = (m.macd != null && m.signal != null) && macdCrossUp(m.macd, m.signal);

        // 5) 处于“近零带 × 2”以内（更宽容）
        boolean nearZero = isFinite(h0) && Math.abs(h0) <= band * 2.0;

        return notWorsening || smaUp || up2of3 || crossUp || nearZero;
    }
    // ===== 新：流动性判定 =====
    static class LiquidityCheck {
        final boolean ok; final double med20; final double avg20; final double today;
        LiquidityCheck(boolean ok, double med20, double avg20, double today) {
            this.ok = ok; this.med20 = med20; this.avg20 = avg20; this.today = today;
        }
    }
    // 52周最低
    static double rollingLow(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd;
        int start = Math.max(0, end - n + 1);
        double lo = Double.POSITIVE_INFINITY;
        for (int i = start; i <= end; i++) lo = Math.min(lo, bars.get(i).low);
        return lo;
    }

    // Minervini Trend Template（略去RS评分；至少满足 6/7 条）
    static boolean minerviniTemplate(List<Bar> bars) {
        int last = bars.size() - 1;
        double price = bars.get(last).close;
        double ma50   = sma(bars,  50, -1);
        double ma150  = sma(bars, 150, -1);
        double ma200  = sma(bars, 200, -1);
        double ma200Prev = sma(bars, 200, -1 - 20); // 至少1个月向上
        double hi52 = rollingHigh(bars, 252, -1);
        double lo52 = rollingLow (bars, 252, -1);

        boolean c1 = price > ma150 && price > ma200;
        boolean c2 = ma150 > ma200;
        boolean c3 = !Double.isNaN(ma200Prev) && ma200 > ma200Prev;
        boolean c4 = ma50 > ma150 && ma50 > ma200;
        boolean c5 = price > ma50;
        boolean c6 = price >= lo52 * 1.25;         // 距52周低点至少+25%
        boolean c7 = price >= hi52 * 0.75;         // 距52周高 ≤25%

        int pass = 0;
        if (c1) pass++; if (c2) pass++; if (c3) pass++;
        if (c4) pass++; if (c5) pass++; if (c6) pass++; if (c7) pass++;
        return pass >= 6;
    }

    static double[] lastNDailyTurnovers(java.util.List<Bar> bars, int N) {
        int n = Math.min(N, bars.size());
        double[] t = new double[n];
        int last = bars.size() - 1;
        for (int i = 0; i < n; i++) {
            Bar b = bars.get(last - (n - 1 - i));
            t[i] = b.close * (double)b.volume; // 注意 volume 单位：应为“股”
        }
        return t;
    }

    static double median(double[] a) {
        if (a.length == 0) return 0;
        double[] b = java.util.Arrays.copyOf(a, a.length);
        java.util.Arrays.sort(b);
        int m = b.length / 2;
        if ((b.length & 1) == 1) return b[m];
        return 0.5 * (b[m - 1] + b[m]);
    }

    static double mean(double[] a) {
        if (a.length == 0) return 0;
        double s = 0; for (double v : a) s += v; return s / a.length;
    }

    static LiquidityCheck liquidityOk(java.util.List<Bar> bars) {
        double[] last20 = lastNDailyTurnovers(bars, 20);
        double med20 = median(last20);
        double avg20 = mean(last20);
        double today = last20.length > 0 ? last20[last20.length - 1] : 0.0;

        boolean liquid =
                (med20 >= LIQ_TURN_MED20_MIN) ||
                        (avg20 >= LIQ_TURN_AVG20_MIN) ||
                        (med20 > 0 && today >= LIQ_TODAY_PUMP_MULT * med20);

        return new LiquidityCheck(liquid, med20, avg20, today);
    }

    // ===== 并发扫描 =====
    static class CsvRow {
        String symbol, name, date, signal;
        double close, sma10, sma20, sma50, high52w, dist52w, volMA20, turnMA20, trigger20h, todayVol;
        public Double adx, diPlus, diMinus, atrPct;
        CsvRow(String s, String n, String d, double c, double m10, double m20, double m50,
               double h52, double d52, double vma, double tma, double trig, long tvol, String sig) {
            this.symbol = s; this.name = n; this.date = d; this.close = c; this.sma10 = m10; this.sma20 = m20; this.sma50 = m50;
            this.high52w = h52; this.dist52w = d52; this.volMA20 = vma; this.turnMA20 = tma; this.trigger20h = trig; this.todayVol = tvol; this.signal = sig;
        }
    }

    static java.util.List<CsvRow> scanAllParallel(java.util.List<String> symbols) throws InterruptedException {
        java.util.List<CsvRow> out = Collections.synchronizedList(new ArrayList<>());
        int threads = Math.min(YH_MAX_PARALLEL, Math.max(2, Runtime.getRuntime().availableProcessors()));
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CompletionService<Void> ecs = new java.util.concurrent.ExecutorCompletionService<>(pool);

        for (String symbol : symbols) {
            ecs.submit(() -> {
                CsvRow row = processOneSymbol(symbol);
                if (row != null) out.add(row);
                return null;
            });
        }
        for (int i = 0; i < symbols.size(); i++) {
            try { ecs.take().get(); } catch (Exception ignored) {}
        }
        pool.shutdown();
        return out;
    }

    static CsvRow processOneSymbol(String symbol) {
        try {
            String name = symbol;
            Chart chart = fetchChart1yDaily(symbol);
            if (chart.bars.size() < 120) return null;

            Bar last = chart.bars.get(chart.bars.size() - 1);
            if (last.close < MIN_PRICE) return null;

            double sma10 = sma(chart.bars, 10, -1);
            double sma20 = sma(chart.bars, 20, -1);
            double sma50 = sma(chart.bars, 50, -1);
            double prevSma20 = sma(chart.bars, 20, -2);
            double prevSma50 = sma(chart.bars, 50, -2);

            double volMA20   = avgVol(chart.bars, 20, -1);
            double turnMA20  = avgTurnover(chart.bars, 20, -1); // 仅用于输出
            double high52w   = rollingHigh(chart.bars, 252, -1);
            double dist52w   = (high52w - last.close) / (high52w <= 0 ? 1.0 : high52w);

            // 流动性（硬约束，保持不变）
            LiquidityCheck L = liquidityOk(chart.bars);
            if (!L.ok) return null;

            // ========= NEW: 结构过滤拆成“强趋势”和“早期趋势”两套 =========
            // 强趋势：价 > 10 > 20 > 50，20/50 抬头，离 52w 高点不远（20% 内）
            boolean maStackStrong = (last.close > sma10 && sma10 > sma20 && sma20 > sma50);
            // 早期：价 > 20 > 50 就行，10 日线可以还没完全排好
            boolean maStackEarly  = (last.close > sma20 && sma20 > sma50);

            boolean maRising = (sma20 > prevSma20 && sma50 > prevSma50);

            boolean nearHighStrong = (dist52w <= MAX_DIST_52W);
            boolean nearHighEarly  = (dist52w <= MAX_DIST_52W_EARLY);

            boolean structureStrong = maStackStrong && maRising && nearHighStrong;
            boolean structureEarly  = maStackEarly  && maRising && nearHighEarly;

            boolean structureOk = structureStrong || structureEarly;
            if (!structureOk) return null;
            // ========= END NEW =========

            // 趋势（ADX/MACD/均线模板）：继续用 passesTrendFilters 控
            boolean trendPass = passesTrendFilters(chart.bars);
            if (!trendPass) return null;

            // 量能过滤：STRICT/NORMAL 仍一票否决；LOOSE 作为打分，不挡路
            boolean volPass = passesVolumeFilters(chart.bars);

            double trigger20h = rollingHigh(chart.bars, 20, -2);
            boolean volBreak  = last.volume >= volMA20 * BREAKOUT_VOL_MULTIPLIER;
            boolean breakout  = (last.close >= trigger20h) && volBreak;

            Bar prev = chart.bars.get(chart.bars.size() - 2);
            double prevSma10 = sma(chart.bars, 10, -2);
            boolean rebound10 = (prev.close < prevSma10 && last.close > sma10)
                    && (last.volume >= volMA20);

            String signal;
            if (volPass) {
                signal = breakout ? "BREAKOUT" : (rebound10 ? "REB10D" : "SETUP");
            } else {
                if (STRICTNESS == Strictness.STRICT || STRICTNESS == Strictness.NORMAL) return null;
                signal = "SETUP"; // LOOSE：量能不过也先列出做二次筛
            }

            Double adx = null, diPlus = null, diMinus = null, atrPct = null;
            try {
                double[] highs  = chart.bars.stream().mapToDouble(b -> b.high).toArray();
                double[] lows   = chart.bars.stream().mapToDouble(b -> b.low).toArray();
                double[] closes = chart.bars.stream().mapToDouble(b -> b.close).toArray();

                DmiAtr d = computeDmiAtr(highs, lows, closes, 14);
                if (d != null) {
                    adx     = d.adx;
                    diPlus  = d.diPlus;
                    diMinus = d.diMinus;
                    atrPct  = d.atrPct;
                }
            } catch (Exception ignore) { /* 指标算失败就留空，不挡流程 */ }

            CsvRow row = new CsvRow(
                    symbol, name, fmtDate(last.ts), last.close,
                    sma10, sma20, sma50, high52w, dist52w,
                    volMA20, turnMA20, trigger20h, last.volume, signal
            );
            row.adx = adx;
            row.diPlus = diPlus;
            row.diMinus = diMinus;
            row.atrPct = atrPct;
            return row;
        } catch (Exception e) {
            System.err.println("处理失败 " + symbol + " -> " + e.getMessage());
            return null;
        }
    }
    // 放在 TrendScreener 里（或单独 Utils 类）
    public static class DmiAtr {
        public double adx, diPlus, diMinus, atr, atrPct;
    }

    public static DmiAtr computeDmiAtr(double[] high, double[] low, double[] close, int n) {
        if (high == null || low == null || close == null) return null;
        int len = close.length;
        if (len < n + 2) return null; // 数据太短

        double sumTR = 0, sumPlusDM = 0, sumMinusDM = 0;
        double atr = 0, plusDMn = 0, minusDMn = 0;

        double[] dxArr = new double[len]; // 存前 2n 的 DX 用来初始化 ADX
        int dxCount = 0;

        // 先累计前 n 个周期的 TR / DM
        for (int i = 1; i <= n; i++) {
            double upMove = high[i] - high[i - 1];
            double downMove = low[i - 1] - low[i];
            double plusDM = (upMove > downMove && upMove > 0) ? upMove : 0;
            double minusDM = (downMove > upMove && downMove > 0) ? downMove : 0;

            double tr = Math.max(high[i] - low[i],
                    Math.max(Math.abs(high[i] - close[i - 1]), Math.abs(low[i] - close[i - 1])));

            sumTR += tr; sumPlusDM += plusDM; sumMinusDM += minusDM;
        }
        atr = sumTR; plusDMn = sumPlusDM; minusDMn = sumMinusDM; // Wilder 的“初值”为 n 期总和

        double diPlus = 100.0 * (plusDMn / atr);
        double diMinus = 100.0 * (minusDMn / atr);
        double dx = (diPlus + diMinus == 0) ? 0 : 100.0 * Math.abs(diPlus - diMinus) / (diPlus + diMinus);
        dxArr[dxCount++] = dx;

        // 接下来逐根用 Wilder 平滑
        for (int i = n + 1; i < len; i++) {
            double upMove = high[i] - high[i - 1];
            double downMove = low[i - 1] - low[i];
            double plusDM = (upMove > downMove && upMove > 0) ? upMove : 0;
            double minusDM = (downMove > upMove && downMove > 0) ? downMove : 0;

            double tr = Math.max(high[i] - low[i],
                    Math.max(Math.abs(high[i] - close[i - 1]), Math.abs(low[i] - close[i - 1])));

            // Wilder 平滑：new = old - old/n + today
            atr      = atr      - (atr      / n) + tr;
            plusDMn  = plusDMn  - (plusDMn  / n) + plusDM;
            minusDMn = minusDMn - (minusDMn / n) + minusDM;

            diPlus = 100.0 * (plusDMn / atr);
            diMinus = 100.0 * (minusDMn / atr);
            dx = (diPlus + diMinus == 0) ? 0 : 100.0 * Math.abs(diPlus - diMinus) / (diPlus + diMinus);

            if (dxCount < 2 * n) dxArr[dxCount++] = dx; // 收集初始化 ADX 的前 2n 个 DX
        }

        // 初始化 ADX：前 n 个 DX 的平均（从第 n 根到第 2n-1 根，含第一段 dxArr[0]）
        double adx;
        if (dxCount >= n) {
            double sumDX = 0;
            for (int i = 0; i < n; i++) sumDX += dxArr[i];
            adx = sumDX / n;

            // 若还有更多 DX，继续用 Wilder 平滑
            for (int i = n; i < dxCount; i++) {
                adx = ((adx * (n - 1)) + dxArr[i]) / n;
            }
        } else {
            adx = dxArr[Math.max(0, dxCount - 1)];
        }

        DmiAtr out = new DmiAtr();
        out.adx = adx;
        out.diPlus = diPlus;
        out.diMinus = diMinus;
        out.atr = atr / n; // 注意：上面 atr 是 n 期“总量”，Wilder 的 ATR = 总量 / n
        out.atrPct = (out.atr / close[len - 1]); // 比例（0.02=2%）
        return out;
    }

    // =========== NaN 安全的工具 ===========
    static boolean isFinite(double x){ return !Double.isNaN(x) && !Double.isInfinite(x); }

    // ===== 数据结构 =====
    static class Bar {
        long ts; double open, high, low, close; long volume;
        Bar(long ts, double o, double h, double l, double c, long v) {
            this.ts = ts; this.open = o; this.high = h; this.low = l; this.close = c; this.volume = v;
        }
    }

    static class Chart {
        String symbol; List<Bar> bars = new ArrayList<>();
    }

    static class Quote {
        String name;
    }

    static class AdxResult {
        double[] adx, diPlus, diMinus;
    }

    static class MacdResult {
        double[] macd, signal, hist;
    }
}
