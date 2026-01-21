package com.kabu.service;  // TrendSellAdvisor.java
// 读取持仓Excel -> 拉取行情 -> 策略判定 -> 输出 decisions.csv （含层级与卖出分配）

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TrendSellAdvisor {

    // ====== 基本配置 ======
    static final String INPUT_EXCEL  = "positions.xlsx";
    static final String OUTPUT_CSV   = "decisions";
    static final ZoneId JP           = ZoneId.of("Asia/Tokyo");
    static final boolean USE_MARKET_TAILWIND = true;

    // 网络与解析
    static final long RATE_LIMIT_MS = 200;
    static final int  MAX_RETRY     = 4;
    static final String UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36";
    static volatile boolean USE_QUERY2 = true;

    // 试仓时间止损（只对 Adds=0 生效）
    static final int    PROBE_MAX_DAYS_NO_BREAKOUT = 10;   // 10 个交易日不突破 20H
    static final int    PROBE_MAX_DAYS_NO_GAIN     = 15;   // 15 个交易日无进展
    static final double PROBE_MIN_GAIN             = 0.05; // +5%

    // ====== 策略开关 & 参数（增强）======
    static final boolean ENABLE_PROFIT_DRAWDOWN = true;
    static final double  DD_PNL_ON = 0.10;      // 浮盈≥10%才启用回撤止盈
    static final double  DD_PCT_FLOOR = 0.05;   // 固定回撤阈值下限（5%）
    static final boolean DD_USE_ATR = true;     // 是否用ATR动态化阈值
    static final double  DD_ATR_MULT = 1.8;     // ATR倍数 → 转为价格占比
    static final double  DD_PCT_MIN = 0.03;     // 动态阈值最小3%
    static final double  DD_PCT_MAX = 0.10;     // 动态阈值最大10%

    static final boolean ENABLE_MOMENTUM_DECAY = true;
    static final int     RSI_DIV_WIN = 40;           // 寻找前高窗口
    static final double  RSI_PRICE_DELTA = 0.02;     // 价较前高≥2%但RSI不新高/拐头
    static final int     MACD_HIST_DOWN_DAYS = 4;    // MACD柱体连续收缩天数
    static final double  MACD_HIST_DROP_RATIO = 0.35;// 收缩比例≥35%

    static final boolean ENABLE_LATE_PULLBACK_PROXY = true;
    // 无分时数据，用“日内高点-收盘回落”+放量做尾盘回落代理
    static final double  LP_MIN_DAY_GAIN = 0.06;   // 当日涨幅≥6%
    static final double  LP_FROM_HIGH_PCT = 0.012; // 距当日高点回撤≥1.2%
    static final double  LP_VOL_MULT = 1.30;       // 当日量≥20日均量×1.3
    static final int     LP_SELL_PCT = 33;         // 卖出33%

    static final boolean ENABLE_TIME_TP = true;    // 情景化“时间止盈/止损”
    static final int     TIME_TP_DAYS = 45;        // 自最近加仓起≥45个交易日
    static final int     TIME_TP_LOOKBACK = 20;    // 近20日未创新高
    static final int     TIME_TP_SELL_PCT = 25;    // 卖出25%
    // 动量衰减是否需要“连2日确认”
    static final boolean MOM_USE_CONFIRM = true;
    static final int MOM_CONFIRM_WINDOW = 7;   // 最近3天窗口（含今天）
    static final int MOM_CONFIRM_MIN_HITS = 4; // 至少出现2次才触发

    // 固定峰值回撤止盈（自最近买入后的最高点回撤 5%）
    static final boolean ENABLE_PEAK_DRAWDOWN_5PCT = true;
    static final double  PEAK_DRAWDOWN_5PCT       = 0.05;      // 5%
    static final String  PEAK_DRAWDOWN_ACTION     = "SELL_ALL"; // 这里默认全清，需要减仓就改成 "SELL_1_2" 等

    // 跳空/暴跌快速风控参数
    static final double GAP_20D_MIN_PCT = 0.02;   // 相对20D向下跳空 ≥ 2%

    static final OkHttpClient http = new OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .callTimeout(20, TimeUnit.SECONDS)
            .build();
    static final ObjectMapper om = new ObjectMapper();
    static long lastCallAt = 0;

    // 简单内存缓存
    static final long CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(15);
    static final Map<String, CacheEntry<JsonNode>> JSON_CACHE = new HashMap<>();
    static final Map<String, CacheEntry<Chart>>    CHART_CACHE = new HashMap<>();
    static class CacheEntry<T> { final long ts = System.currentTimeMillis(); final T val; CacheEntry(T v){ this.val=v; } boolean fresh(){ return System.currentTimeMillis()-ts <= CACHE_TTL_MS; } }

    // ====== 主流程 ======
    public static void main(String[] args) throws Exception {
        List<Position> positions = loadPositionsFromExcel(INPUT_EXCEL);
        if (positions.isEmpty()) {
            System.err.println("Excel 内未读取到任何持仓；请检查列名：股票编号 / 买入价 / 买入时间 / 买入股数 （可选：层级）");
            return;
        }
        System.out.println("持仓合并后股票数: " + positions.size());

        List<DecisionRow> results = new ArrayList<>();
        for (Position pos : positions) {
            String symbol = ensureTokyoSymbol(pos.symbol);
            try {
                Chart chart = fetchChart1yDaily(symbol);
                if (chart.bars.size() < 60) {
                    results.add(DecisionRow.na(symbol, pos));
                    continue;
                }
                Decision d = decide(pos, chart.bars);
                if (needsSell(d.action)) {
                    d.sellPlan = planSellLots(pos, d.action);
                }
                results.add(DecisionRow.of(symbol, pos, chart, d));
                Thread.sleep(160);
            } catch (Exception ex) {
                System.err.println("处理失败 " + symbol + " -> " + ex.getMessage());
                results.add(DecisionRow.err(symbol, pos, ex.getMessage()));
            }
        }

        writeCsv(results, OUTPUT_CSV);
        System.out.println("完成 -> " + OUTPUT_CSV + " (总行数=" + results.size() + ")");
    }

    // ====== Excel 读入（支持“层级”可选列） ======
    static List<Position> loadPositionsFromExcel(String path) {
        Map<String, Position> map = new LinkedHashMap<>();
        try (InputStream is =
                     TrendSellAdvisor.class.getClassLoader().getResourceAsStream(path) != null
                             ? TrendSellAdvisor.class.getClassLoader().getResourceAsStream(path)
                             : new FileInputStream(path);
             Workbook wb = WorkbookFactory.create(is)) {

            Sheet sh = wb.getSheetAt(0);
            Row header = sh.getRow(0);
            if (header == null) throw new IllegalArgumentException("找不到表头行");

            int colSymbol = findCol(header, "股票编号");
            int colPrice  = findCol(header, "买入价");
            int colDate   = findCol(header, "买入时间");
            int colQty    = findCol(header, "买入股数");

            Integer colLevel = null;
            try { colLevel = findCol(header, "层级"); } catch (Exception ignore) {}

            for (int r = 1; r <= sh.getLastRowNum(); r++) {
                Row row = sh.getRow(r);
                if (row == null) continue;

                String symbol = getString(row.getCell(colSymbol)).trim();
                if (symbol.isEmpty()) continue;

                Double price = getDouble(row.getCell(colPrice));
                if (price == null || price <= 0) continue;

                Integer qty = getInt(row.getCell(colQty));
                if (qty == null || qty <= 0) qty = 1;

                long ts = parseDateToEpoch(getString(row.getCell(colDate)));

                Integer level = null;
                if (colLevel != null) {
                    Integer lv = getInt(row.getCell(colLevel));
                    if (lv != null && lv >= 0) level = lv;
                }

                Position p = map.computeIfAbsent(symbol, Position::new);
                p.lots.add(new Lot(ts, price, qty, level));
            }

            for (Position p : map.values()) {
                boolean allNull = p.lots.stream().allMatch(l -> l.level == null);
                if (allNull) {
                    p.lots.sort(Comparator.comparingLong(l -> l.buyTs));
                    for (int i = 0; i < p.lots.size(); i++) p.lots.get(i).level = i;
                } else {
                    p.lots.sort(Comparator.<Lot, Integer>comparing(l -> l.level)
                            .thenComparingLong(l -> l.buyTs));
                }
            }
        } catch (Exception e) {
            System.err.println("读取Excel出错: " + e.getMessage());
        }
        return new ArrayList<>(map.values());
    }

    static int findCol(Row header, String name) {
        for (int i=0;i<header.getLastCellNum();i++){
            String v = getString(header.getCell(i)).trim();
            if (name.equals(v)) return i;
        }
        throw new IllegalArgumentException("缺少列: " + name);
    }
    static String getString(Cell c){
        if (c==null) return "";
        if (c.getCellType()==CellType.STRING) return c.getStringCellValue();
        if (c.getCellType()==CellType.NUMERIC){
            if (DateUtil.isCellDateFormatted(c)){
                Instant ins = c.getDateCellValue().toInstant();
                return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(JP).format(ins);
            }
            double v=c.getNumericCellValue();
            if (Math.floor(v)==v) return String.valueOf((long)v);
            return String.valueOf(v);
        }
        if (c.getCellType()==CellType.BOOLEAN) return String.valueOf(c.getBooleanCellValue());
        return "";
    }
    static Double getDouble(Cell c){
        try{
            if (c==null) return null;
            if (c.getCellType()==CellType.NUMERIC) return c.getNumericCellValue();
            return Double.valueOf(getString(c));
        }catch(Exception e){ return null; }
    }
    static Integer getInt(Cell c){
        try{
            if (c==null) return null;
            if (c.getCellType()==CellType.NUMERIC) return (int)Math.round(c.getNumericCellValue());
            return Integer.valueOf(getString(c).replaceAll("\\.0+$",""));
        }catch(Exception e){ return null; }
    }
    static long parseDateToEpoch(String s){
        if (s==null || s.isEmpty()) return 0L;
        String[] fmts = {"yyyy-MM-dd HH:mm","yyyy/MM/dd HH:mm","yyyy-MM-dd","yyyy/MM/dd"};
        for (String f : fmts){
            try{
                LocalDateTime ldt;
                if (f.contains("HH")){
                    ldt = LocalDateTime.parse(s, DateTimeFormatter.ofPattern(f));
                }else{
                    ldt = LocalDate.parse(s, DateTimeFormatter.ofPattern(f)).atStartOfDay();
                }
                return ldt.atZone(JP).toEpochSecond();
            }catch(DateTimeParseException ignored){}
        }
        return 0L;
    }

    // ====== 抓行情（Yahoo 优先，Stooq 兜底）======
    static String yahooBase(){ return USE_QUERY2 ? "https://query2.finance.yahoo.com" : "https://query1.finance.yahoo.com"; }

    static Chart fetchChart1yDaily(String symbol) throws Exception {
        String key = "chart:"+symbol;
        CacheEntry<Chart> hit = CHART_CACHE.get(key);
        if (hit!=null && hit.fresh()) return hit.val;
        try{
            Chart c = fetchChartFromYahoo(symbol);
            CHART_CACHE.put(key, new CacheEntry<>(c));
            return c;
        }catch(IOException e){
            Chart c = fetchChartFromStooq(symbol);
            CHART_CACHE.put(key, new CacheEntry<>(c));
            return c;
        }
    }

    static Chart fetchChartFromYahoo(String symbol) throws IOException, InterruptedException {
        String url = yahooBase()+"/v8/finance/chart/"+symbol+"?range=1y&interval=1d";
        JsonNode root = getJson(url, symbol);
        JsonNode err  = root.path("chart").path("error");
        if (!err.isMissingNode() && !err.isNull()) throw new IOException("chart error");

        JsonNode res = root.path("chart").path("result").get(0);
        JsonNode ts  = res.path("timestamp");
        JsonNode q   = res.path("indicators").path("quote").get(0);

        Chart c = new Chart(); c.symbol = symbol;
        for (int i=0;i<ts.size();i++){
            long t = ts.get(i).asLong();
            double o=getNum(q,"open",i), h=getNum(q,"high",i), l=getNum(q,"low",i), cl=getNum(q,"close",i);
            long v = (long)getNum(q,"volume",i);
            if (!Double.isNaN(cl) && v>=0) c.bars.add(new Bar(t,o,h,l,cl,v));
        }
        return c;
    }

    static Chart fetchChartFromStooq(String symbol) throws Exception {
        String code = symbol.toLowerCase(Locale.ROOT).replace(".t",".jp");
        String url  = "https://stooq.com/q/d/l/?s="+code+"&i=d";
        Request req = new Request.Builder().url(url)
                .header("User-Agent", UA)
                .header("Accept","text/csv,*/*;q=0.8")
                .build();
        try(Response r = http.newCall(req).execute()){
            if (!r.isSuccessful()) throw new IOException("stooq HTTP "+r.code());
            byte[] buf = r.body()!=null ? r.body().bytes() : new byte[0];
            String csv = new String(buf, StandardCharsets.UTF_8).trim();
            if (!csv.startsWith("Date") || csv.indexOf('\n')<0 || csv.split("\\R").length<=1){
                throw new IOException("stooq empty");
            }
            Chart c = new Chart(); c.symbol=symbol;
            String[] lines = csv.split("\\R");
            for (int i=1;i<lines.length;i++){
                String[] f = lines[i].split(",");
                if (f.length<6) continue;
                long ts = LocalDate.parse(f[0]).atStartOfDay(JP).toEpochSecond();
                double o=parseD(f[1]), h=parseD(f[2]), l=parseD(f[3]), cl=parseD(f[4]);
                long v = (long)parseD(f[5]);
                if (!Double.isNaN(cl)) c.bars.add(new Bar(ts,o,h,l,cl,v));
            }
            if (c.bars.isEmpty()) throw new IOException("stooq no bars");
            return c;
        } finally {
            Thread.sleep(220 + ThreadLocalRandom.current().nextInt(120));
        }
    }
    static double parseD(String s){ try{ return Double.parseDouble(s);}catch(Exception e){ return Double.NaN; } }

    static JsonNode getJson(String url, String symbol) throws IOException, InterruptedException {
        CacheEntry<JsonNode> jh = JSON_CACHE.get(url);
        if (jh!=null && jh.fresh()) return jh.val;

        long wait = RATE_LIMIT_MS - (System.currentTimeMillis()-lastCallAt);
        if (wait>0) Thread.sleep(wait);

        IOException last = null;
        for (int i=0;i<=MAX_RETRY;i++){
            lastCallAt = System.currentTimeMillis();
            Request req = new Request.Builder()
                    .url(url)
                    .header("User-Agent", UA)
                    .header("Accept","application/json")
                    .build();

            try(Response resp = http.newCall(req).execute()){
                int code = resp.code();
                byte[] buf = (resp.body()!=null) ? resp.body().bytes() : new byte[0];
                if (code==200){
                    if (buf.length==0) throw new IOException("empty body");
                    String head = new String(buf, 0, Math.min(buf.length, 64), StandardCharsets.UTF_8);
                    if (head.startsWith("<") || head.startsWith("<!--")){
                        USE_QUERY2 = !USE_QUERY2;
                        Thread.sleep(800L*(i+1));
                        url = url.replace("https://query1.finance.yahoo.com", yahooBase())
                                .replace("https://query2.finance.yahoo.com", yahooBase());
                        continue;
                    }
                    JsonNode node = om.readTree(buf);
                    JSON_CACHE.put(url,new CacheEntry<>(node));
                    return node;
                }
                if (code==401){
                    USE_QUERY2 = !USE_QUERY2;
                    Thread.sleep(1200L*(i+1));
                    url = url.replace("https://query1.finance.yahoo.com", yahooBase())
                            .replace("https://query2.finance.yahoo.com", yahooBase());
                    continue;
                }
                if (code==429 || code>=500){
                    long backoff = (long)Math.pow(2,i)*500L + ThreadLocalRandom.current().nextInt(400);
                    Thread.sleep(backoff);
                    continue;
                }
                throw new IOException("HTTP "+code);
            }catch(IOException e){ last = e; }
        }
        throw (last!=null ? last : new IOException("request failed"));
    }

    // ====== 指标 & 决策 ======
    static Decision decide(Position pos, List<Bar> bars){
        Decision d = new Decision(); d.action="HOLD";

        Bar last = bars.get(bars.size()-1);
        Bar prev = bars.get(bars.size()-2);

        double m10 = sma(bars,10,-1), m15=sma(bars,15,-1), m20=sma(bars,20,-1);
        double m40 = sma(bars,40,-1), m30=sma(bars,30,-1);
        double a14 = atr(bars,14,-1);
        double vma20 = avgVol(bars,20,-1);
        double[] b20 = boll(bars,20,-1); double up=b20[1];
        double r14 = rsi(bars,14,-1);

        double avgCost = pos.avgCost();
        double pnl = Double.isNaN(avgCost) ? Double.NaN : (last.close-avgCost)/avgCost;

        // 层级 & “全清参考线”（LossMA用于CSV展示）
        int adds = pos.addCount(); // 0=观察仓,1=一层仓,2+=两层及以上
        double lossMA = (adds==0 ? m20 : (adds==1 ? m20 : m15)); // 全清线：0→20D，1→20D，2+→15D
        d.refs.put("Adds", (double) adds);
        d.refs.put("LossMA", lossMA);

        // 1) 跳空/暴跌（快速风控）
        // 1.1 大幅跳空跌破20D（昨收仍在20D上方）
        if (!Double.isNaN(m20) && m20 > 0) {
            // 昨日收盘在20D上方，说明20D还是有效支撑
            boolean wasAbove20 = prev.close > m20;

            // 相对20D向下跳空的幅度（正数=向下跳空）
            double gapFrom20Pct = (m20 - last.open) / m20;

            // 条件：昨天在20D上方 + 今天开盘相对20D向下跳空≥2% + 收盘仍在20D下方
            if (wasAbove20 && gapFrom20Pct >= GAP_20D_MIN_PCT && last.close < m20) {
                if (adds == 0) {
                    // 观察仓：直接全清
                    d.action = "SELL_ALL";
                    d.reasons.add(String.format(
                            Locale.US,
                            "观察仓：Gap-down %.2f%% 跳空跌破20D并收在其下→全清",
                            gapFrom20Pct * 100.0
                    ));
                } else {
                    // 多层仓位：先减半，避免一次性砍光
                    d.action = "SELL_1_2";
                    d.reasons.add(String.format(
                            Locale.US,
                            "重仓：Gap-down %.2f%% 跳空跌破20D并收在其下→先减半",
                            gapFrom20Pct * 100.0
                    ));
                }
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl);
                return d;
            }
        }
        if ((last.close-prev.close)/prev.close <= -0.04 && last.volume>=vma20){
            d.action="SELL_1_2"; d.reasons.add("单日-4%且放量");
            if (last.close<m20) d.reasons.add("收破20D");
            fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
        }

        // 试仓时间止损（Adds=0）
        if (adds == 0 && !pos.lots.isEmpty()) {
            Lot probe = null;
            for (Lot l : pos.lots) { if (l.level != null && l.level == 0) { probe = l; break; } }
            if (probe == null) { probe = pos.lots.get(0); }
            int idxEntry = firstBarIndexAtOrAfter(bars, probe.buyTs);
            if (idxEntry >= 0 && idxEntry < bars.size()-1) {
                int daysSince = (bars.size()-1) - idxEntry;
                double trigger20h = rollingHigh(bars, 20, -2);
                double maxCloseSince = maxClose(bars, idxEntry, bars.size()-1);

                boolean noBreakoutTooLong =
                        daysSince >= PROBE_MAX_DAYS_NO_BREAKOUT
                                && maxCloseSince < trigger20h
                                && last.close < m10;

                boolean noGainTooLong =
                        daysSince >= PROBE_MAX_DAYS_NO_GAIN
                                && (last.close - probe.buyPrice) / probe.buyPrice < PROBE_MIN_GAIN;

                if (noBreakoutTooLong) {
                    d.action = "SELL_ALL";
                    d.reasons.add("试仓超时：10日未突破20H且低于10D");
                    fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
                }
                if (noGainTooLong) {
                    d.action = "SELL_ALL";
                    d.reasons.add("试仓超时：15日无进展(<+5%)");
                    fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
                }
            }
        }

        // 2) 分层均线止损（带0.3%缓冲）：按照你的规则
        double buf = 0.997;
        if (adds == 0) {
            // 观察仓：跌破20D → 全清
            if (last.close < m20 * buf) {
                d.action = "SELL_ALL";
                d.reasons.add("观察仓：收盘跌破20D");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
            }
        } else if (adds == 1) {
            // 一层仓：先判断更严重的 20D，再判断 15D
            if (last.close < m40 * buf) {
                d.action = "SELL_ALL";
                d.reasons.add("一层仓：收盘跌破40D→清仓");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
            }
            if (last.close < m30 * buf) {
                d.action = "SELL_1_2";
                d.reasons.add("一层仓：收盘跌破30D→减半");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
            }
        } else { // adds >= 2
            // 两层及以上：先判断更严重的 15D，再判断 10D
            if (last.close < m30 * buf) {
                d.action = "SELL_ALL";
                d.reasons.add("两层及以上：收盘跌破30D→清仓");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
            }
            if (last.close < m20 * buf) {
                d.action = "SELL_1_2";
                d.reasons.add("两层及以上：收盘跌破20D→减半");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl); return d;
            }
        }
        // 3.0 固定峰值回撤止盈（自最近买入后的最高点回撤 5%）
        if (ENABLE_PEAK_DRAWDOWN_5PCT && !Double.isNaN(pnl) && pnl > 0) {
            // 以“最近一次买入时间”为起点
            int idxEntryPeak = firstBarIndexAtOrAfter(bars, pos.lastBuyTs());
            if (idxEntryPeak < 0) idxEntryPeak = 0;

            double peakSinceEntry = maxHigh(bars, idxEntryPeak, bars.size() - 1);
            if (peakSinceEntry > 0) {
                double ddFromPeak = (peakSinceEntry - last.close) / peakSinceEntry; // 相对最高点的回撤比例

                if (ddFromPeak >= PEAK_DRAWDOWN_5PCT) {
                    d.action = PEAK_DRAWDOWN_ACTION; // 默认 SELL_ALL，可在上面常量里改
                    d.reasons.add(String.format(Locale.US,
                            "固定峰值回撤止盈：自最近买入高点回撤 %.2f%% ≥ %.2f%%",
                            ddFromPeak * 100.0, PEAK_DRAWDOWN_5PCT * 100.0));

                    // 顺便把峰值和回撤记录到 refs 里，方便在 CSV 里看
                    d.refs.put("PeakHigh", peakSinceEntry);
                    d.refs.put("Drawdown%", ddFromPeak * 100.0);

                    fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl);
                    return d;
                } else {
                    // 即便未触发，也可以记录一下，方便调试 / 分析
                    d.refs.put("PeakHigh", peakSinceEntry);
                    d.refs.put("Drawdown%", ddFromPeak * 100.0);
                }
            }
        }
        // 3.1 浮盈回撤止盈
        if (ENABLE_PROFIT_DRAWDOWN && !Double.isNaN(pnl) && pnl >= DD_PNL_ON) {
            int idxEntry2 = firstBarIndexAtOrAfter(bars, pos.lastBuyTs());
            if (idxEntry2 < 0) idxEntry2 = 0;
            double peakH = maxHigh(bars, idxEntry2, bars.size()-1);
            if (peakH > 0) {
                double dd = (peakH - last.close) / peakH;
                double ddAtr = DD_USE_ATR ? (DD_ATR_MULT * a14 / peakH) : 0.0;
                double ddTrig = clamp(Math.max(DD_PCT_FLOOR, ddAtr), DD_PCT_MIN, DD_PCT_MAX);
                if (dd >= ddTrig) {
                    d.action = (pnl >= 0.20 ? "SELL_1_2" : "SELL_PCT_30");
                    d.reasons.add(String.format(Locale.US,
                            "浮盈回撤止盈：从峰值回撤 %.2f%% ≥ 阈值 %.2f%%",
                            dd*100.0, ddTrig*100.0));
                    fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
                }
                d.refs.put("PeakHigh", peakH);
                d.refs.put("Drawdown%", dd*100.0);
                d.refs.put("DDTrig%", ddTrig*100.0);
            }
        }

        // 3.2 动量衰减：RSI 顶背离 / MACD 柱体收缩
// 3.2 动量衰减：RSI 顶背离 / MACD 柱体收缩（近3天出现≥2次才执行）
        if (ENABLE_MOMENTUM_DECAY && bars.size() >= 60) {
            int N = bars.size() - 1;

            // —— RSI 顶背离：近3天内出现≥2次 ——
            boolean rsiToday = rsiTopDivergenceAt(bars, N, RSI_DIV_WIN, RSI_PRICE_DELTA);
            boolean rsiD1    = (N-1 >= 0) ? rsiTopDivergenceAt(bars, N-1, RSI_DIV_WIN, RSI_PRICE_DELTA) : false;
            boolean rsiD2    = (N-2 >= 0) ? rsiTopDivergenceAt(bars, N-2, RSI_DIV_WIN, RSI_PRICE_DELTA) : false;

            boolean rsiOk = rsiToday; // 默认允许“当天即触发”
            if (MOM_USE_CONFIRM) {
                int hits = (rsiToday?1:0) + (rsiD1?1:0) + (rsiD2?1:0);
                rsiOk = hits >= MOM_CONFIRM_MIN_HITS;
            }
            if (rsiOk) {
                d.action = "TRIM_1_3"; // 减1/3
                d.reasons.add(MOM_USE_CONFIRM
                        ? "动量衰减：顶背离（3天内≥2次确认）"
                        : "动量衰减：顶背离");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl);
                return d;
            }

            // —— MACD 柱体收缩（正区）：近3天内出现≥2次 ——
            boolean macdToday = macdContractionAt(bars, N,     MACD_HIST_DOWN_DAYS, MACD_HIST_DROP_RATIO);
            boolean macdD1    = (N-1 >= 0) ? macdContractionAt(bars, N-1, MACD_HIST_DOWN_DAYS, MACD_HIST_DROP_RATIO) : false;
            boolean macdD2    = (N-2 >= 0) ? macdContractionAt(bars, N-2, MACD_HIST_DOWN_DAYS, MACD_HIST_DROP_RATIO) : false;

            boolean macdOk = macdToday;
            if (MOM_USE_CONFIRM) {
                int hits = (macdToday?1:0) + (macdD1?1:0) + (macdD2?1:0);
                macdOk = hits >= MOM_CONFIRM_MIN_HITS;
            }
            if (macdOk) {
                d.action = "SELL_PCT_30"; // 卖30%
                d.reasons.add(MOM_USE_CONFIRM
                        ? "动量衰减：MACD柱体连续收缩（3天内≥2次确认）"
                        : "动量衰减：MACD柱体连续收缩");
                fillRefs(d, m10, m15, m20, a14, r14, avgCost, pnl);
                return d;
            }
        }

        // 3.3 尾盘回落代理（高-收回落+放量+大阳日）
        if (ENABLE_LATE_PULLBACK_PROXY) {
            double dayGain = (last.close - prev.close) / prev.close;
            double backFromHigh = (last.high > 0) ? (last.high - last.close) / last.high : 0.0;
            boolean volUp = last.volume >= vma20 * LP_VOL_MULT;
            if (dayGain >= LP_MIN_DAY_GAIN && backFromHigh >= LP_FROM_HIGH_PCT && volUp) {
                d.action = "SELL_PCT_" + LP_SELL_PCT;
                d.reasons.add(String.format(Locale.US,
                        "尾盘回落代理：当日涨幅%.2f%%、距高点回落%.2f%%、放量≥%.2fx",
                        dayGain*100.0, backFromHigh*100.0, LP_VOL_MULT));
                fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
            }
        }

        // 3.x ATR 追踪止盈（分档止盈）
        double trail = calcTrail(pnl, last.close, a14, m10, m15, m20);
        d.refs.put("trail", trail);
        if (last.close < trail*buf){
            if (!Double.isNaN(pnl) && pnl >= 0.20) {
                d.action="SELL_1_2";  // 收益高 → 卖多点
            } else if (!Double.isNaN(pnl) && pnl >= 0.10) {
                d.action="TRIM_1_3";  // 中等收益 → 卖1/3
            } else {
                d.action="SELL_1_4";  // 低收益 → 卖1/4
            }
            d.reasons.add("跌破ATR追踪线（分档止盈）");
            fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
        }

        // 3.4 情景化时间止盈
        if (ENABLE_TIME_TP) {
            int idxEntry3 = firstBarIndexAtOrAfter(bars, pos.lastBuyTs());
            if (idxEntry3 < 0) idxEntry3 = 0;
            int daysHeld = (bars.size()-1) - idxEntry3;
            double rh20 = rollingHigh(bars, TIME_TP_LOOKBACK, -1);
            boolean notMakingHigh = last.close < rh20; // 近20日未创新高
            if (daysHeld >= TIME_TP_DAYS && notMakingHigh) {
                d.action = "SELL_PCT_" + TIME_TP_SELL_PCT;
                d.reasons.add(String.format(Locale.US,
                        "时间止盈：持仓≥%d日且未创新高，先减%s%%",
                        TIME_TP_DAYS, String.valueOf(TIME_TP_SELL_PCT)));
                fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
            }
        }

        // 4) 逢强先减（保留）
        if (last.close>up && prev.close>up && r14>=78){
            d.action="TRIM_1_3"; d.reasons.add("布林上轨外连收 + RSI高位");
            fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl); return d;
        }

        // 5) 买回提示 & 市场顺风标识（保留）
        boolean reAdd = (last.close>m10 && last.volume>=vma20);
        d.refs.put("readdSignal", reAdd?1.0:0.0);

        boolean mktOk = !USE_MARKET_TAILWIND || marketTailwind(bars);
        d.refs.put("marketOK", mktOk?1.0:0.0);

        d.reasons.add("趋势完好（≥10/15/20D）");
        fillRefs(d,m10,m15,m20,a14,r14,avgCost,pnl);
        return d;
    }

    static boolean needsSell(String action){
        return "SELL_ALL".equals(action) || "SELL_1_2".equals(action)
                || "SELL_1_4".equals(action) || "TRIM_1_3".equals(action)
                || (action!=null && action.startsWith("SELL_PCT_"));
    }

    // 卖出分配：LIFO（后进先出），支持通用 SELL_PCT_xx
    static class LotSell { int lotIndex; int sellQty; LotSell(int i,int q){lotIndex=i;sellQty=q;} }
    static List<LotSell> planSellLots(Position pos, String action) {
        long cur = pos.totalQty();
        long target;

        if ("SELL_ALL".equals(action)) {
            target = cur;
        } else if ("SELL_1_2".equals(action)) {
            target = (long) Math.ceil(cur * 0.5);
        } else if ("SELL_1_4".equals(action)) {
            target = (long) Math.ceil(cur * 0.25);
        } else if ("TRIM_1_3".equals(action)) {
            target = (long) Math.ceil(cur / 3.0);
        } else if (action != null && action.startsWith("SELL_PCT_")) {
            try {
                int pct = Integer.parseInt(action.substring("SELL_PCT_".length()));
                target = (long) Math.ceil(cur * (pct / 100.0));
            } catch (Exception e) {
                target = 0L;
            }
        } else {
            target = 0L;
        }

        List<LotSell> plan = new ArrayList<>();
        if (target <= 0) return plan;

        for (int i = pos.lots.size() - 1; i >= 0 && target > 0; i--) {
            Lot lot = pos.lots.get(i);
            int take = (int) Math.min(target, (long) lot.qty);
            if (take > 0) {
                plan.add(new LotSell(i, take));
                target -= take;
            }
        }
        return plan;
    }

    static double calcTrail(double pnl, double close, double atr14, double m10, double m15, double m20){
        if (Double.isNaN(pnl)) return Math.max(m20, close - 2.0*atr14);
        if (pnl < 0.10) return Math.max(m20, close - 2.0*atr14);
        if (pnl < 0.20) return Math.max(m15, close - 1.7*atr14);
        return Math.max(m10, close - 1.3*atr14);
    }

    static boolean marketTailwind(List<Bar> bars){
        if (bars.size()<210) return true;
        Bar last = bars.get(bars.size()-1);
        double m50=sma(bars,50,-1), m200=sma(bars,200,-1), m200_prev=sma(bars,200,-2);
        return last.close>m50 && last.close>m200 && (m200 - m200_prev) > 0;
    }

    // ====== 指标函数 ======
    static double sma(List<Bar> bars, int n, int idxFromEnd){
        int end = bars.size()+idxFromEnd; int start=end-n+1;
        if (start<0) return Double.NaN; double s=0;
        for (int i=start;i<=end;i++) s+=bars.get(i).close; return s/n;
    }
    static double avgVol(List<Bar> bars, int n, int idxFromEnd){
        int end = bars.size()+idxFromEnd; int start=end-n+1;
        if (start<0) return Double.NaN; double s=0;
        for (int i=start;i<=end;i++) s+=bars.get(i).volume; return s/n;
    }
    static double atr(List<Bar> bars, int n, int idxFromEnd){
        int end = bars.size()+idxFromEnd; int start=end-n+1;
        if (start<=0) return Double.NaN; double s=0;
        for (int i=start;i<=end;i++){
            Bar c=bars.get(i), p=bars.get(i-1);
            double tr = Math.max(c.high-c.low, Math.max(Math.abs(c.high-p.close), Math.abs(c.low-p.close)));
            s+=tr;
        }
        return s/n;
    }
    static double[] boll(List<Bar> bars, int n, int idxFromEnd){
        int end = bars.size()+idxFromEnd; int start=end-n+1;
        if (start<0) return new double[]{Double.NaN,Double.NaN,Double.NaN};
        double ma=0; for (int i=start;i<=end;i++) ma+=bars.get(i).close; ma/=n;
        double var=0; for (int i=start;i<=end;i++){ double d=bars.get(i).close-ma; var+=d*d; }
        double sd = Math.sqrt(var/n);
        return new double[]{ma, ma+2*sd, ma-2*sd};
    }
    static double rsi(List<Bar> bars, int n, int idxFromEnd){
        int end = bars.size()+idxFromEnd; int start=end-n+1;
        if (start<=0) return Double.NaN;
        double up=0,dn=0;
        for (int i=start;i<=end;i++){
            double chg = bars.get(i).close - bars.get(i-1).close;
            if (chg>0) up+=chg; else dn-=chg;
        }
        if (up+dn==0) return 50;
        double rs = (up/n)/((dn/n)+1e-9);
        return 100 - 100/(1+rs);
    }
    static double getNum(JsonNode q, String field, int i){
        JsonNode arr = q.path(field);
        if (arr==null || arr.isMissingNode() || i>=arr.size()) return Double.NaN;
        JsonNode v = arr.get(i);
        return (v==null || v.isNull()) ? Double.NaN : v.asDouble();
    }
    static String ensureTokyoSymbol(String raw){
        String s = raw.trim();
        return s.contains(".") ? s : (s + ".T");
    }

    // ====== 数据结构 ======
    static class Bar { long ts; double open,high,low,close; long volume;
        Bar(long ts,double o,double h,double l,double c,long v){ this.ts=ts; this.open=o; this.high=h; this.low=l; this.close=c; this.volume=v; } }
    static class Chart { String symbol; List<Bar> bars = new ArrayList<>(); }
    static class Lot {
        long buyTs; double buyPrice; int qty; Integer level;
        Lot(long t,double p,int q,Integer lv){ buyTs=t; buyPrice=p; qty=q; level=lv; }
    }
    static class Position {
        String symbol; List<Lot> lots = new ArrayList<>();
        Position(String s){symbol=s;}
        double avgCost(){
            long tot=0; double amt=0;
            for (Lot l: lots){ amt += l.buyPrice*l.qty; tot += l.qty; }
            return tot==0? Double.NaN : amt/tot;
        }
        long totalQty(){ return lots.stream().mapToLong(l->l.qty).sum(); }
        long lastBuyTs(){ return lots.stream().mapToLong(l->l.buyTs).max().orElse(0L); }
        int addCount(){ // 最高层级值；没有则按 lots.size()-1
            int maxLv = lots.stream().mapToInt(l -> l.level==null? -1 : l.level).max().orElse(-1);
            return Math.max(maxLv, lots.size()-1);
        }
    }
    static class Decision {
        String action; List<String> reasons = new ArrayList<>();
        Map<String,Double> refs = new LinkedHashMap<>();
        List<LotSell> sellPlan = Collections.emptyList();
    }
    static void fillRefs(Decision d, double m10,double m15,double m20,double atr,double rsi,double avgCost,double pnl){
        d.refs.put("M10",m10); d.refs.put("M15",m15); d.refs.put("M20",m20);
        d.refs.put("ATR14",atr); d.refs.put("RSI14",rsi);
        if (!Double.isNaN(avgCost)) d.refs.put("AvgCost",avgCost);
        if (!Double.isNaN(pnl)) d.refs.put("PnL%", pnl*100.0);
    }

    static int firstBarIndexAtOrAfter(List<Bar> bars, long ts) {
        for (int i = 0; i < bars.size(); i++) if (bars.get(i).ts >= ts) return i;
        return -1;
    }
    static double maxClose(List<Bar> bars, int start, int end) {
        double m = Double.NEGATIVE_INFINITY;
        for (int i = Math.max(0, start); i <= end && i < bars.size(); i++) m = Math.max(m, bars.get(i).close);
        return m;
    }
    static double rollingHigh(List<Bar> bars, int n, int idxFromEnd) {
        int end = bars.size() + idxFromEnd;
        int start = Math.max(0, end - n + 1);
        double hi = Double.NEGATIVE_INFINITY;
        for (int i = start; i <= end; i++) hi = Math.max(hi, bars.get(i).high);
        return hi;
    }

    // ====== 额外工具/指标（增强需要）======
    static double clamp(double v, double lo, double hi){ return Math.max(lo, Math.min(hi, v)); }
    static double[] rsiSeries(List<Bar> bars, int n){
        int N = bars.size();
        double[] arr = new double[N];
        Arrays.fill(arr, Double.NaN);
        if (N <= n) return arr;
        for (int end=n; end<N; end++){
            double up=0,dn=0;
            for (int i=end-n+1;i<=end;i++){
                double chg = bars.get(i).close - bars.get(i-1).close;
                if (chg>0) up+=chg; else dn-=chg;
            }
            if (up+dn==0) arr[end]=50;
            else {
                double rs = (up/n)/((dn/n)+1e-9);
                arr[end] = 100 - 100/(1+rs);
            }
        }
        return arr;
    }
    // 在指定索引 idx 判定是否出现“RSI 顶背离”
    static boolean rsiTopDivergenceAt(List<Bar> bars, int idx, int win, double priceDelta) {
        if (idx <= 3 || idx >= bars.size()) return false;
        double[] rsiArr = rsiSeries(bars, 14);
        int start = Math.max(0, idx - win - 5);
        int prevHighIdx = start;
        for (int i = start; i <= idx - 3; i++) {
            if (bars.get(i).close > bars.get(prevHighIdx).close) prevHighIdx = i;
        }
        boolean priceHigher = bars.get(idx).close >= bars.get(prevHighIdx).close * (1.0 + priceDelta);
        boolean rsiNotHigher = !Double.isNaN(rsiArr[idx]) && !Double.isNaN(rsiArr[prevHighIdx])
                && (rsiArr[idx] <= rsiArr[prevHighIdx] || rsiArr[idx] < rsiArr[idx - 1]);
        return priceHigher && rsiNotHigher;
    }

    // 在指定索引 idx 判定是否“MACD柱体正区连续收缩，并且总收缩≥阈值”
    static boolean macdContractionAt(List<Bar> bars, int idx, int downDays, double dropRatio) {
        if (idx <= downDays || idx >= bars.size()) return false;
        Macd M = macd(bars);
        if (M.hist[idx] <= 0) return false; // 只在正区考虑收缩
        for (int i = idx - downDays + 1; i <= idx; i++) {
            if (!(M.hist[i] < M.hist[i - 1])) return false; // 必须天天变小
        }
        double base = M.hist[idx - downDays];
        double drop = (base - M.hist[idx]) / (Math.abs(base) + 1e-9);
        return drop >= dropRatio;
    }

    static class Macd { double[] dif, dea, hist; }
    static Macd macd(List<Bar> bars){
        int N = bars.size();
        Macd m = new Macd();
        m.dif = new double[N]; m.dea = new double[N]; m.hist = new double[N];
        if (N==0) return m;
        double k12 = 2.0/(12+1), k26 = 2.0/(26+1), k9 = 2.0/(9+1);
        double ema12 = bars.get(0).close, ema26 = bars.get(0).close, dea = 0;
        for (int i=0;i<N;i++){
            double c = bars.get(i).close;
            if (i==0){
                ema12=c; ema26=c; m.dif[i]=0; dea=0; m.dea[i]=dea; m.hist[i]=m.dif[i]-dea;
            }else{
                ema12 = ema12 + k12*(c - ema12);
                ema26 = ema26 + k26*(c - ema26);
                m.dif[i] = ema12 - ema26;
                dea     = dea + k9*(m.dif[i] - dea);
                m.dea[i]= dea;
                m.hist[i]= m.dif[i] - dea;
            }
        }
        return m;
    }

    // 最高 high（含端点，自动夹住下标）
    static double maxHigh(List<Bar> bars, int start, int end) {
        double mx = Double.NEGATIVE_INFINITY;
        int s = Math.max(0, start);
        int e = Math.min(bars.size() - 1, end);
        for (int i = s; i <= e; i++) {
            mx = Math.max(mx, bars.get(i).high);
        }
        return mx;
    }

    // ====== 输出表 ======
    static class DecisionRow {
        String symbol; String date; double lastClose; long lastVol;
        double avgCost; long totalQty;
        String action; String reason;
        double m10,m15,m20,atr,rsi,trail,lossMA; int marketOK, readd, adds;
        String sellPlan;

        static DecisionRow of(String symbol, Position pos, Chart c, Decision d){
            DecisionRow r = new DecisionRow();
            Bar last = c.bars.get(c.bars.size()-1);
            r.symbol = symbol;
            r.date   = DateTimeFormatter.ISO_LOCAL_DATE.withZone(JP).format(Instant.ofEpochSecond(last.ts));
            r.lastClose = last.close;
            r.lastVol   = last.volume;
            r.avgCost   = pos.avgCost();
            r.totalQty  = pos.totalQty();
            r.action    = d.action;
            r.reason    = String.join(" | ", d.reasons);
            r.m10 = opt(d.refs.get("M10")); r.m15=opt(d.refs.get("M15")); r.m20=opt(d.refs.get("M20"));
            r.atr = opt(d.refs.get("ATR14")); r.rsi=opt(d.refs.get("RSI14"));
            r.trail = opt(d.refs.getOrDefault("trail", Double.NaN));
            r.marketOK = d.refs.getOrDefault("marketOK",0.0)>0.5?1:0;
            r.readd    = d.refs.getOrDefault("readdSignal",0.0)>0.5?1:0;
            r.adds     = (int)Math.round(d.refs.getOrDefault("Adds", 0.0));
            r.lossMA   = opt(d.refs.getOrDefault("LossMA", Double.NaN)); // 全清参考线
            r.sellPlan = (d.sellPlan==null || d.sellPlan.isEmpty()) ? ""
                    : d.sellPlan.stream().map(ps -> "lot#"+ps.lotIndex+":"+ps.sellQty)
                    .collect(Collectors.joining(" | "));
            return r;
        }
        static DecisionRow na(String symbol, Position pos){
            DecisionRow r=new DecisionRow(); r.symbol=symbol; r.action="HOLD";
            r.reason="数据不足"; r.totalQty=pos.totalQty(); r.avgCost=pos.avgCost(); return r;
        }
        static DecisionRow err(String symbol, Position pos, String msg){
            DecisionRow r=new DecisionRow(); r.symbol=symbol; r.action="ERROR";
            r.reason=msg; r.totalQty=pos.totalQty(); r.avgCost=pos.avgCost(); return r;
        }
        static double opt(Double x){ return x==null? Double.NaN : x; }
    }

    static void writeCsv(List<DecisionRow> rows, String path) throws IOException {
        path=path+"_"+LocalDate.now().toString()+".csv";
        try(PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))){
            pw.println("Symbol,Date,LastClose,LastVol,AvgCost,TotalQty,Action,Reason,M10,M15,M20,ATR14,RSI14,Trail,LossMA,Adds,MarketOK,ReAddSignal,SellPlan");
            for (DecisionRow r : rows){
                pw.printf(Locale.US,"%s,%s,%.2f,%d,%.2f,%d,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%s%n",
                        r.symbol, nvl(r.date), r.lastClose, r.lastVol, r.avgCost, r.totalQty,
                        r.action, csvSafe(r.reason), r.m10,r.m15,r.m20,r.atr,r.rsi,r.trail,
                        r.lossMA, r.adds, r.marketOK, r.readd, csvSafe(r.sellPlan));
            }
        }
    }
    static String nvl(String s){ return s==null?"":s; }
    static String csvSafe(String s){ return s==null? "" : s.replace(",", " "); }
}
