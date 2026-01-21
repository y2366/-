package com.kabu.service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * 读取目录中的 candidates_YYYY-MM-DD.csv（示例列同你给出的），
 * 在给定 windowDays 内，按 趋势强度 / 信号强度 / 混合分 做排名。
 *
 * 兼容 Java 11。仅用 CSV 字段，不拉行情。
 */
public final class CsvWindowRanker {

    private CsvWindowRanker() {}

    // ========= 配置 =========
    public enum RankMode { TREND_ONLY, SIGNAL_ONLY, HYBRID }

    private static final String FILE_PREFIX = "candidates_";
    private static final String FILE_SUFFIX = ".csv";
    private static final DateTimeFormatter DATE_IN_NAME = DateTimeFormatter.ISO_LOCAL_DATE;

    // —— 信号分参数
    // —— 信号分参数（更偏爱“最近刚发生的突破”）——
    private static final double SIGNAL_BREAKOUT_BASE = 1.20;  // BREAKOUT 权重 ↑
    private static final double SIGNAL_REB10D_BASE   = 0.70;  // REB10D 权重 ↑
    private static final double SIGNAL_RECENCY_DECAY = 0.88;  // 旧信号衰减更快


    // —— 趋势分权重（总计=1.00）
    private static final double W_MA_STACK   = 0.35;  // Close>SMA10>SMA20>SMA50
    private static final double W_MA_SLOPE   = 0.20;  // SMA20/SMA50 抬头
    private static final double W_NEAR_52WH  = 0.25;  // 距52周高越近越好
    private static final double W_VOL_TREND  = 0.20;  // TodayVol 相对 VolMA20

    // ========= 主入口 =========
    /**
     * @param dir         存放 candidates_YYYY-MM-DD.csv 的目录
     * @param windowDays  统计最近 N 天（按文件名日期）
     * @param mode        排名模式：趋势/信号/混合
     * @param topN        取前 N 名（<=0 表示全部）
     */
    public static List<Ranked> rankFromDir(String dir, int windowDays, RankMode mode, int topN) throws IOException {
        List<Path> files = listCandidateFiles(dir);
        if (files.isEmpty()) return Collections.emptyList();

        // 找到最新日期
        LocalDate maxDate = files.stream()
                .map(CsvWindowRanker::dateFromFile)
                .max(LocalDate::compareTo)
                .orElse(LocalDate.now());

        LocalDate fromDate = maxDate.minusDays(Math.max(0, windowDays - 1));

        // 读取窗口内的文件，按 symbol 聚合
        Map<String, List<Row>> bySymbol = new HashMap<>();
        for (Path f : files) {
            LocalDate d = dateFromFile(f);
            if (d.isBefore(fromDate)) continue; // 只取窗口内
            readOneFile(f, bySymbol);
        }

        // 计算分数
        List<Ranked> out = new ArrayList<>();
        for (Map.Entry<String, List<Row>> e : bySymbol.entrySet()) {
            List<Row> rows = e.getValue();
            rows.sort(Comparator.comparing(r -> r.date)); // 升序

            // 趋势分：窗口内日均
            TrendScore ts = trendScore(rows);

            // 信号分：窗口内事件计数 + 新近度加权
            double ss = signalScore(rows, maxDate);

            double trend100  = ts.score * 100.0;
            double signal100 = ss * 100.0;
            double total;
            if (mode == RankMode.TREND_ONLY) {
                total = trend100;
            } else if (mode == RankMode.SIGNAL_ONLY) {
                total = signal100;
            } else {
                // HYBRID：更偏爱“最近刚有动作”的股票
                total = 0.4 * trend100 + 0.6 * signal100;
            }


            Row last = rows.get(rows.size() - 1);
            Ranked r = new Ranked();
            r.symbol = e.getKey();
            r.name   = last.name;
            r.latestDate = last.date;
            r.totalScore = total;
            r.trendScore = trend100;
            r.signalScore= signal100;

            // 参考执行区间（与此前约定一致）
            r.entryLow  = last.trigger20h;
            r.entryHigh = last.trigger20h * 1.03;
            r.stop      = (Double.isNaN(last.sma10) ? last.close * 0.97 : last.sma10);

            r.note = String.format(Locale.ROOT,
                    "stack=%.2f slope=%.2f near52w=%.2f vol=%.2f stage=%.2f brk=%d reb=%d",
                    ts.maStack, ts.maSlope, ts.near52w, ts.volTrend, ts.stage,
                    ts.breakoutCnt, ts.reboundCnt);


            out.add(r);
        }

        // 排序
        out.sort(Comparator
                .comparingDouble((Ranked r) -> -r.totalScore)
                .thenComparing(r -> r.symbol));

        if (topN > 0 && out.size() > topN) {
            return new ArrayList<>(out.subList(0, topN));
        }
        return out;
    }

    // 写出 TSV
    public static void writeTsv(List<Ranked> list, String path) {
        if (list == null || list.isEmpty()) return;
        if (path == null || path.isEmpty()) {
            path = "window_rank_" + LocalDate.now() + ".tsv";
        }
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(
                new FileOutputStream(path), StandardCharsets.UTF_8))) {
            pw.println("Rank\tSymbol\tName\tLatestDate\tTotal\tTrend\tSignal\tEntryLow\tEntryHigh\tStop\tNote");
            int i = 1;
            for (Ranked r : list) {
                pw.printf(Locale.US,
                        "%d\t%s\t%s\t%s\t%.1f\t%.1f\t%.1f\t%.2f\t%.2f\t%.2f\t%s%n",
                        i++, r.symbol, safe(r.name), r.latestDate,
                        r.totalScore, r.trendScore, r.signalScore,
                        r.entryLow, r.entryHigh, r.stop,
                        r.note.replace('\t', ' '));
            }
        } catch (Exception e) {
            System.err.println("写TSV失败: " + e.getMessage());
        }
    }

    // ========= 评分实现 =========
    private static TrendScore trendScore(List<Row> rows) {
        int n = rows.size();
        double stackSum = 0.0;
        double slopeCnt = 0.0, slopeSum = 0.0; // 两条均线
        double nearSum  = 0.0;
        double volSum   = 0.0;
        double stageSum = 0.0;                 // NEW：扩张度（相对 50 日线）

        int breakoutCnt = 0, reboundCnt = 0;

        for (int i = 0; i < n; i++) {
            Row r = rows.get(i);

            // MA 堆叠：Close>SMA10>SMA20>SMA50
            int stack = 0;
            if (!nan(r.close) && !nan(r.sma10) && r.close > r.sma10) stack++;
            if (!nan(r.sma10) && !nan(r.sma20) && r.sma10 > r.sma20) stack++;
            if (!nan(r.sma20) && !nan(r.sma50) && r.sma20 > r.sma50) stack++;
            stackSum += (stack / 3.0);

            // 均线斜率：SMA20/SMA50 较昨日抬头
            if (i > 0) {
                Row p = rows.get(i - 1);
                if (!nan(r.sma20) && !nan(p.sma20)) { slopeSum += (r.sma20 > p.sma20 ? 1.0 : 0.0); slopeCnt++; }
                if (!nan(r.sma50) && !nan(p.sma50)) { slopeSum += (r.sma50 > p.sma50 ? 1.0 : 0.0); slopeCnt++; }
            }

            // 距 52 周高：1 - (Dist52W / MAX_DIST_52W)，再裁剪到 [0,1]
            double near = 1.0 - (r.dist52w / Math.max(TrendScreener.MAX_DIST_52W, 1e-6));
            nearSum += clamp01(near);

            // 量能强弱：TodayVol / (VolMA20 * 1.2) 裁剪到 [0,1]
            if (!nan(r.todayVol) && !nan(r.volMA20) && r.volMA20 > 0) {
                double v = r.todayVol / (r.volMA20 * 1.2);
                volSum += clamp01(v);
            }

            // NEW: 扩张度（相对 50 日线），越接近 50 日线记分越高
            double stage = 1.0;
            if (!nan(r.close) && !nan(r.sma50) && r.sma50 > 0) {
                double ext = (r.close / r.sma50) - 1.0; // 高出 50 日线的比例
                if (ext <= 0.05) {
                    stage = 1.0;    // 刚离开 50 日线：典型早期
                } else if (ext <= 0.15) {
                    stage = 0.9;    // 中期
                } else if (ext <= 0.30) {
                    stage = 0.6;    // 有点飞
                } else {
                    stage = 0.3;    // 已经飞很高，属于晚期
                }
            }
            stageSum += stage;

            // 统计事件个数（用于 note）
            if ("BREAKOUT".equalsIgnoreCase(r.signal)) breakoutCnt++;
            if ("REB10D".equalsIgnoreCase(r.signal))  reboundCnt++;
        }

        double maStackAvg = stackSum / n;
        double maSlopeAvg = (slopeCnt > 0 ? (slopeSum / slopeCnt) : 0.0);
        double nearAvg    = nearSum  / n;
        double volAvg     = volSum   / n;
        double stageAvg   = stageSum / n;

        // 原始趋势分
        double score = W_MA_STACK * maStackAvg
                + W_MA_SLOPE * maSlopeAvg
                + W_NEAR_52WH * nearAvg
                + W_VOL_TREND * volAvg;

        // NEW：用阶段因子“打折”，早期票 (stage≈1) 基本不打折，过度扩张的票明显降分
        score = score * (0.5 + 0.5 * stageAvg); // 你也可以改成 score *= stageAvg，看你想压多狠

        TrendScore ts = new TrendScore();
        ts.score       = clamp01(score);
        ts.maStack     = maStackAvg;
        ts.maSlope     = maSlopeAvg;
        ts.near52w     = nearAvg;
        ts.volTrend    = volAvg;
        ts.stage       = stageAvg;
        ts.breakoutCnt = breakoutCnt;
        ts.reboundCnt  = reboundCnt;
        return ts;
    }


    private static double signalScore(List<Row> rows, LocalDate maxDate) {
        double weighted = 0.0;

        boolean breakoutToday = false;        // 今天有没有 BREAKOUT
        boolean breakoutBeforeToday = false;  // 窗口内是否曾经有过更早的 BREAKOUT

        for (Row r : rows) {
            long age = Math.max(0, ChronoUnit.DAYS.between(r.date, maxDate)); // 越近 age 越小
            double rec = Math.pow(SIGNAL_RECENCY_DECAY, age);

            String sig = (r.signal == null ? "" : r.signal.toUpperCase(Locale.ROOT));
            boolean isBrk = "BREAKOUT".equals(sig);
            boolean isReb = "REB10D".equals(sig);

            if (isBrk) {
                weighted += SIGNAL_BREAKOUT_BASE * rec;
                if (r.date.equals(maxDate)) {
                    breakoutToday = true;
                } else {
                    breakoutBeforeToday = true;
                }
            } else if (isReb) {
                weighted += SIGNAL_REB10D_BASE * rec;
            }
        }

        // NEW：如果“今天是窗口内第一次出现 BREAKOUT”，说明是刚启动，额外加成
        if (breakoutToday && !breakoutBeforeToday) {
            weighted *= 1.3; // 你可以改成 1.2/1.5 自己微调
        }

        // 软归一：1 - e^(-x) ∈ [0,1)
        return clamp01(1.0 - Math.exp(-weighted));
    }

    // ========= 读取 CSV =========
    private static void readOneFile(Path file, Map<String, List<Row>> bySymbol) throws IOException {
        LocalDate fileDate = dateFromFile(file);
        try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; } // 跳表头
                line = line.trim();
                if (line.isEmpty()) continue;
                // 简单 split（你的写出逻辑已将逗号替换为空格，安全）
                String[] f = line.split(",", -1);
                if (f.length < 14) continue;

                Row r = new Row();
                r.symbol     = f[0].trim();
                r.name       = f[1].trim();
                r.date       = parseDateSafe(f[2], fileDate);
                r.close      = parseD(f[3]);
                r.sma10      = parseD(f[4]);
                r.sma20      = parseD(f[5]);
                r.sma50      = parseD(f[6]);
                /* 52WHigh = f[7] 不用 */
                r.dist52w    = parseD(f[8]);
                r.volMA20    = parseD(f[9]);
                /* TurnoverMA20 = f[10] 不用 */
                r.trigger20h = parseD(f[11]);
                r.todayVol   = parseD(f[12]);
                r.signal     = (f.length >= 14 ? f[13].trim() : "");

                bySymbol.computeIfAbsent(r.symbol, k -> new ArrayList<>()).add(r);
            }
        }
    }

    private static List<Path> listCandidateFiles(String dir) throws IOException {
        List<Path> out = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path p : ds) {
                String name = p.getFileName().toString();
                if (name.startsWith(FILE_PREFIX) && name.endsWith(FILE_SUFFIX)) {
                    out.add(p);
                }
            }
        }
        out.sort(Comparator.comparing(CsvWindowRanker::dateFromFile)); // 升序
        return out;
    }

    private static LocalDate dateFromFile(Path p) {
        String name = p.getFileName().toString();
        int s = FILE_PREFIX.length();
        int e = name.length() - FILE_SUFFIX.length();
        String d = name.substring(s, e);
        try {
            return LocalDate.parse(d, DATE_IN_NAME);
        } catch (Exception ex) {
            return LocalDate.MIN;
        }
    }

    private static LocalDate parseDateSafe(String s, LocalDate fallback) {
        try { return LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE); }
        catch (Exception ignore) { return fallback; }
    }

    private static double parseD(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return Double.NaN; }
    }

    private static boolean nan(double x) { return Double.isNaN(x) || Double.isInfinite(x); }

    private static double clamp01(double x) {
        if (x < 0) return 0;
        if (x > 1) return 1;
        return x;
    }

    private static String safe(String s) { return s == null ? "" : s.replace('\t',' ').replace('\n',' '); }

    // ========= DTO =========
    private static final class Row {
        String symbol, name, signal;
        LocalDate date;
        double stage;              // 新增：相对 50 日线的“阶段”评分（越早期越接近 1）
        double close, sma10, sma20, sma50, dist52w, volMA20, trigger20h, todayVol;
    }

    private static final class TrendScore {
        double score;
        double maStack, maSlope, near52w, stage,volTrend;
        int breakoutCnt, reboundCnt;
    }

    public static final class Ranked {
        public String symbol, name, note;
        public LocalDate latestDate;
        public double totalScore, trendScore, signalScore;
        public double entryLow, entryHigh, stop;
    }

    // ========= 简单命令行（可选）=========
    // 用法: java CsvWindowRanker . 10 HYBRID 50 window_rank.tsv
    public static void main(String[] args) throws Exception {
        String dir = args.length > 0 ? args[0] : ".";
        int window = args.length > 1 ? Integer.parseInt(args[1]) : 15;
        RankMode mode = args.length > 2 ? RankMode.valueOf(args[2]) : RankMode.HYBRID;
        int topN = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        String out = args.length > 4 ? args[4] : "window_rank_" + LocalDate.now() + ".tsv";

        List<Ranked> ranked = rankFromDir(dir, window, mode, topN);
        writeTsv(ranked, out);

        System.out.println("完成： " + out + "  (Top=" + ranked.size() + ")");
    }
}
