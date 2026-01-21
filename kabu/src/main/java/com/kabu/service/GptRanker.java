package com.kabu.service;

// ====== 追加的 import ======
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;

import com.openai.models.ChatModel;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;

import java.util.*;
import java.util.stream.Collectors;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;

public final class GptRanker {

    // GPT 模型（保持你原来设置）
    private static final ChatModel MODEL = ChatModel.GPT_5_2025_08_07;

    // 输出 TSV 保存路径（可改）
    public static final Path RANKED_TSV = Paths.get("candidates_ranked.tsv");

    public static class Candidate {
        String symbol;
        String name;
        String signal;
        double close;
        double sma10, sma20, sma50;
        double dist52w;
        double volMA20;
        double todayVol;
        double turnoverMA20;
        Double adx;
        Double diPlus;
        Double diMinus;
        Double atrPct;

        // —— 新增：消息面 —— //
        Double newsPct;     // 0-100，新闻看多概率
        String newsBrief;   // 抓到的标题摘要（便于追溯）

        Map<String,Object> toMap() {
            Map<String,Object> m = new LinkedHashMap<>();
            m.put("symbol", symbol);
            m.put("name", name);
            m.put("signal", signal);
            m.put("close", close);
            m.put("sma10", sma10);
            m.put("sma20", sma20);
            m.put("sma50", sma50);
            m.put("dist52w", dist52w);
            m.put("volMA20", volMA20);
            m.put("todayVol", todayVol);
            m.put("turnoverMA20", turnoverMA20);
            if (adx != null) m.put("adx", adx);
            if (diPlus != null) m.put("diPlus", diPlus);
            if (diMinus != null) m.put("diMinus", diMinus);
            if (atrPct != null) m.put("atrPct", atrPct);
            if (newsPct != null) m.put("newsPct", newsPct);
            if (newsBrief != null) m.put("newsBrief", newsBrief);
            return m;
        }
    }

    public static class Ranked {
        int rank;
        String symbol;
        double score;    // 0-100
        double entryLow;
        double entryHigh;
        double stop;
        String reason;
    }

    // 仅对“有信号”的行（你在 main 里已过滤掉 SETUP 了）做排名
    public static void rankOnlySignaled(List<TrendScreener.CsvRow> signaled, int topN) {
        if (signaled == null || signaled.isEmpty()) return ;

        // CsvRow -> Candidate
        List<Candidate> cands = signaled.stream().map(r -> {
            Candidate c = new Candidate();
            c.symbol = r.symbol;
            c.name = r.name;
            c.signal = r.signal;           // 例如 BREAKOUT / REB10D
            c.close = r.close;
            c.sma10 = r.sma10;
            c.sma20 = r.sma20;
            c.sma50 = r.sma50;
            c.dist52w = r.dist52w;
            c.volMA20 = r.volMA20;
            c.todayVol = r.todayVol;
            c.turnoverMA20 = r.turnMA20;   // 字段名不同，注意对应
            // 如果后续你把 ADX/DI/ATR 写进 CsvRow，这里顺便补上：
             c.adx = r.adx; c.diPlus = r.diPlus; c.diMinus = r.diMinus; c.atrPct = r.atrPct;
            return c;
        }).collect(Collectors.toList());

        // —— 新增：只对这些“有信号”的候选，抓新闻并得到 newsPct —— //
        Map<String, NewsScorer.NewsScore> score=  NewsScorer.fillNewsScoreBatch(cands);
        List<Candidate> candidates=new ArrayList<>();
        for(int i=0;i<cands.size();i++){
          if (score.get(cands.get(i).symbol)!=null){
              Candidate c = cands.get(i);
             c.newsPct=score.get(cands.get(i).symbol).getPct();
             c.newsBrief=score.get(cands.get(i).symbol).getReason();
             candidates.add(c);
          }

        }

        rankWithGPT(candidates, Math.min(topN, candidates.size()),score);
    }

    /** 用 OpenAI Responses API 对候选打分并按可建仓性排序（仅处理传入的 candidates） */
    public static void rankWithGPT(List<Candidate> candidates, int topN, Map<String, NewsScorer.NewsScore> score) {
        String reqId = java.util.UUID.randomUUID().toString();
        System.out.println("rankWithGPT CALL " + reqId + " cand=" + candidates.size());
        if (candidates == null || candidates.isEmpty()) return ;

        // 1) 组装给 GPT 的 TSV（新增 newsPct/newsBrief 列）
        String header = String.join("\t", Arrays.asList(
                "symbol","name","signal","close","sma10","sma20","sma50",
                "dist52w","volMA20","todayVol","turnoverMA20",
                "adx","diPlus","diMinus","atrPct",
                "newsPct","newsBrief"
        ));
        List<String> lines = new ArrayList<>();
        lines.add(header);
        for (Candidate c : candidates) {
            lines.add(String.join("\t", Arrays.asList(
                    nz(c.symbol), nz(c.name), nz(c.signal),
                    d(c.close), d(c.sma10), d(c.sma20), d(c.sma50),
                    d(c.dist52w), d(c.volMA20), d(c.todayVol), d(c.turnoverMA20),
                    d(c.adx), d(c.diPlus), d(c.diMinus), d(c.atrPct),
                    d(c.newsPct), nz(c.newsBrief)
            )));
        }
        String table = String.join("\n", lines);

        // 2) 评分与输出格式（把“消息面 10%”纳入说明）
        String system = ""
                + "你是纪律化的量化/技术面交易助手，只对给定候选做“最适合建仓”的排序。\n"
                + "评分(0-100)：\n"
                + " 40% 趋势质量：SMA10>SMA20>SMA50 加分；ADX 20~50 加分（>60 适度降权）；DI+>DI- 加分。\n"
                + " 30% 入场位置：\n"
                + "   - BREAKOUT：TodayVol/VolMA20 越高越好；dist52w 越小越佳；避免过热。\n"
                + "   - 其它回踩/二次启动：靠近并重新站上均线更佳（|close/sma20-1|<=5%）。\n"
                + " 15% 流动性：turnoverMA20 越大越好。\n"
                + " 10% 消息面：newsPct 越高越好（50 为中性）。\n"
                + "  5% 风险：atrPct 过大或 dist52w>0.2 降权。\n"
                + "缺失数据降低得分但不直接淘汰。\n"
                + "务必只返回 TSV，表头严格是：rank\tsymbol\tscore\tentryLow\tentryHigh\tstop\treason\n"
                + "entry/stop 给出数值（以收盘价/均线/ATR估算），不要输出多余文字或 Markdown。";

        String user = "TOPN=" + topN + "。以下是候选清单（TSV）：\n\n" + table;

        // 3) 调用 Responses API
        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        ResponseCreateParams params = ResponseCreateParams.builder()
                .model(MODEL)
                .instructions(system)
                .input(user)
                .build();

        String raw;
        try {
            Response resp = client.responses().create(params);
            raw = resp.toString();
            if (raw == null || raw.isBlank()) raw = resp.toString();
        } catch (Exception e) {
            throw new RuntimeException("调用 OpenAI 失败: " + e.getMessage(), e);
        }

//        // 4) 解析 TSV
//        String cleaned = stripCodeFence(raw).trim();
//        try { Files.writeString(RANKED_TSV, cleaned, StandardCharsets.UTF_8); } catch (Exception ignore) {}
//        return parseTsvRobust(cleaned);
    }

    // ===== TSV 解析 & 工具 =====
    private static List<Ranked> parseTsvRobust(String tsv) {
        List<Ranked> out = new ArrayList<>();
        if (tsv == null || tsv.isBlank()) return out;

        String[] lines = tsv.split("\\R");
        int start = 0;
        for (int i = 0; i < lines.length; i++) {
            String l = lines[i].trim().toLowerCase(Locale.ROOT);
            if (l.startsWith("rank\t") && l.contains("\tsymbol\t")) { start = i; break; }
        }
        for (int i = start + 1; i < lines.length; i++) {
            String row = lines[i].trim();
            if (row.isEmpty() || !row.contains("\t")) continue;
            String[] f = row.split("\\t");
            if (f.length < 7) continue;

            Ranked r = new Ranked();
            r.rank = safeParseInt(f[0]);
            r.symbol = f[1].trim();
            r.score = safeParseDouble(f[2]);
            r.entryLow = safeParseDouble(f[3]);
            r.entryHigh = safeParseDouble(f[4]);
            r.stop = safeParseDouble(f[5]);

            StringBuilder rs = new StringBuilder();
            for (int k = 6; k < f.length; k++) {
                if (k > 6) rs.append(' ');
                rs.append(f[k].trim());
            }
            r.reason = rs.toString();
            if (r.symbol != null && !r.symbol.isEmpty()) out.add(r);
        }
        out.sort(Comparator.<Ranked>comparingDouble(x -> -x.score).thenComparingInt(x -> x.rank));
        return out;
    }

    private static int safeParseInt(String s) {
        try { return Integer.parseInt(s.replaceAll(",", "").trim()); } catch (Exception e) { return 0; }
    }
    private static double safeParseDouble(String s) {
        try { return Double.parseDouble(s.replaceAll(",", "").trim()); } catch (Exception e) { return Double.NaN; }
    }
    private static String d(Double v) { return v == null ? "" : String.format(Locale.US, "%.6f", v); }
    private static String nz(String s) { return s == null ? "" : s; }
    private static String stripCodeFence(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.startsWith("```")) {
            int nl = t.indexOf('\n');
            if (nl >= 0) t = t.substring(nl + 1);
            int end = t.lastIndexOf("```");
            if (end >= 0) t = t.substring(0, end);
        }
        return t.trim();
    }
}
