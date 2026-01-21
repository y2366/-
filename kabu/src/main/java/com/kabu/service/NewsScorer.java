package com.kabu.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.models.responses.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.ChatModel;

import java.util.regex.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/** 只处理传入的候选；为每只股票生成新闻摘要，并让 GPT 一次性打出 0-100 的看多百分比。 */
public final class NewsScorer {

    // 用便宜一些的模型做情绪分析，减少成本；也可以用你同一个模型
    private static final ChatModel MODEL_FOR_NEWS = ChatModel.GPT_4_1_MINI;
    private static final ObjectMapper JSON = new ObjectMapper();


    public static Map<String, NewsScore>  fillNewsScoreBatch(List<GptRanker.Candidate> cands) {
        Map<String, NewsScore> scores=new HashMap<>();
        if (cands == null || cands.isEmpty()) return scores;

        // 1) 并发抓网页 -> 提炼每只的 “标题摘要”
        Map<String, String> digestBySymbol = fetchNewsDigests(cands);

        // 把摘要写回去，空的置为中性
        for (GptRanker.Candidate c : cands) {
            String dg = digestBySymbol.getOrDefault(c.symbol, "");
            c.newsBrief = dg;
            c.newsPct = 50.0; // 默认中性，等会儿再被 GPT 覆盖
        }

        // 2) 组合成一个批量 prompt，让 GPT 对所有有摘要的股票一次性输出 TSV: symbol\tpct\treason
        List<GptRanker.Candidate> withDigest = cands.stream()
                .filter(x -> x.newsBrief != null && !x.newsBrief.isBlank())
                .collect(Collectors.toList());
        if (withDigest.isEmpty()) return scores;

        StringBuilder user = new StringBuilder();
        user.append("请对以下每只股票，根据“仅有的新闻标题摘要”，评估短期(5-10个交易日)看多概率百分比(0-100)，")
                .append("并给出不超过15字的中文理由。只输出 TSV：symbol\\tbullishPct\\treason，每行一只，按给出顺序；")
                .append("若信息不足请给 50 并写 '信息不足'。\n\n");

        for (GptRanker.Candidate c : withDigest) {
            user.append("### ").append(c.symbol).append("\n").append(c.newsBrief).append("\n\n");
        }

        String system = "你是金融新闻情绪分析助手。不要编造新闻，只依据给出的标题列表判断。严格输出 TSV。";

        OpenAIClient client = OpenAIOkHttpClient.fromEnv();
        ResponseCreateParams params = ResponseCreateParams.builder()
                .model(MODEL_FOR_NEWS)
                .instructions(system)
                .input(user.toString())
                .build();


        try {
            Response resp = client.responses().create(params);
            String respStr = String.valueOf(resp);
             scores =
                    parseFromResponseToString(respStr);
        } catch (Exception e) {
            System.err.println("NewsScorer 调用 GPT 失败: " + e.getMessage());
            return scores;
        }
           return scores;


    }

    // —— 抓 minkabu 新闻页，提炼最多 8 条 “- 标题 (时间?)” —— //
// 依赖：jsoup、Jackson（或你已有的 JSON 库）
// import com.fasterxml.jackson.databind.*;
// import org.jsoup.*;
// import org.jsoup.nodes.*;
// import org.jsoup.select.*;


    /**
     * 从 kabutan 抓取每只股票最近的新闻标题（仅标题+时间），返回 map: symbol -> 多行摘要
     * - 优先调用 JSON: https://kabutan.jp/js/json/announce_free.json?code={code}&page={p}
     * - 若失败则解析 https://kabutan.jp/stock/news?code={code} 的 HTML
     *
     * @param cands  有信号的候选
     * @return       每只股票“新闻摘要”的 map，用于拼进 GPT 提示词
     */
// 需要：Jsoup + 你的 HttpTool.get(String url, Map<String,String> headers, int connMs, int readMs)

    private static Map<String, String> fetchNewsDigests(List<GptRanker.Candidate> cands) {
        final int PAGES = 2;      // 每只股抓多少页
        final int MAX_LINES = 10; // 每只股最多保留多少条
        final String NMODE = "1"; // 0=全部，想只看“開示”可改 3（你也可根据页面 Tab 的 nmode 调整）

        Map<String, String> out = new ConcurrentHashMap<>();
        int threads = Math.min(6, Math.max(2, cands.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();

        for (GptRanker.Candidate c : cands) {
            futures.add(pool.submit(() -> {
                String code = (c.symbol == null ? "" : c.symbol.replaceAll("\\D", ""));
                if (code.isEmpty()) return;

                List<String> lines = new ArrayList<>();

                for (int p = 1; p <= PAGES && lines.size() < MAX_LINES; p++) {
                    String url = "https://kabutan.jp/stock/news?code=" + code + "&nmode=" + NMODE + "&page=" + p;
                    try {
                        String html = HttpTool.get(
                                url,
                                Map.of("User-Agent","Mozilla/5.0",
                                        "Accept","text/html,application/xhtml+xml"),
                                10000, 10000);
                        Document doc = Jsoup.parse(html, url);

                        // 兼容两种表格：s_news_list（个股页）/ g_news_list（部分列表页）
                        Elements rows = doc.select("table.s_news_list tr, table.g_news_list tr");
                        for (Element tr : rows) {
                            // 时间
                            String when = "";
                            Element t = tr.selectFirst("td.news_time time");
                            if (t != null) {
                                String dt = t.hasAttr("datetime") ? t.attr("datetime") : t.text();
                                if (dt != null && !dt.isBlank()) when = dt.replace('\u00A0',' ').trim();
                            } else {
                                Element td = tr.selectFirst("td.news_time");
                                if (td != null) when = td.text().replace('\u00A0',' ').trim();
                            }

                            // 分类（材料/開示/特集/テク…）
                            String cat = "";
                            Element catEl = tr.selectFirst("div.newslist_ctg");
                            if (catEl != null) cat = catEl.text().replace('\u00A0',' ').trim();

                            // 标题：优先 IR/開示（disclosures），否则普通新闻（/news/）
                            Element a = tr.selectFirst("td.td_kaiji a[href*=disclosures], td a[href^=/stock/news], td a[href^=/news/], td a[href^=https://kabutan.jp/news/]");
                            if (a == null) continue;

                            String title = a.text().replace('\u00A0',' ').trim();
                            if (title.isBlank()) continue;

                            String line = "・" + (when.isBlank() ? "" : when + " ")
                                    + (cat.isBlank() ? "" : "[" + cat + "] ")
                                    + title;
                            lines.add(line);
                            if (lines.size() >= MAX_LINES) break;
                        }
                    } catch (Exception e) {
                        System.err.println("抓取 kabutan 失败 " + c.symbol + " p=" + p + " : " + e.getMessage());
                    }
                }

                if (!lines.isEmpty()) {
                    // 去重并写回
                    LinkedHashSet<String> dedup = new LinkedHashSet<>(lines);
                    out.put(c.symbol, String.join("\n", dedup));
                }
            }));
        }

        for (Future<?> f : futures) try { f.get(30, TimeUnit.SECONDS); } catch (Exception ignore) {}
        pool.shutdown();
        return out;
    }
    public static final class NewsScore {
        private final double pct;
        private final String reason;
        public NewsScore(double pct, String reason) {
            this.pct = pct;
            this.reason = reason;
        }
        public double getPct() { return pct; }
        public String getReason() { return reason; }
        @Override public String toString() {
            return "NewsScore{pct=" + pct + ", reason='" + reason + "'}";
        }
    }

    // 跨行、非贪婪地抓取  text= ...... , type=output_text 之间的内容
    private static final Pattern P_OUTPUT_TEXT =
            Pattern.compile("text=(.*?)(?:,\\s*type=output_text|\\}\\])", Pattern.DOTALL);

    /** 从 Response.toString() 抽出所有 output_text 段并拼成 TSV（可能有多段） */
    public static String extractTsvFromResponseToString(String respStr) {
        if (respStr == null) return "";
        StringBuilder sb = new StringBuilder();
        Matcher m = P_OUTPUT_TEXT.matcher(respStr);
        while (m.find()) {
            String chunk = m.group(1);
            if (chunk != null && !chunk.trim().isEmpty()) {
                sb.append(chunk.trim()).append('\n');
            }
        }
        return sb.toString().trim();
    }

    /** 解析 TSV -> Map<symbol, NewsScore>；每行最多拆成 3 段（避免理由被再切） */
    public static Map<String, NewsScore> parseNewsScoresTsv(String tsv) {
        Map<String, NewsScore> map = new LinkedHashMap<>();
        if (tsv == null || tsv.trim().isEmpty()) return map;

        String[] lines = tsv.split("\\r?\\n");
        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("```")) continue; // 忽略 code fence
            String[] f = line.split("\\t", 3);
            if (f.length < 2) continue;

            String symbol = f[0].trim();
            String pctStr = f[1].replace("%", "").trim();
            String reason = (f.length >= 3) ? f[2].trim() : "";

            if (symbol.isEmpty() || pctStr.isEmpty()) continue;
            try {
                double pct = Double.parseDouble(pctStr);
                map.put(symbol, new NewsScore(pct, reason));
            } catch (NumberFormatException ignore) {
                // 忽略无法解析的行
            }
        }
        return map;
    }

    /** 便捷：一步到位，从 Response.toString() 得到 Map */
    public static Map<String, NewsScore> parseFromResponseToString(String respStr) {
        String tsv = extractTsvFromResponseToString(respStr);
        return parseNewsScoresTsv(tsv);
    }
    private NewsScorer() {}
}
