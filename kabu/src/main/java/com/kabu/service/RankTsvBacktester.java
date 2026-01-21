package com.kabu.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * 从 window_rank_YYYY-MM-DD.tsv 里读取前 N 名股票，
 * 假设在 BUY_DATE 按当天第一个交易日的「开盘价」买入，
 * 持有到最近一根日K 的「收盘价」，计算盈亏。
 *
 * 依赖 TrendScreener.fetchChart1yDaily(...) / TrendScreener.Chart / TrendScreener.Bar / TrendScreener.JP
 */
public class RankTsvBacktester {

    // ===== 默认配置（可通过 main 参数覆盖） =====
    private static String TSV_PATH = "window_rank_2025-10-27.tsv";
    private static LocalDate BUY_DATE = LocalDate.of(2025, 10, 24); // 名义买入日
    private static int TOP_N = 10;              // 取前 N 名
    private static int SHARES_PER_STOCK = 100;  // 每只股票买入股数

    private static final ZoneId JP = TrendScreener.JP;

    private static class PLRow {
        String symbol;
        LocalDate buyDate, lastDate;
        double buyPrice, lastPrice;
        int shares;
        double pl, plPct;
    }

    public static void main(String[] args) throws Exception {
        // 参数: [tsvPath] [buyDate yyyy-MM-dd] [topN] [shares]
        if (args.length >= 1 && !args[0].isEmpty()) {
            TSV_PATH = args[0];
        }
        if (args.length >= 2 && !args[1].isEmpty()) {
            BUY_DATE = LocalDate.parse(args[1]);
        }
        if (args.length >= 3 && !args[2].isEmpty()) {
            TOP_N = Integer.parseInt(args[2]);
        }
        if (args.length >= 4 && !args[3].isEmpty()) {
            SHARES_PER_STOCK = Integer.parseInt(args[3]);
        }

        System.out.println("TSV 文件: " + TSV_PATH);
        System.out.println("名义买入日: " + BUY_DATE + "  (实际用该日及之后第一个交易日的开盘价)");
        System.out.println("取前 " + TOP_N + " 名股票, 每只 " + SHARES_PER_STOCK + " 股\n");

        List<String> symbols = loadSymbolsFromTsv(TSV_PATH, TOP_N);
        if (symbols.isEmpty()) {
            System.err.println("没有从 TSV 中读到任何股票代码");
            return;
        }

        System.out.println("股票列表: " + String.join(", ", symbols));
        System.out.println();

        List<PLRow> results = new ArrayList<>();
        for (String symbol : symbols) {
            PLRow r = backtestOne(symbol, BUY_DATE, SHARES_PER_STOCK);
            if (r != null) {
                results.add(r);
            }
        }

        if (results.isEmpty()) {
            System.out.println("所有股票都回测失败或无数据。");
            return;
        }

        // 按收益率从高到低排序
        results.sort(Comparator.comparingDouble((PLRow r) -> r.plPct).reversed());

        printResultTable(results);
        printSummary(results);
    }

    /**
     * 从 window_rank_*.tsv 读取前 topN 行的 Symbol 列。
     * 文件头应该是：Rank\tSymbol\tName\t...
     */
    private static List<String> loadSymbolsFromTsv(String path, int topN) throws IOException {
        List<String> list = new ArrayList<>();
        Path p = Paths.get(path);
        if (!Files.exists(p)) {
            throw new IOException("TSV 文件不存在: " + p.toAbsolutePath());
        }

        try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { // 跳过表头
                    first = false;
                    continue;
                }
                if (list.size() >= topN) break;
                line = line.trim();
                if (line.isEmpty()) continue;

                // 按制表符分隔
                String[] f = line.split("\t");
                if (f.length < 2) continue;
                String symbol = f[1].trim();
                if (!symbol.isEmpty()) {
                    list.add(symbol);
                }
            }
        }
        return list;
    }

    /**
     * 对单个股票回测：买入日 = buyDate 起，第一个交易日的开盘价；卖出价 = 最新一根日K 收盘价。
     */
    private static PLRow backtestOne(String symbol, LocalDate buyDate, int sharesPerStock) {
        try {
            TrendScreener.Chart chart = TrendScreener.fetchChart1yDaily(symbol);
            if (chart == null || chart.bars == null || chart.bars.isEmpty()) {
                System.out.println("[" + symbol + "] 无K线数据，跳过");
                return null;
            }

            TrendScreener.Bar buyBar = null;
            LocalDate realBuyDate = null;

            for (TrendScreener.Bar b : chart.bars) {
                LocalDate d = Instant.ofEpochSecond(b.ts).atZone(JP).toLocalDate();
                if (!d.isBefore(buyDate)) { // d >= buyDate
                    buyBar = b;
                    realBuyDate = d;
                    break;
                }
            }

            if (buyBar == null) {
                System.out.println("[" + symbol + "] 在 " + buyDate + " 之后没有交易日，跳过");
                return null;
            }

            TrendScreener.Bar lastBar = chart.bars.get(chart.bars.size() - 1);
            LocalDate lastDate = Instant.ofEpochSecond(lastBar.ts).atZone(JP).toLocalDate();

            double buyPrice = buyBar.open;
            double lastPrice = lastBar.close;

            if (buyPrice <= 0 || Double.isNaN(buyPrice) || Double.isNaN(lastPrice)) {
                System.out.println("[" + symbol + "] 买入价或现价非法，跳过");
                return null;
            }

            PLRow row = new PLRow();
            row.symbol = symbol;
            row.buyDate = realBuyDate;
            row.lastDate = lastDate;
            row.buyPrice = buyPrice;
            row.lastPrice = lastPrice;
            row.shares = sharesPerStock;
            row.pl = (lastPrice - buyPrice) * sharesPerStock;
            row.plPct = (lastPrice - buyPrice) / buyPrice * 100.0;

            return row;
        } catch (Exception e) {
            System.out.println("[" + symbol + "] 回测失败: " + e.getMessage());
            return null;
        }
    }

    private static void printResultTable(List<PLRow> rows) {
        // 简单表格输出
        System.out.println("=== 单股盈亏（按每只 " + rows.get(0).shares + " 股） ===");
        System.out.printf(
                "%-8s %-10s %-9s %-10s %-9s %6s %12s %8s%n",
                "Symbol", "BuyDate", "BuyPrice", "LastDate", "LastPrice", "Shares", "PL(JPY)", "PL(%)"
        );
        for (PLRow r : rows) {
            System.out.printf(
                    "%-8s %-10s %9.2f %-10s %9.2f %6d %12.2f %8.2f%n",
                    r.symbol,
                    r.buyDate,
                    r.buyPrice,
                    r.lastDate,
                    r.lastPrice,
                    r.shares,
                    r.pl,
                    r.plPct
            );
        }
        System.out.println();
    }

    private static void printSummary(List<PLRow> rows) {
        double totalCost = 0.0;
        double totalValue = 0.0;
        int win = 0, lose = 0, flat = 0;

        for (PLRow r : rows) {
            double cost = r.buyPrice * r.shares;
            double value = r.lastPrice * r.shares;
            totalCost += cost;
            totalValue += value;

            if (r.pl > 0) win++;
            else if (r.pl < 0) lose++;
            else flat++;
        }

        double totalPl = totalValue - totalCost;
        double overallPct = (totalCost > 0) ? (totalPl / totalCost * 100.0) : 0.0;

        double avgPct = rows.stream().mapToDouble(r -> r.plPct).average().orElse(0.0);

        System.out.println("=== 汇总 ===");
        System.out.printf("股票数量: %d  (盈利: %d, 亏损: %d, 持平: %d)%n",
                rows.size(), win, lose, flat);
        System.out.printf("总买入成本: %.2f 日元%n", totalCost);
        System.out.printf("当前市值:   %.2f 日元%n", totalValue);
        System.out.printf("总盈亏:     %.2f 日元%n", totalPl);
        System.out.printf("组合整体收益率: %.2f%%%n", overallPct);
        System.out.printf("等权平均收益率: %.2f%%%n", avgPct);
    }
}
