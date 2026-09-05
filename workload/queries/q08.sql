SELECT
    ss_store_sk,
    SUM(ss_ext_sales_price) AS revenue,
    SUM(ss_net_profit) AS profit
FROM store_sales
GROUP BY ss_store_sk
ORDER BY revenue DESC;
