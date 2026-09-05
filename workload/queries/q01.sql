SELECT
    i_category,
    SUM(ss_ext_sales_price) AS revenue,
    SUM(ss_net_profit) AS profit
FROM store_sales
JOIN item
    ON ss_item_sk = i_item_sk
GROUP BY i_category
ORDER BY revenue DESC;
