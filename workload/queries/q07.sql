SELECT
    d_year,
    i_category,
    SUM(ss_net_profit) AS profit
FROM store_sales
JOIN item
    ON ss_item_sk = i_item_sk
JOIN date_dim
    ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY d_year, i_category
ORDER BY profit DESC;
