SELECT
    d_year,
    d_moy,
    SUM(ss_ext_sales_price) AS revenue,
    SUM(ss_net_profit) AS profit
FROM store_sales
JOIN date_dim
    ON ss_sold_date_sk = d_date_sk
GROUP BY d_year, d_moy
ORDER BY d_year, d_moy;
