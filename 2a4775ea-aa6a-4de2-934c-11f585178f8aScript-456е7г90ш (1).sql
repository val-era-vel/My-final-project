WITH games_payments_with_language AS (
    SELECT
        p.*,
        pu.language
    FROM
        project.games_payments p
    JOIN
        project.games_paid_users pu ON p.user_id = pu.user_id
),
Monthly_Revenue AS (
    SELECT
        DATE_TRUNC('month', payment_date) AS month,
        user_id,
        SUM(revenue_amount_usd) AS total_revenue,
        COUNT(DISTINCT user_id) AS paid_users
    FROM
        games_payments_with_language
    GROUP BY
        DATE_TRUNC('month', payment_date), user_id
),
New_MRR AS (
    SELECT
        gpwl.user_id,
        DATE_TRUNC('month', gpwl.payment_date) AS month,
        SUM(gpwl.revenue_amount_usd) AS revenue
    FROM
        games_payments_with_language gpwl
    JOIN (
        SELECT user_id, MIN(DATE_TRUNC('month', payment_date)) AS first_payment_month
        FROM games_payments_with_language
        GROUP BY user_id
    ) AS new_users
    ON gpwl.user_id = new_users.user_id
    AND DATE_TRUNC('month', gpwl.payment_date) = new_users.first_payment_month
    GROUP BY gpwl.user_id, month
),
Churned_Users AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MAX(payment_date)) AS last_payment_month,
        CASE
            WHEN LEAD(MAX(payment_date)) OVER (PARTITION BY user_id ORDER BY MAX(payment_date)) IS NULL
            OR LEAD(MAX(payment_date)) OVER (PARTITION BY user_id ORDER BY MAX(payment_date)) > MAX(payment_date) + INTERVAL '1 month'
            THEN 1
            ELSE 0
        END AS is_churned
    FROM
        games_payments_with_language
    GROUP BY user_id
),
Churned_Revenue AS (
    SELECT
        DATE_TRUNC('month', p.payment_date) AS month,
        p.user_id,
        SUM(p.revenue_amount_usd) AS churned_revenue
    FROM
        games_payments_with_language p
    JOIN Churned_Users cu ON p.user_id = cu.user_id
    WHERE cu.is_churned = 1
    GROUP BY DATE_TRUNC('month', p.payment_date), p.user_id
),
Expansion_Contraction_MRR AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date) AS month,
        revenue_amount_usd,
        LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY payment_date) AS previous_revenue,
        CASE WHEN revenue_amount_usd > LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY payment_date)
            THEN revenue_amount_usd - LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY payment_date)
            ELSE 0 END AS expansion_mrr,
        CASE WHEN revenue_amount_usd < LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY payment_date)
            THEN LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY payment_date) - revenue_amount_usd
            ELSE 0 END AS contraction_mrr
    FROM
        games_payments_with_language
),
Customer_Lifetime AS (
    SELECT
        user_id,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date,
        DATE_PART('month', AGE(MAX(payment_date), MIN(payment_date))) + 1 AS lifetime_months,
        SUM(revenue_amount_usd) AS total_revenue
    FROM
        games_payments_with_language
    GROUP BY user_id
),
LTV AS (
    SELECT
        AVG(total_revenue) AS avg_ltv,
        AVG(lifetime_months) AS avg_lifetime
    FROM
        Customer_Lifetime
)
SELECT
    mr.month,
    mr.user_id,
    mr.total_revenue AS MRR,
    COALESCE(nmr.revenue, 0) AS new_MRR,
    COALESCE(cu.is_churned, 0) AS churned_users,
    COALESCE(cr.churned_revenue, 0) AS churned_revenue,
    COALESCE(SUM(ecm.expansion_mrr), 0) AS expansion_MRR,
    COALESCE(SUM(ecm.contraction_mrr), 0) AS contraction_MRR,
    ltv.avg_ltv,
    ltv.avg_lifetime
FROM
    Monthly_Revenue mr
LEFT JOIN New_MRR nmr ON mr.month = nmr.month AND mr.user_id = nmr.user_id
LEFT JOIN Churned_Users cu ON mr.month = cu.last_payment_month AND mr.user_id = cu.user_id
LEFT JOIN Churned_Revenue cr ON mr.month = cr.month AND mr.user_id = cr.user_id
LEFT JOIN Expansion_Contraction_MRR ecm ON mr.month = ecm.month AND mr.user_id = ecm.user_id
JOIN LTV ltv ON TRUE
GROUP BY
    mr.month, mr.user_id, ltv.avg_ltv, ltv.avg_lifetime, mr.total_revenue, nmr.revenue, cu.is_churned, cr.churned_revenue
ORDER BY
    mr.month, mr.user_id