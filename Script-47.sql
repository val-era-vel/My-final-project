WITH games_payments_with_language AS (
    SELECT p.*, pu.language, pu.age
    FROM project.games_payments p
    JOIN project.games_paid_users pu ON p.user_id = pu.user_id
),

Monthly_Revenue AS (
    select
        DATE_TRUNC('month', payment_date) AS month,
        age, 
        language,
        user_id,
        SUM(revenue_amount_usd) AS total_revenue,
        COUNT(DISTINCT user_id) AS paid_users
    FROM games_payments_with_language
    GROUP BY DATE_TRUNC('month', payment_date), user_id, language, age 
),

New_MRR AS (
    SELECT
        gpwl.user_id,
        DATE_TRUNC('month', gpwl.payment_date) AS month,
        SUM(gpwl.revenue_amount_usd) AS revenue
    FROM games_payments_with_language gpwl
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
            THEN 1 ELSE 0
        END AS is_churned
    FROM games_payments_with_language
    GROUP BY user_id
),

Churned_Revenue AS (
    SELECT
        cu.last_payment_month,
        SUM(gpwl.revenue_amount_usd) AS churned_revenue
    FROM Churned_Users cu
    JOIN games_payments_with_language gpwl ON cu.user_id = gpwl.user_id
    WHERE cu.is_churned = 1
    AND DATE_TRUNC('month', gpwl.payment_date) = cu.last_payment_month
    GROUP BY cu.last_payment_month
),

Expansion_Contraction_MRR AS (
    WITH Monthly_User_Revenue AS (
        SELECT
            user_id,
            DATE_TRUNC('month', payment_date) AS month,
            SUM(revenue_amount_usd) AS revenue_amount_usd
        FROM games_payments_with_language
        GROUP BY user_id, DATE_TRUNC('month', payment_date)
    )
    SELECT
        user_id,
        month,
        revenue_amount_usd,
        LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY month) AS previous_revenue,
        CASE
            WHEN revenue_amount_usd > LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY month)
            THEN revenue_amount_usd - LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY month)
            ELSE 0
        END AS expansion_mrr,
        CASE
            WHEN revenue_amount_usd < LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY month)
            THEN LAG(revenue_amount_usd) OVER (PARTITION BY user_id ORDER BY month) - revenue_amount_usd
            ELSE 0
        END AS contraction_mrr
    FROM Monthly_User_Revenue
),
user_lifetime AS ( 
SELECT user_id, MIN(payment_date) AS first_payment_date, MAX(payment_date) AS last_payment_date, 
EXTRACT(MONTH FROM AGE(MAX(payment_date), MIN(payment_date))) AS LT_months
FROM games_payments_with_language 
GROUP BY user_id 
),
 
LTV AS (
    SELECT
        AVG(lt.LT_months) AS avg_lifetime_before_churn,
        AVG(mr.total_revenue) AS avg_total_revenue_per_user
    FROM user_lifetime lt
    JOIN Monthly_Revenue mr ON lt.user_id = mr.user_id

),

Previous_Month_Revenue AS (
    SELECT
        month,
        SUM(total_revenue) AS prev_month_revenue,
        SUM(paid_users) AS prev_month_paid_users
    FROM Monthly_Revenue
    GROUP BY month
)
SELECT
        mr.month::date,
        mr.age, 
        mr.paid_users,
        mr.language,
        mr.total_revenue AS MRR,
        COALESCE(nmr.revenue, 0) AS new_MRR,
        COUNT(DISTINCT cu.user_id) AS churned_users,
        COALESCE(cr.churned_revenue, 0) AS churned_revenue,
        COALESCE(ecm.expansion_mrr, 0) AS expansion_MRR,
        COALESCE(ecm.contraction_mrr, 0) AS contraction_MRR,
        COALESCE(prev_mr.prev_month_paid_users, 0) AS prev_month_paid_users,
        COALESCE(prev_mr.prev_month_revenue, 0) AS prev_month_revenue,
        ltv.avg_lifetime_before_churn,
        ltv.avg_total_revenue_per_user,
        CASE WHEN mr.paid_users > 0 THEN mr.total_revenue / mr.paid_users ELSE 0 END AS ARPPU,
        COUNT(DISTINCT nmr.user_id) AS new_paid_users,
        CASE WHEN prev_mr.prev_month_paid_users > 0 THEN COUNT(DISTINCT cu.user_id) * 1.0 / prev_mr.prev_month_paid_users ELSE 0 END AS churn_rate,
        CASE WHEN prev_mr.prev_month_revenue > 0 THEN COALESCE(cr.churned_revenue, 0) * 1.0 / prev_mr.prev_month_revenue ELSE 0 END AS revenue_churn_rate
    FROM Monthly_Revenue mr
    LEFT JOIN New_MRR nmr ON mr.month = nmr.month AND mr.user_id = nmr.user_id
    LEFT JOIN Churned_Users cu ON mr.month = cu.last_payment_month AND mr.user_id = cu.user_id
    LEFT JOIN Churned_Revenue cr ON mr.month = cr.last_payment_month
    LEFT JOIN Expansion_Contraction_MRR ecm ON mr.month = ecm.month AND mr.user_id = ecm.user_id
    LEFT JOIN Previous_Month_Revenue prev_mr ON mr.month = prev_mr.month + INTERVAL '1 month'
    JOIN LTV ltv ON TRUE
    GROUP BY
        mr.month, mr.user_id, mr.total_revenue, nmr.revenue, cr.churned_revenue, language, 
        mr.paid_users, prev_mr.prev_month_paid_users, prev_mr.prev_month_revenue, 
        ecm.expansion_mrr, ecm.contraction_mrr, ltv.avg_lifetime_before_churn,
        ltv.avg_total_revenue_per_user, mr.age 
    ORDER BY mr.month, mr.user_id