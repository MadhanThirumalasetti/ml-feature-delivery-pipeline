-- fct_account_features.sql
-- Final ML-ready feature table per account, consumed directly
-- by the Scikit-learn forecasting model. Full lineage tracked
-- from raw Salesforce CRM ingestion through this mart layer.

with opportunities as (

    select * from {{ ref('stg_salesforce_opportunities') }}

),

-- Aggregate opportunity-level data up to account level
account_stats as (

    select
        account_id,
        count(opportunity_id)                           as total_deals,
        sum(amount_usd)                                 as total_pipeline_usd,
        avg(amount_usd)                                 as avg_deal_size_usd,
        min(close_date)                                 as first_close_date,
        max(close_date)                                 as last_close_date,

        -- Win rate: share of closed-won deals
        avg(case when stage_name = 'Closed Won' then 1.0 else 0.0 end) as win_rate,

        -- Days between first and last deal (account longevity signal)
        datediff('day', min(close_date), max(close_date)) as account_age_days,

        -- Average days to close per deal
        avg(datediff('day', created_at, close_date))    as avg_days_to_close

    from opportunities
    group by account_id

),

-- Rolling 90-day revenue window per account
rolling_features as (

    select
        account_id,
        close_date,
        sum(amount_usd) over (
            partition by account_id
            order by close_date
            rows between 89 preceding and current row
        ) as rolling_90d_revenue,

        count(opportunity_id) over (
            partition by account_id
            order by close_date
            rows between 89 preceding and current row
        ) as rolling_90d_deal_count

    from opportunities

),

latest_rolling as (

    select account_id, rolling_90d_revenue, rolling_90d_deal_count
    from rolling_features
    qualify row_number() over (partition by account_id order by close_date desc) = 1

),

final as (

    select
        s.account_id,
        s.total_deals,
        s.total_pipeline_usd,
        s.avg_deal_size_usd,
        s.win_rate,
        s.account_age_days,
        s.avg_days_to_close,
        r.rolling_90d_revenue,
        r.rolling_90d_deal_count,

        -- Feature freshness metadata
        current_timestamp()  as _feature_computed_at

    from account_stats s
    left join latest_rolling r
        on s.account_id = r.account_id

)

select * from final
