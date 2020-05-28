WITH
    product_info              AS (
        SELECT
            confirmation_number,
            op.product_code,
            start_market_code,
            end_market_code,
            op.start_ts,
            op.end_ts,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.start_ts)    start_rn,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.end_ts DESC) end_rn

        FROM
            bronze.order o
                INNER JOIN bronze.order_product op
                           ON op.order_sfid = o.order_sfid
                LEFT JOIN  bronze.order_product_location
                           ON order_product_location.order_product_sfid = op.order_product_sfid
                LEFT JOIN  bronze.location
                           ON order_product_location.location_sfid = location.location_sfid
    ),
    order_markets             AS (
        SELECT
            start_info.confirmation_number,
            max(start_info.start_market_code) AS start_market_code,
            max(end_info.end_market_code)     AS end_market_code
        FROM
            product_info start_info
                JOIN product_info end_info
                     ON end_info.confirmation_number = start_info.confirmation_number

        WHERE
            (start_info.start_rn = 1 OR end_info.end_rn = 1)
        GROUP BY 1
    ),

    booked_revenue_info       AS (
        SELECT
            confirmation_number,
            sum(CASE
                    WHEN pricing_strategy_code = 'FIXED'       THEN unitprice
                    WHEN o.affiliated_partner_sfid IS NOT NULL THEN unitprice
                    ELSE (unitprice * estimated_duration)
                END)                                                                         booked_revenue,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.created_ts)      created_start_rn,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.created_ts DESC) created_end_rn
        FROM
            bronze.order o
                INNER JOIN bronze.order_product op
                           ON op.order_sfid = o.order_sfid
                LEFT JOIN  bronze.product p2
                           ON p2.product_code = op.product_code
        WHERE
            family NOT IN ('Reduction', 'Tip') AND
            package_quote_uuid IS NULL

        GROUP BY
            1, op.created_ts
    ),

    booked_revenue            AS (
        SELECT
            confirmation_number,
            max(CASE WHEN created_start_rn = 1 THEN booked_revenue END) AS booked_revenue_at_booking,
            max(CASE WHEN created_end_rn = 1 THEN booked_revenue END)   AS booked_revenue_at_move_start
        FROM
            booked_revenue_info
        WHERE
            (created_start_rn = 1 OR created_end_rn = 1)
        GROUP BY 1
    ),
    -- get billable duration when it is not written
billable_package_duration AS (
        SELECT
            o.confirmation_number
            , max(billable_duration) AS billable_duration_calc
        FROM
            bronze."order" o
                INNER JOIN bronze.order_product op
                           ON o.order_sfid = op.order_sfid
                LEFT JOIN bronze.product p2
                          ON p2.product_code = op.product_code
        WHERE
              op.order_product_status NOT IN ('REMOVED', 'CANCELLED')
          AND o.order_status = 'COMPLETED'
          AND package_quote_uuid IS NOT NULL
        GROUP BY 1
    ), 
 non_packages_legacy as (
        select confirmation_number
            , order_product_uuid
            , op.product_code
            , unitprice
            , pricing_strategy_code
            , billable_duration
            , p2.family
        from bronze.order_product op
                join bronze.order o on o.order_sfid=op.order_sfid
                join bronze.product p2 on p2.product_code = op.product_code
        where confirmation_number ilike '%ord%'
            and order_product_status not ilike '%removed%'
            and p2.family not in ('Reduction')
            and op.product_code not in ( 'TIP','LEGACY_FEE')
            and package_quote_uuid is null
            and order_status = 'COMPLETED'
    ),
    -- executed revenue is at the package level and add ons. It is calculated as billable duration * hours for HOURLY
    -- price strategies and is the unit price for fixed price strategies.
    -- Price strategies are set at the order item level
legacy_executed as (
        select npl.confirmation_number
            , sum( 
                   case when pricing_strategy_code = 'FIXED' then unitprice
                         when pricing_strategy_code = 'HOURLY' then unitprice*billable_duration 
                    end) as executed_revenue
        from non_packages_legacy npl
        where confirmation_number not in (
                                                                    select confirmation_number from bronze.order_product op
                                                                                    join bronze.order o on o.order_sfid=op.order_sfid
                                                                                    join bronze.product p2 on p2.product_code = op.product_code
                                                                    where confirmation_number ilike '%ord%'
                                                                                and order_product_status not ilike '%removed%'
                                                                                and p2.family not in ('Reduction')
                                                                                and package_quote_uuid is not null)
        group by 1
    ),

executed AS (
        SELECT
            o.confirmation_number
            , sum(CASE
                    WHEN pricing_strategy_code = 'FIXED' THEN unitprice
                    WHEN billable_duration IS NULL THEN billable_duration_calc * unitprice
                    ELSE billable_duration_calc * unitprice
                END) AS executed_revenue
        FROM
            bronze."order" o
                INNER JOIN bronze.order_product op
                           ON o.order_sfid = op.order_sfid
                LEFT JOIN billable_package_duration bpd
                           ON bpd.confirmation_number = o.confirmation_number
                LEFT JOIN bronze.product p2
                          ON p2.product_code = op.product_code
        WHERE
              op.order_product_status NOT IN ('REMOVED', 'CANCELLED')
          AND family NOT IN ('Reduction', 'Tip')
          AND o.order_status = 'COMPLETED'
          AND package_quote_uuid IS NULL
        GROUP BY 1
    ),
executed_revenue as (
        select executed.confirmation_number
              ,sum(coalesce(executed.executed_revenue,0)+coalesce(legacy_executed.executed_revenue,0)) as executed_revenue
        from executed
                full join legacy_executed on legacy_executed.confirmation_number = executed.confirmation_number
        group by 1
    ),


    order_times_info          AS (
        SELECT
            confirmation_number,
            op.created_ts,
            min(op.start_ts) AS                                                              start_ts,
            max(op.end_ts)   AS                                                              end_ts,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.created_ts)      created_start_rn,
            row_number() OVER (PARTITION BY confirmation_number ORDER BY op.created_ts DESC) created_end_rn
        FROM
            bronze.order o
                INNER JOIN bronze.order_product op
                           ON op.order_sfid = o.order_sfid
                LEFT JOIN  bronze.product p2
                           ON p2.product_code = op.product_code
        WHERE
            family NOT IN ('Reduction', 'Tip') AND
            op.start_ts IS NOT NULL
        GROUP BY 1, op.created_ts
        ORDER BY 1
    ),

    order_times               AS (
        SELECT
            confirmation_number,
            min(CASE WHEN created_start_rn = 1 THEN start_ts END) AS booked_start_time_at_booking,
            max(CASE WHEN created_start_rn = 1 THEN end_ts END)   AS booked_end_time_at_booking,
            min(CASE WHEN created_end_rn = 1 THEN start_ts END)   AS booked_start_time_at_move_start,
            max(CASE WHEN created_end_rn = 1 THEN end_ts END)     AS booked_end_time_at_move_start
        FROM
            order_times_info
        WHERE
            (created_start_rn = 1 OR created_end_rn = 1)
        GROUP BY 1
    ),

    job_slot_times            AS (
        SELECT
            o.confirmation_number,
            row_number() OVER (PARTITION BY op.order_product_sfid ORDER BY minimum_qualification_level DESC) AS rn,
            js.clock_in_ts,
            js.clock_out_ts,
            op.product_code,
            js.job_slot_status,
            datediff('minute', js.clock_in_ts, js.clock_out_ts)                                              AS minutes_worked
        FROM
            bronze.order o
                JOIN bronze.order_product op
                     ON o.order_sfid = op.order_sfid
                JOIN bronze.job_slot js
                     ON js.order_product_uuid = op.order_product_uuid
        WHERE
            job_slot_status IN ('Assigned', 'Unassigned', 'Cancelled')
    ),

    order_actual_times        AS (
        SELECT
            confirmation_number,
            min(clock_in_ts)  AS actual_start_time_ts,
            max(clock_out_ts) AS actual_end_time_ts
        FROM
            job_slot_times
        WHERE
            rn = 1 AND
            job_slot_status != 'Cancelled'
        GROUP BY 1
    ),

    actual_pros               AS (
        SELECT
            confirmation_number,
            count(*)                                                        AS number_of_pros_actual,
            count(CASE WHEN product_code = 'TRANSIT' THEN 1 END)            AS number_of_driver_pros_actual,
            count(CASE WHEN product_code != 'TRANSIT' THEN 1 END)           AS number_of_labor_pros_actual,
            sum(minutes_worked)                                             AS total_minutes_worked,
            sum(CASE WHEN product_code = 'TRANSIT' THEN minutes_worked END) AS transit_minutes_worked
        FROM
            job_slot_times
        WHERE
            minutes_worked > 0
        GROUP BY 1
    ),

    customer_transactions     AS (
        SELECT
            o.confirmation_number,
            sum(amount)                                                  AS total_amount,
            sum(CASE WHEN description = 'Deposit' THEN amount END)       AS deposit_amount,
            sum(CASE WHEN description = 'Refund' THEN amount END)        AS refund_amount,
            count(CASE WHEN description = 'Refund' THEN amount END)      AS number_of_refunds,
            sum(CASE WHEN description != 'Refund' THEN amount END)       AS total_amount_without_refunds,
            min(CASE WHEN description = 'Deposit' THEN processed_ts END) AS deposit_ts

        FROM
            bronze.transaction
                INNER JOIN bronze.order o
                           ON o.order_code = order_uuid

        WHERE
            processed_ts IS NOT NULL AND
            lower(description) NOT LIKE '%tip%'

        GROUP BY 1
    ),

    pro_transactions          AS (
        SELECT
            o.confirmation_number,
            sum(transaction.amount) AS pro_payment_total
        FROM
            bronze.order o
                INNER JOIN bronze.order_product
                           USING (order_sfid)
                INNER JOIN bronze.job_slot
                           USING (order_product_sfid)
                INNER JOIN bronze.transaction
                           USING (job_slot_sfid)
        WHERE
            processed_ts IS NOT NULL
        GROUP BY 1
    ),

    package                   AS (
        SELECT
            confirmation_number,
            order_product.product_code,
            product.name
        FROM
            bronze.order o
                JOIN bronze.order_product
                     USING (
                            order_sfid)
                JOIN bronze.product
                     USING (
                            product_code)
        WHERE
            family = 'Package' AND
            order_product_status != 'REMOVED'
    ),

    booked_pros               AS (
        SELECT
            o.confirmation_number,
            sum(op.workers)                                                 AS number_of_pros_booked,
            sum(CASE WHEN op.product_code != 'TRANSIT' THEN op.workers END) AS number_of_labor_pros_booked,
            sum(CASE WHEN op.product_code = 'TRANSIT' THEN op.workers END)  AS number_of_driver_pros_booked
        FROM
            bronze.order o
                INNER JOIN bronze.order_product op
                           ON op.order_sfid = o.order_sfid
        GROUP BY 1
    ),

    promo_amount              AS (
        SELECT
            confirmation_number,
            sum(unitprice) AS promo_amount,
            count(*)       AS number_of_promos
        FROM
            bronze.order o
                INNER JOIN bronze.order_product op
                           ON o.order_sfid = op.order_sfid
        WHERE
            op.product_code = 'PROMOCODE' AND
            order_product_status != 'REMOVED'
        GROUP BY 1
    ),

    damage_info               AS (
        SELECT
            c.order_sfid,
            count(*)            AS damage_claims,
            sum(unitprice * -1) AS damage_amount
        FROM
            bronze.case c
                LEFT JOIN bronze.order_product
                          ON c.order_sfid = order_product.order_sfid AND product_code = 'SETTLEMENT'
        WHERE
            c.is_deleted IS FALSE AND
            claim_type = 'Damage' AND
            c.order_sfid IS NOT NULL
        GROUP BY 1
    ),

    appeasement_info          AS (
        SELECT
            order_sfid,
            count(*)            AS appeasement_count,
            sum(unitprice * -1) AS appeasement_amount
        FROM
            bronze.order_product
        WHERE
            product_code = 'APPEASEMENT'
        GROUP BY 1
    ),

    order_reviews             AS (
        SELECT
            order_code  AS confirmation_number,
            avg(rating) AS order_review_score,
            count(*)    AS number_of_order_reviews

        FROM
            bronze.order_review
        GROUP BY 1
    )


--


--Legacy orders remove booked at revenue for reporting purposes
-- Start and end market are pulled from the order products.
-- The start market is the first location of the move and the end market is the last market of the entire move
SELECT
    o.confirmation_number,
    o.order_code,
    o.order_sfid,
    order_markets.start_market_code              AS start_market_code,
    start_market.name                            AS start_market_name,
    order_markets.end_market_code                AS end_market_code,
    end_market.name                              AS end_market_name,
    o.is_guaranteed,
    executed_revenue,
    'USD'                                        AS currency,
    o.order_status,
    CASE
        WHEN order_markets.confirmation_number LIKE 'ord_%' THEN NULL
        ELSE booked_revenue_at_booking
    END                                          AS booked_revenue_at_booking,
    CASE
        WHEN order_markets.confirmation_number LIKE 'ord_%' THEN NULL
        ELSE booked_revenue_at_move_start
    END                                          AS booked_revenue_at_move_start,
    package.product_code                         AS package_code,
    package.name                                 AS package_name,
    long_distance                                AS is_longdistance,
    affiliated_partner_code,
    affiliated_partner_sfid,
    account.name                                 AS affiliated_partner_name,
    o.account_sfid,
    o.account_uuid,
    booked_start_time_at_booking,
    booked_end_time_at_booking,
    booked_start_time_at_move_start,
    booked_end_time_at_move_start,
    actual_start_time_ts,
    actual_end_time_ts,
    number_of_pros_booked,
    number_of_driver_pros_booked,
    number_of_labor_pros_booked,
    number_of_pros_actual,
    number_of_driver_pros_actual,
    number_of_labor_pros_actual,
    total_minutes_worked,
    transit_minutes_worked,
    -- Transactions
    round(customer_transactions.total_amount, 2) AS payment_amount,
    'USD'                                        AS payment_currency,
    customer_transactions.deposit_amount,
    deposit_ts,
    customer_transactions.refund_amount          AS total_discount_ammount,
    customer_transactions.number_of_refunds      AS number_of_discounts,
    promo_amount.promo_amount,
    promo_amount.number_of_promos,
    appeasement_count,
    appeasement_amount,

    cancellation_reason,
    cancellation_type,

    order_review_score,
    number_of_order_reviews,

    pro_transactions.pro_payment_total,
    damage_claims                                AS number_of_damage_claims,
    damage_amount,
    o.created_ts,
    o.system_updated_ts                          AS modified_at_ts,
    getdate()                                    AS etl_updated_ts


FROM
    bronze.order o
        INNER JOIN order_markets
                   ON order_markets.confirmation_number = o.confirmation_number
        INNER JOIN booked_revenue
                   ON booked_revenue.confirmation_number = o.confirmation_number
        INNER JOIN order_times
                   ON order_times.confirmation_number = o.confirmation_number
        LEFT JOIN  executed_revenue
                   ON executed_revenue.confirmation_number = o.confirmation_number
        LEFT JOIN  order_actual_times
                   ON order_actual_times.confirmation_number = o.confirmation_number
        LEFT JOIN  actual_pros
                   ON actual_pros.confirmation_number = o.confirmation_number
        LEFT JOIN  customer_transactions
                   ON customer_transactions.confirmation_number = o.confirmation_number
        LEFT JOIN  package
                   ON o.confirmation_number = package.confirmation_number
        LEFT JOIN  bronze.market start_market
                   ON start_market.market_code = order_markets.start_market_code
        LEFT JOIN  bronze.market end_market
                   ON end_market.market_code = order_markets.end_market_code
        LEFT JOIN  booked_pros
                   ON booked_pros.confirmation_number = o.confirmation_number
        LEFT JOIN  bronze.account
                   ON account.account_sfid = affiliated_partner_sfid
        LEFT JOIN  pro_transactions
                   ON o.confirmation_number = pro_transactions.confirmation_number
        LEFT JOIN  promo_amount
                   ON promo_amount.confirmation_number = o.confirmation_number
        LEFT JOIN  damage_info
                   ON damage_info.order_sfid = o.order_sfid
        LEFT JOIN  appeasement_info
                   ON appeasement_info.order_sfid = o.order_sfid
        LEFT JOIN  order_reviews
                   ON order_reviews.confirmation_number = o.confirmation_number


ORDER BY
    1