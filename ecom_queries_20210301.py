querryForStatsP0 = """SELECT COUNT(distinct uuidPath)                                                            as orders_with_blocking,
       if(SUM(order_total) IS NULL, 0, SUM(order_total))                                   AS sales_with_blocking,
       if(SUM(max_saving_per_order) IS NULL, 0.0, SUM(max_saving_per_order))               AS estimated_savings,
       if(SUM(appliedDiscount) IS NULL, 0.0, SUM(appliedDiscount))                         AS applied_discount,
		sales_with_blocking + applied_discount                               AS gross_revenue_with_coupons_blocked_applied,
		CASE
			WHEN estimated_savings = 0 OR gross_revenue_with_coupons_blocked_applied = 0 THEN 0
			ELSE 100 * (estimated_savings / gross_revenue_with_coupons_blocked_applied) END AS percentage_of_gross_sales_saved 
FROM (
         SELECT uuidPath,
                location,
                publisherHostname
         FROM ecomm_all_events
         WHERE clientEvent = 1
           AND eventName = 0
           AND uuidPath is not null
           AND location like '%/checkouts/%/thank_you%'
           AND idx_date >= :start_date_str
           AND idx_date <= :end_date_str
           AND (publisherHostname in
                (SELECT distinct lower(host)
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId =:organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
               )
         GROUP BY uuidPath,
                  location,
                  publisherHostname
         ) TrustedPageViews
         INNER JOIN (

    SELECT uuidPath,
           OneLastMaxSaving.orderUrl                                                AS orderUrl,
           appliedDiscount,
            toFloat32( replaceRegexpAll( orderTotal, '\\\\\\\\D*(\\\\\\\\d*\\\\\\\\.?\\\\\\\\d*)\\\\\\\\D*', '\\\\\\\\1' ) ) AS order_total,
           OneLastMaxSaving.maxSaving                                               AS max_saving_per_order,
           maxTimestamp,
           first_coupon,
           count(*)                                                                 as num_duplicates,
           MAX(maxSaving)                                                           as max_sav

    FROM ecomm_all_events AllEvents
             INNER JOIN
             (select uuidPath, orderUrl, maxSaving, maxTimestamp, min(coupon) as first_coupon, COUNT(*)
              from ecomm_all_events All2
                       inner join
                   ( SELECT uuidPath,
                            MaxSavingPerOrder.orderUrl  AS orderUrl,
                            MaxSavingPerOrder.maxSaving AS maxSaving,
                            MAX(timestamp)              AS maxTimestamp
                     FROM ecomm_all_events AllEventsInternal
                              INNER JOIN

                          (
                              SELECT uuidPath,
                                     orderUrl    AS orderUrl,
                                     MAX(saving) AS maxSaving

                              FROM ecomm_all_events
                              WHERE serviceEvent = 1
                                AND trimBoth(currencySymbol) = '$'
                                AND trimBoth(orderCurrencySymbol) = '$'
                                AND eventName = 0
                                AND uuidPath is not null
                                AND idx_date >= :start_date_str
                                AND idx_date <= :end_date_str
                                AND (publisherHostname in
                                     (SELECT distinct lower(host)
                                      FROM preagg_org_id_hosts
                                      WHERE orgId = :organizationId)
                                  OR publisherHostname in
                                     (SELECT distinct concat('www.', lower(host))
                                      FROM preagg_org_id_hosts
                                      WHERE orgId = :organizationId)
                                  OR publisherHostname in
                                     (SELECT distinct concat('shop.', lower(host))
                                      FROM preagg_org_id_hosts
                                      WHERE orgId = :organizationId)
                                  OR publisherHostname in
                                     (SELECT distinct concat('www.shop.', lower(host))
                                      FROM preagg_org_id_hosts
                                      WHERE orgId = :organizationId)
                                    )
                                And couponCode is not null
                                AND (coupon like '%nenlahapcbofgnanklpelkaejcehkggg%' or
                                     coupon like '%bmnlcjabgnpnenekpadlanbbkooimhnj%')

                              GROUP BY uuidPath, orderUrl

                              ) MaxSavingPerOrder ON
                                  AllEventsInternal.uuidPath = MaxSavingPerOrder.uuidPath
                                  AND AllEventsInternal.orderUrl = MaxSavingPerOrder.orderUrl
                                  AND AllEventsInternal.saving = MaxSavingPerOrder.maxSaving
                     WHERE AllEventsInternal.serviceEvent = 1
                       AND trimBoth(AllEventsInternal.currencySymbol) = '$'
                       AND trimBoth(AllEventsInternal.orderCurrencySymbol) = '$'
                       AND AllEventsInternal.eventName = 0
                       AND AllEventsInternal.uuidPath is not null
                       AND AllEventsInternal.idx_date >= :start_date_str
                       AND AllEventsInternal.idx_date <= :end_date_str
                       AND (AllEventsInternal.publisherHostname in
                            (SELECT distinct lower(host)
                             FROM preagg_org_id_hosts
                             WHERE orgId = :organizationId)
                         OR AllEventsInternal.publisherHostname in
                            (SELECT distinct concat('www.', lower(host))
                             FROM preagg_org_id_hosts
                             WHERE orgId = :organizationId)
                         OR AllEventsInternal.publisherHostname in
                            (SELECT distinct concat('shop.', lower(host))
                             FROM preagg_org_id_hosts
                             WHERE orgId = :organizationId)
                         OR AllEventsInternal.publisherHostname in
                            (SELECT distinct concat('www.shop.', lower(host))
                             FROM preagg_org_id_hosts
                             WHERE orgId = :organizationId)
                           )
                     GROUP BY uuidPath, MaxSavingPerOrder.orderUrl, MaxSavingPerOrder.maxSaving

                       ) LastMaxSavingPerOrder
                   on All2.timestamp = LastMaxSavingPerOrder.maxTimestamp
                       AND All2.saving = LastMaxSavingPerOrder.maxSaving
                       AND All2.uuidPath = LastMaxSavingPerOrder.uuidPath
                       AND All2.orderUrl = LastMaxSavingPerOrder.orderUrl

              group by uuidPath, orderUrl, maxSaving, maxTimestamp ) OneLastMaxSaving
         ON
                 AllEvents.timestamp = OneLastMaxSaving.maxTimestamp
                 AND AllEvents.saving = OneLastMaxSaving.maxSaving
                 AND AllEvents.uuidPath = OneLastMaxSaving.uuidPath
                 AND AllEvents.orderUrl = OneLastMaxSaving.orderUrl
                 AND AllEvents.coupon = OneLastMaxSaving.first_coupon
    WHERE AllEvents.serviceEvent = 1
      AND trimBoth(AllEvents.currencySymbol) = '$'
      AND trimBoth(AllEvents.orderCurrencySymbol) = '$'
      AND AllEvents.eventName = 0
      AND AllEvents.uuidPath is not null
      AND (coupon like '%nenlahapcbofgnanklpelkaejcehkggg%' or coupon like '%bmnlcjabgnpnenekpadlanbbkooimhnj%')
      AND AllEvents.idx_date >= :start_date_str
      AND AllEvents.idx_date <= :end_date_str
      AND (AllEvents.publisherHostname in
           (SELECT distinct lower(host)
            FROM preagg_org_id_hosts
            WHERE orgId = :organizationId)
        OR AllEvents.publisherHostname in
           (SELECT distinct concat('www.', lower(host))
            FROM preagg_org_id_hosts
            WHERE orgId = :organizationId)
        OR AllEvents.publisherHostname in
           (SELECT distinct concat('shop.', lower(host))
            FROM preagg_org_id_hosts
            WHERE orgId = :organizationId)
        OR AllEvents.publisherHostname in
           (SELECT distinct concat('www.shop.', lower(host))
            FROM preagg_org_id_hosts
            WHERE orgId = :organizationId)
          )
    group by uuidPath, orderUrl, appliedDiscount, order_total, max_saving_per_order, maxTimestamp, first_coupon) CouponWinner
                ON TrustedPageViews.uuidPath = CouponWinner.uuidPath
                AND  substring(TrustedPageViews.location, 1, position(TrustedPageViews.location, 'thank_you') - 2) =  CouponWinner.orderUrl"""
querryForStatsP1 = """SELECT
       SUM(count_per_order) AS total_coupons_blocked,
       ifNull(COUNT(DISTINCT uuidPath), 0) AS total_carts_with_blocking,
       ifNull(COUNT(DISTINCT couponCode), 0) as total_unique_coupon_codes
FROM
     (
      SELECT
             uuidPath, orderUrl, upper(trimBoth(couponCode)) AS couponCode, count(*) count_per_order
      FROM ecomm_all_events
      WHERE clientEvent = 1 AND eventName = 3 AND uuidPath is not null
        AND (coupon like '%nenlahapcbofgnanklpelkaejcehkggg%' or coupon like '%bmnlcjabgnpnenekpadlanbbkooimhnj%')
        AND idx_date >= :start_date_str AND idx_date <= :end_date_str
        AND (publisherHostname in
                (SELECT distinct lower(host)
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId =:organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
               )
        AND trimBoth(orderCurrencySymbol) = '$' GROUP BY uuidPath, orderUrl, couponCode )"""
querryForStatsP2 = """SELECT
		count(uuid) AS total_protected_pageviews
		FROM ecomm_all_events
		WHERE eventName = 0 AND clientEvent = 1 AND uuidPath is not null AND idx_date >= :start_date_str AND idx_date <= :end_date_str
		AND (publisherHostname in
                (SELECT distinct lower(host)
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId =:organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
               )"""
querryforthankyoupage = """SELECT 
			count(*) as thankyoupage FROM ecomm_all_events
			WHERE (publisherHostname in
                (SELECT distinct lower(host)
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId =:organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
               )
	AND idx_date >= :start_date_str AND idx_date <= :end_date_str
	AND clientEvent = 1 AND eventName = 0 AND uuidPath is not null AND like(location, '%checkouts%') 
	AND like(location, '%thank_you%')"""
querryforextentionblocked = """SELECT 
		count(*) as extension_blocked
		FROM ecomm_all_events
		WHERE (publisherHostname in
                (SELECT distinct lower(host)
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
             OR publisherHostname in
                (SELECT distinct concat('shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId =:organizationId)
             OR publisherHostname in
                (SELECT distinct concat('www.shop.', lower(host))
                 FROM preagg_org_id_hosts
                 WHERE orgId = :organizationId)
               )
  AND idx_date >= :start_date_str AND idx_date <= :end_date_str
  and extensionState = 1 and (extension like '%nenlahapcbofgnanklpelkaejcehkggg%' or extension  like '%bmnlcjabgnpnenekpadlanbbkooimhnj%')"""