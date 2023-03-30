{{
    config (engine='MergeTree()',
      order_by=['id'],
      
    )
}}

SELECT

      date_part('week',order_date) as week ,
      status
    

FROM {{ ref('v_stg_orders') }} AS c
GROUP BY week,status
order by week desc
    
