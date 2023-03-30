{{
    config (engine='MergeTree()',
      order_by=['id'],
      
    )
}}

SELECT

      (select order_date 
			from {{ ref('v_stg_orders') }} 
				where date_part('week',order_date)=week limit 1),
      status,
      (select count(order_date )
					from {{ ref('v_stg_orders') }} c1
						where date_part('week',order_date)=week and c1.status=c.status),
        week
    

FROM {{ ref('first_mart') }} AS c

    
