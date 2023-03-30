

WITH all_history as (

SELECT

      hs.CUSTOMER_PK,
      s.first_name,
      s.last_name,
      s.email,
      s.EFFECTIVE_FROM,
      coalesce(lead(EFFECTIVE_FROM) over (partition by hs.CUSTOMER_KEY order by s.EFFECTIVE_FROM),'now') as EFFECTIVE_to
    

FROM {{ ref('hub_customer') }} AS hs
  left join {{ ref('sat_customer_details') }} AS s on hs.CUSTOMER_PK=s.CUSTOMER_PK

)

select
    CUSTOMER_PK,
    first_name,
    last_name,
    email,
    EFFECTIVE_FROM,
    EFFECTIVE_to
from all_history
where true

    
