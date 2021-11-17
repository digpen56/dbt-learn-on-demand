select
    order_id,
    sum(amount) as total_payment
from {{ ref('stg_payments') }}
group by 1
having not(total_payment>=0)