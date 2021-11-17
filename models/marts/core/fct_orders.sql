with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select * from {{ ref('stg_orders')}}
),

fct_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        sum(amount) as amount
    from orders
        inner join payments 
        on (orders.order_id = payments.order_id)
    group by 1,2
)

select * from fct_orders