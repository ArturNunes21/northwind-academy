with
    fonte_transportadoras as (
        select
        cast(ID as varchar) as pk_transportadora
        , cast(COMPANYNAME as varchar) as nm_compania
        , cast(PHONE as varchar) as telefone
        from {{ source('erp', 'shipper') }}
    )

select *
from fonte_transportadoras