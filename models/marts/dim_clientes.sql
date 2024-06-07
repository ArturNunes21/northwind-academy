with
    clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

    , pedido_por_itens as (
        select *
        from {{ ref('int_pedido_por_itens') }}
    )

    , joined as (
        select
            clientes.PK_CLIENTE
            , clientes.NM_EMPRESA_CLIENTE
            , clientes.CIDADE_CLIENTE
            , clientes.REGIAO_CLIENTE
            , clientes.PAIS_CLIENTE
            , pedido.FK_PEDIDO
            , pedido.QUANTIDADE
            , pedido.DESCONTO_PERC
            , pedido.PRECO_DA_UNIDADE
        from pedido_por_itens as pedido
        left join clientes on pedido.fk_cliente = clientes.pk_cliente
        where PK_CLIENTE is not null
    )

    , metricas as (
        select
            PK_CLIENTE
            , NM_EMPRESA_CLIENTE
            , CIDADE_CLIENTE
            , PAIS_CLIENTE
            , REGIAO_CLIENTE
            , round(sum(QUANTIDADE * (1 - DESCONTO_PERC) * PRECO_DA_UNIDADE), 2) as valor_total_compras
            , count(FK_PEDIDO) as qtde_pedidos
            , round((sum(QUANTIDADE * (1 - DESCONTO_PERC) * PRECO_DA_UNIDADE) / count(FK_PEDIDO)), 2) as vlr_medio_compra
        from joined
        group by
            PK_CLIENTE
            , NM_EMPRESA_CLIENTE
            , CIDADE_CLIENTE
            , REGIAO_CLIENTE
            , PAIS_CLIENTE
        order by PK_CLIENTE
    )

select *
from metricas
