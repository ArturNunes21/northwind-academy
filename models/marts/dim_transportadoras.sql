with
    transp as (
        select *
        from {{ ref('stg_erp__transportadoras') }}
    )

    , ordens as (
        select *
        from {{ ref('stg_erp__ordens') }}
    )

    , joined as (
        select
        transp.PK_TRANSPORTADORA
        , ordens.FK_CLIENTE
        , transp.NM_COMPANIA
        , transp.TELEFONE
        , ordens.DATA_DO_PEDIDO
        , ordens.FRETE
        , ordens.PAIS_DESTINATARIO
        , ordens.REGIAO_DESTINATARIO
        , ordens.DATA_DO_ENVIO
        , ordens.DATA_REQUERIDA_ENTREGA
        from ordens
        left join transp on ordens.fk_transportadora = transp.pk_transportadora
    )

    , metricas as (
        select
        PK_TRANSPORTADORA
        , NM_COMPANIA
        , TELEFONE
        , count(pk_transportadora) as qtde_entregas
        , round(sum(frete), 2) as valor_total_frete
        , round((sum(frete) / count(pk_transportadora)), 2) as vlr_medio_frete
        from joined
        group by pk_transportadora
        , nm_compania
        , telefone
    )

select *
from metricas