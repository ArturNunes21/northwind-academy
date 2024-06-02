WITH
    fonte_produtos as (
        SELECT
            CAST(ID as int) as pk_produto
            , CAST(SUPPLIERID as int) as fk_fornecedor 
            , CAST(CATEGORYID as int) as fk_categoria
            , CAST(PRODUCTNAME as string) as nm_produto
            , CAST(QUANTITYPERUNIT as string) as quantidade_por_unidade
            , CAST(UNITPRICE as numeric(18,2)) as preco_por_unidade --até 18 casas, 2 casas decimais
            , CAST(UNITSINSTOCK as int) as unidade_em_estoque
            , CAST(UNITSONORDER as int) as unidade_por_pedido
            , CAST(REORDERLEVEL as int) as nivel_de_pedido
            , case
            --troca o valor das colunas
                WHEN DISCONTINUED = 0 then 'não'
                WHEN DISCONTINUED = 1 then 'sim'
            end as is_discontinuado
        FROM {{ source('erp', 'product') }} --erp = source / product = tabela
    )

    SELECT *
    FROM fonte_produtos
