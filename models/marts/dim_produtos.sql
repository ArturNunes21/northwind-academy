WITH
    produtos as (
        SELECT *
        FROM {{ ref('stg_erp__produtos') }}
    )

    , categorias as (
        SELECT *
        FROM {{ ref('stg_erp__categorias') }}
    )

    , fornecedores as (
        SELECT *
        FROM {{ ref('stg_erp__fornecedores') }}
    )

    , joined as (
        SELECT
            produtos.PK_PRODUTO
            , produtos.NM_PRODUTO
            , produtos.QUANTIDADE_POR_UNIDADE
            , produtos.PRECO_POR_UNIDADE
            , produtos.UNIDADE_EM_ESTOQUE
            , produtos.UNIDADE_POR_PEDIDO
            , produtos.NIVEL_DE_PEDIDO
            , produtos.IS_DISCONTINUADO
            , categorias.NM_CATEGORIA
            , categorias.DESCRICAO_CATEGORIA
            , fornecedores.NM_FORNECEDOR
            , fornecedores.CIDADE_FORNECEDOR
            , fornecedores.PAIS_FORNECEDOR
        FROM produtos
        LEFT JOIN categorias ON produtos.fk_categoria = categorias.pk_categoria
        LEFT JOIN fornecedores ON produtos.fk_fornecedor = fornecedores.pk_fornecedor
    )

SELECT *
FROM joined