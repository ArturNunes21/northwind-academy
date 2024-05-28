WITH
    fonte_categorias as (
        SELECT 
            cast(ID as int) as pk_categoria
            , cast(CATEGORYNAME as varchar) as nm_categoria
            , cast(DESCRIPTION as varchar) as descricao_categoria
        FROM {{ source('erp', 'category') }} --erp = source / category = tabela
    )

    SELECT *
    FROM fonte_categorias
