WITH
    fonte_fornecedores as (
        SELECT
            cast(C1 as int) as pk_fornecedor
            , cast(C2 as varchar) as nm_fornecedor
            , cast(C6 as varchar) as cidade_fornecedor
            , cast(C9 as varchar) as pais_fornecedor 
        FROM {{ source('erp', 'supplier') }} --erp = source / supplier = tabela
        where C1 != 'Id' --Limpeza de dados, onde removemos uma linha desnecessária, em que a 1ª linha é o nome das colunas
    )

    SELECT *
    FROM fonte_fornecedores
