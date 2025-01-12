with base as (
    select *
    from {{ ref('stg_quickbooks__account_tmp') }}
),

account as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns 
        that are expected/needed (staging_columns from dbt_quickbooks_source/models/tmp/) and compares it with columns 
        in the source (source_columns from dbt_quickbooks_source/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_quickbooks__account_tmp')),
                staging_columns=get_account_columns()
            )
        }}

        {{ 
            fivetran_utils.source_relation(
                union_schema_variable='quickbooks_union_schemas', 
                union_database_variable='quickbooks_union_databases'
                ) 
        }}
    from base
),

ar_accounts as (
    select 
        id,
        source_relation,
        row_number() over (partition by source_relation order by updated_at desc) as ar_rank
    from account 
    where account.active = true and account.account_sub_type = 'AccountsReceivable'
),

final as (
    select
        cast(a.id as {{ dbt.type_string() }}) as account_id,
        cast(a.account_number as {{ dbt.type_string() }}) as account_number,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then true
            else a.sub_account
        end as is_sub_account,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then 
                (select cast(ar2.id as {{ dbt.type_string() }})
                 from ar_accounts ar2
                 where ar2.ar_rank = 1 
                 and ar2.source_relation = a.source_relation)
            else cast(a.parent_account_id as {{ dbt.type_string() }})
        end as parent_account_id,
        a.name,
        a.account_type,
        a.account_sub_type,
        a.classification,
        a.balance,
        a.balance_with_sub_accounts,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then 
                false
            else active
        end as is_active,
        a.created_at,
        a.currency_id,
        a.description,
        a.fully_qualified_name,
        a.updated_at,
        a.source_relation,
        a._fivetran_deleted

    from account a
    left join ar_accounts ar 
        on a.id = ar.id 
        and a.source_relation = ar.source_relation
)

select *
from final
where is_active = true and not coalesce(_fivetran_deleted, false)