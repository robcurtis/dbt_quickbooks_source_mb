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
        cast(id as {{ dbt.type_string() }}) as account_id,
        cast(account_number as {{ dbt.type_string() }}) as account_number,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then true
            else sub_account
        end as is_sub_account,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then 
                (select cast(ar2.id as {{ dbt.type_string() }})
                 from ar_accounts ar2
                 where ar2.ar_rank = 1 
                 and ar2.source_relation = a.source_relation)
            else cast(parent_account_id as {{ dbt.type_string() }})
        end as parent_account_id,
        name,
        account_type,
        account_sub_type,
        classification,
        balance,
        balance_with_sub_accounts,
        case 
            when a.account_sub_type = 'AccountsReceivable' and ar.ar_rank > 1 then 
                false
            else active
        end as is_active,
        created_at,
        currency_id,
        description,
        fully_qualified_name,
        updated_at,
        a.source_relation,
        _fivetran_deleted

    from account a
    left join ar_accounts ar 
        on a.id = ar.id 
        and a.source_relation = ar.source_relation
)

select *
from final
where not coalesce(_fivetran_deleted, false)