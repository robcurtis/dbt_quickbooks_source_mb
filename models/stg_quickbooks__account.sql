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

final as (

    select
        cast(id as {{ dbt.type_string() }}) as account_id,
        cast(account_number as {{ dbt.type_string() }}) as account_number,
        CASE WHEN 
            (source_relation = 'quickbooks_bhdsc' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
            (source_relation = 'quickbooks_bhec' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
            (source_relation = 'quickbooks_bvsc' and cast(account_number as {{ dbt.type_string() }}) IN ('1105', '1121'))
            THEN true 
        ELSE sub_account 
        END as is_sub_account,
        CASE WHEN 
            (source_relation = 'quickbooks_bhdsc' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
            (source_relation = 'quickbooks_bhec' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
            (source_relation = 'quickbooks_bvsc' and cast(account_number as {{ dbt.type_string() }}) IN ('1105', '1121')) 
            THEN (
                SELECT MAX(id)  -- Using MAX to ensure single result
                FROM account a
                WHERE a.source_relation = source_relation 
                AND a.account_number = '1100'
            )
        ELSE cast(parent_account_id as {{ dbt.type_string() }}) 
        END as parent_account_id,
        name,
        account_type,
        account_sub_type,
        classification,
        balance,
        balance_with_sub_accounts,
        active as is_active,
        created_at,
        currency_id,
        description,
        fully_qualified_name,
        updated_at,
        source_relation,
        _fivetran_deleted 
    from account
    where 
        (sub_account = false AND active = true) 
        OR (
            sub_account = true AND (
                CASE WHEN 
                (source_relation = 'quickbooks_bhdsc' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
                (source_relation = 'quickbooks_bhec' and cast(account_number as {{ dbt.type_string() }}) = '1105') or
                (source_relation = 'quickbooks_bvsc' and cast(account_number as {{ dbt.type_string() }}) IN ('1105', '1121')) 
                THEN (
                    SELECT MAX(id)  -- Using MAX to ensure single result
                    FROM account a
                    WHERE a.source_relation = source_relation 
                    AND a.account_number = '1100'
                )
            ELSE cast(parent_account_id as {{ dbt.type_string() }}) 
            END -- parent_account_id
        ) in (
            select id 
            from account a
            where a.active = true
        )
    ) OR
        (source_relation = 'quickbooks_bhdsc' and account_number <> '1116' and account_number <> '1120') or 
        (source_relation = 'quickbooks_bhec' and account_number <> '1116' and account_number <> '1120') or 
        (source_relation = 'quickbooks_bvsc' and account_number <> '1110')
)

select *
from final
where not coalesce(_fivetran_deleted, false)