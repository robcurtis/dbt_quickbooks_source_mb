with base as (

    select * 
    from {{ ref('stg_quickbooks__credit_memo_line_bundle_tmp') }}

),

fields as (

    select
        /*
        The below macro is used to generate the correct SQL for package staging models. It takes a list of columns 
        that are expected/needed (staging_columns from dbt_quickbooks_source/models/tmp/) and compares it with columns 
        in the source (source_columns from dbt_quickbooks_source/macros/).
        For more information refer to our dbt_fivetran_utils documentation (https://github.com/fivetran/dbt_fivetran_utils.git).
        */
        {{
            fivetran_utils.fill_staging_columns(
                source_columns=adapter.get_columns_in_relation(ref('stg_quickbooks__credit_memo_line_bundle_tmp')),
                staging_columns=get_credit_memo_line_bundle_columns()
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
        cast(credit_memo_id as {{ dbt.type_string() }}) as credit_memo_id,
        credit_memo_line_index,
        index,
        line_num,
        description,
        amount,
        unit_price,
        cast( {{ dbt.date_trunc('day', 'service_date') }} as date) as service_date,
        discount_rate,
        cast(item_id as {{ dbt.type_string() }}) as item_id,
        cast(class_id as {{ dbt.type_string() }}) as class_id,
        quantity,
        cast(account_id as {{ dbt.type_string() }}) as account_id,
        cast(tax_code_id as {{ dbt.type_string() }}) as tax_code_id,
        discount_amount,
        _fivetran_deleted,
        source_relation
    from fields
)

select * 
from final
where not coalesce(_fivetran_deleted, false)