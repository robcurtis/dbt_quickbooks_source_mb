--To disable this model, set the using_invoice variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_invoice', True)) }}

with base as (

    select * 
    from {{ ref('stg_quickbooks__invoice_tmp') }}

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
                source_columns=adapter.get_columns_in_relation(ref('stg_quickbooks__invoice_tmp')),
                staging_columns=get_invoice_columns()
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
        cast(f.id as {{ dbt.type_string() }}) as invoice_id,
        f.balance,
        cast(f.doc_number as {{ dbt.type_string() }}) as doc_number,
        f.total_amount,
        f.currency_id,
        cast(f.department_id as {{ dbt.type_string() }}) as department_id,
        cast(f.deposit_to_account_id as {{ dbt.type_string() }}) as deposit_to_account_id,
        cast(c.ar_account_id as {{ dbt.type_string() }}) as receivable_account_id,
        f.exchange_rate,
        cast( {{ dbt.date_trunc('day', 'f.transaction_date') }} as date) as transaction_date,
        cast(f.customer_id as {{ dbt.type_string() }}) as customer_id,
        cast(f.billing_address_id as {{ dbt.type_string() }}) as billing_address_id,
        f.shipping_address_id,
        f.delivery_type,
        cast( {{ dbt.date_trunc('day', 'f.due_date') }} as date) as due_date, 
        cast(f.class_id as {{ dbt.type_string() }}) as class_id,
        f.created_at,
        f.updated_at,
        f._fivetran_deleted,
        f.source_relation
    from fields f
    left join {{ ref('stg_quickbooks__customer') }} c
        on f.customer_id = c.customer_id
        and f.source_relation = c.source_relation
)

select * 
from final
where not coalesce(_fivetran_deleted, false)
