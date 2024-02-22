{{ config(materialized="view") }}

with
    source as (

        select *
        from {{ source("staging", "fhv_data") }}
        where date(pickup_datetime) between '2019-01-01' and '2019-12-31'

    )
select
    {{ dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }}
    as fhvid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }}
    as dispatching_base_number,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    cast(sr_flag as numeric) as sr_flag,
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }}
    as affiliated_base_number
from source

{% if var("is_test_run", default=true) %} limit 100 {% endif %}
