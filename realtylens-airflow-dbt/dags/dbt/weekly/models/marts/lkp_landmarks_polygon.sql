{{ config(
    materialized='table'
) }}

with stg_landmark_polygon as (
    select 
        l.landmark_id,
        l.landmark_name,
        l.address,
        l.feat_type,
        l.sub_type,
        l.vanity_name,
        l.secondary_name,
        l.bldg,
        l.parent_name,
        l.parent_type,
        l.acreage,
        l.parent_acreage,
        l.shape_area,
        l.shape_length,
        l.polygon_type,
        l.polygon_coordinates,
        z.zip_code
    from {{ ref('stg_landmark_polygon') }} l
    join {{ ref('stg_zip_code_polygon') }} z
    on ST_WITHIN(l.polygon_coordinates, z.polygon_coordinates)
)

select * from stg_landmark_polygon