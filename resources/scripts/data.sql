BEGIN;

-- Dates
with dates as (
  select distinct to_date(nullif(trim(sale_date), ''), 'MM/DD/YYYY') as d from public.mock_data
  union select distinct to_date(nullif(trim(product_release_date), ''), 'MM/DD/YYYY') from public.mock_data
  union select distinct to_date(nullif(trim(product_expiry_date),  ''), 'MM/DD/YYYY') from public.mock_data
)
insert into dim_date (date_key, date_actual, year, quarter, month, day, week_of_year, is_weekend)
select
  extract(year from d)::int*10000 + extract(month from d)::int*100 + extract(day from d)::int as date_key,
  d,
  extract(year from d)::int,
  extract(quarter from d)::int,
  extract(month from d)::int,
  extract(day from d)::int,
  extract(week from d)::int,
  (extract(isodow from d)::int in (6,7))
from dates
where d is not null
on conflict (date_key) do nothing;

-- Geography
insert into dim_geo_country(country_name)
select country from (
  select distinct nullif(trim(customer_country),'') as country from public.mock_data
  union select distinct nullif(trim(seller_country),'') from public.mock_data
  union select distinct nullif(trim(store_country),'') from public.mock_data
  union select distinct nullif(trim(supplier_country),'') from public.mock_data
) s
where country is not null
on conflict do nothing;

insert into dim_geo_state(state_name, country_key)
select distinct
  nullif(trim(store_state),''),
  c.country_key
from public.mock_data md
join dim_geo_country c on c.country_name = md.store_country
where nullif(trim(store_state),'') is not null
on conflict do nothing;

insert into dim_geo_city(city_name, state_key, country_key)
select distinct
  nullif(trim(md.store_city),''),
  st.state_key,
  c.country_key
from public.mock_data md
join dim_geo_country c on c.country_name = md.store_country
left join dim_geo_state st
  on st.country_key = c.country_key and st.state_name = md.store_state
where nullif(trim(md.store_city),'') is not null
on conflict do nothing;

insert into dim_geo_city(city_name, state_key, country_key)
select distinct
  nullif(trim(md.supplier_city),''),
  null::int,
  c.country_key
from public.mock_data md
join dim_geo_country c on c.country_name = md.supplier_country
where nullif(trim(md.supplier_city),'') is not null
on conflict do nothing;

insert into dim_geo_postal(postal_code, country_key)
select distinct
  nullif(trim(md.customer_postal_code),''),
  c.country_key
from public.mock_data md
join dim_geo_country c on c.country_name = md.customer_country
where nullif(trim(md.customer_postal_code),'') is not null
on conflict do nothing;

insert into dim_geo_postal(postal_code, country_key)
select distinct
  nullif(trim(md.seller_postal_code),''),
  c.country_key
from public.mock_data md
join dim_geo_country c on c.country_name = md.seller_country
where nullif(trim(md.seller_postal_code),'') is not null
on conflict do nothing;

-- Pets
insert into dim_pet_category(pet_category)
select distinct nullif(trim(pet_category),'')
from public.mock_data
where nullif(trim(pet_category),'') is not null
on conflict do nothing;

insert into dim_pet_type(pet_type, pet_category_key)
select distinct
  nullif(trim(customer_pet_type),''),
  pc.pet_category_key
from public.mock_data md
join dim_pet_category pc on pc.pet_category = md.pet_category
where nullif(trim(customer_pet_type),'') is not null
on conflict do nothing;

insert into dim_pet_breed(pet_breed, pet_type_key)
select distinct
  nullif(trim(customer_pet_breed),''),
  pt.pet_type_key
from public.mock_data md
join dim_pet_type pt on pt.pet_type = md.customer_pet_type
where nullif(trim(customer_pet_breed),'') is not null
on conflict do nothing;

insert into dim_pet(pet_name, pet_breed_key)
select distinct
  nullif(trim(customer_pet_name),''),
  pb.pet_breed_key
from public.mock_data md
left join dim_pet_breed pb on pb.pet_breed = md.customer_pet_breed
where nullif(trim(customer_pet_name),'') is not null
on conflict do nothing;

-- Parties
insert into dim_customer (customer_natural_id, first_name, last_name, age, email, postal_key, pet_key)
select distinct
  sale_customer_id,
  nullif(trim(customer_first_name),''),
  nullif(trim(customer_last_name),''),
  nullif(customer_age, 0),
  nullif(trim(customer_email),''),
  gp.postal_key,
  p.pet_key
from public.mock_data md
left join dim_geo_country cc on cc.country_name = md.customer_country
left join dim_geo_postal gp on gp.country_key = cc.country_key
    and gp.postal_code = nullif(trim(md.customer_postal_code),'')
left join dim_pet p on p.pet_name = nullif(trim(md.customer_pet_name),'')
on conflict (email) do nothing;

insert into dim_seller (seller_natural_id, first_name, last_name, email, postal_key)
select distinct
  sale_seller_id,
  nullif(trim(seller_first_name),''),
  nullif(trim(seller_last_name),''),
  nullif(trim(seller_email),''),
  gp.postal_key
from public.mock_data md
left join dim_geo_country sc on sc.country_name = md.seller_country
left join dim_geo_postal gp on gp.country_key = sc.country_key
    and gp.postal_code = nullif(trim(md.seller_postal_code),'')
on conflict (email) do nothing;

insert into dim_store (store_name, location, city_key, state_key, country_key, phone, email)
select distinct
  nullif(trim(store_name),''),
  nullif(trim(store_location),''),
  c.city_key,
  s.state_key,
  co.country_key,
  nullif(trim(store_phone),''),
  nullif(trim(store_email),'')
from public.mock_data md
left join dim_geo_country co on co.country_name = md.store_country
left join dim_geo_state   s  on s.country_key = co.country_key and s.state_name = md.store_state
left join dim_geo_city    c  on c.country_key = co.country_key
                               and (c.state_key is not distinct from s.state_key)
                               and c.city_name = nullif(trim(md.store_city),'')
where nullif(trim(store_name),'') is not null
on conflict (store_name) do nothing;

insert into dim_supplier (supplier_name, contact, email, phone, address, city_key, country_key)
select distinct
  nullif(trim(supplier_name),''),
  nullif(trim(supplier_contact),''),
  nullif(trim(supplier_email),''),
  nullif(trim(supplier_phone),''),
  nullif(trim(supplier_address),''),
  c.city_key,
  co.country_key
from public.mock_data md
left join dim_geo_country co on co.country_name = md.supplier_country
left join dim_geo_city    c  on c.country_key = co.country_key
                               and c.city_name = nullif(trim(md.supplier_city),'')
where nullif(trim(supplier_name),'') is not null
on conflict (supplier_name) do nothing;

-- Product
insert into dim_brand(brand_name)
select distinct nullif(trim(product_brand),'')
from public.mock_data
where nullif(trim(product_brand),'') is not null
on conflict do nothing;

insert into dim_category(category_name)
select distinct nullif(trim(product_category),'')
from public.mock_data
where nullif(trim(product_category),'') is not null
on conflict do nothing;

insert into dim_color(color_name)
select distinct nullif(trim(product_color),'')
from public.mock_data
where nullif(trim(product_color),'') is not null
on conflict do nothing;

insert into dim_size(size_name)
select distinct nullif(trim(product_size),'')
from public.mock_data
where nullif(trim(product_size),'') is not null
on conflict do nothing;

insert into dim_material(material_name)
select distinct nullif(trim(product_material),'')
from public.mock_data
where nullif(trim(product_material),'') is not null
on conflict do nothing;

insert into dim_product(
    product_natural_id, product_name, brand_key, category_key,
    color_key, size_key, material_key, weight, description, rating, reviews,
    release_date_key, expiry_date_key
)
select distinct
    sale_product_id,
    nullif(trim(product_name),''),
    b.brand_key,
    cat.category_key,
    col.color_key,
    sz.size_key,
    m.material_key,
    nullif(product_weight, 0),
    nullif(trim(product_description),''),
    nullif(product_rating, 0),
    nullif(product_reviews, 0),
    rd.date_key,
    ed.date_key
from public.mock_data md
left join dim_brand    b   on b.brand_name = md.product_brand
left join dim_category cat on cat.category_name = md.product_category
left join dim_color    col on col.color_name = md.product_color
left join dim_size     sz  on sz.size_name = md.product_size
left join dim_material m   on m.material_name = md.product_material
left join dim_date     rd  on rd.date_actual = to_date(nullif(trim(md.product_release_date),''), 'MM/DD/YYYY')
left join dim_date     ed  on ed.date_actual = to_date(nullif(trim(md.product_expiry_date), ''), 'MM/DD/YYYY')
where nullif(trim(product_name),'') is not null
on conflict (product_natural_id) do nothing;

insert into bridge_product_supplier(product_key, supplier_key)
select distinct
  p.product_key,
  s.supplier_key
from public.mock_data md
join dim_product  p on p.product_natural_id = md.sale_product_id
join dim_supplier s on s.supplier_name = md.supplier_name
on conflict do nothing;

-- Fact
insert into fact_sales(
    sale_date_key, customer_key, seller_key, product_key, store_key, supplier_key,
    sale_id, quantity, line_total, unit_price
)
select
    d.date_key,
    c.customer_key,
    s.seller_key,
    p.product_key,
    st.store_key,
    sp.supplier_key,
    md.id,
    coalesce(md.sale_quantity, md.product_quantity, 1),
    nullif(md.sale_total_price, 0),
    nullif(md.product_price, 0)
from public.mock_data md
join dim_date d
  on d.date_actual = to_date(nullif(trim(md.sale_date),''), 'MM/DD/YYYY')
left join dim_customer c on c.email = nullif(trim(md.customer_email),'')
left join dim_seller   s on s.email = nullif(trim(md.seller_email),'')
left join dim_store    st on st.store_name = nullif(trim(md.store_name),'')
left join dim_supplier sp on sp.supplier_name = nullif(trim(md.supplier_name),'')
left join dim_product  p  on p.product_natural_id = md.sale_product_id
where d.date_key is not null;

COMMIT;