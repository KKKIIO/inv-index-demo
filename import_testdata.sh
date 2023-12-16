cat testdata.csv  | psql -h localhost -U postgres -d postgres -c "
begin;
create temp table batch_orders (
    id INTEGER PRIMARY KEY,
    order_status SMALLINT NOT NULL,
    product_id INTEGER NOT NULL,
    provider_id INTEGER DEFAULT NULL,
    create_time TIMESTAMP NOT NULL
);

copy batch_orders (id, order_status, product_id, provider_id, create_time)
from STDIN
with (format 'csv',
      header,
      delimiter ',');

insert into orders (id, order_status, product_id, provider_id, create_time)
select batch_orders.id,
       batch_orders.order_status,
       batch_orders.product_id,
       batch_orders.provider_id,
       batch_orders.create_time
from batch_orders;
commit;"