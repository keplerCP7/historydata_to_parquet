CREATE EXTERNAL TABLE tmp_product_master
(
   cuc            varchar(100),
   grupo_tipo     varchar(100),
   tipo           varchar(100),
   linea          varchar(100),
   grupo_producto varchar(100),
   pais           varchar(100),
   campana_i      varchar(100),
   campana_f      varchar(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION 's3://hdata-belcorp/data-campaign-manager/top/lb/product-master'
tblproperties ("skip.header.line.count"="1");

msck repair table tmp_product_master;

CREATE EXTERNAL TABLE tmp_types_offer
(
   clase_to     varchar(100),
   grupo_to     varchar(100),
   c_to         varchar(100),
   comb_to      varchar(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION 's3://hdata-belcorp/data-campaign-manager/top/lb/types-offer'
tblproperties ("skip.header.line.count"="1");

msck repair table tmp_types_offer;

CREATE EXTERNAL TABLE tblpq_product_master
(
  cuc            varchar(100),
  grupo_tipo     varchar(100),
  tipo           varchar(100),
  linea          varchar(100),
  grupo_producto varchar(100),
  pais           varchar(100),
  campana_i      varchar(100),
  campana_f      varchar(100)
)
STORED AS PARQUET
LOCATION 's3://hdata-belcorp/parquet-campaign-manager/top/lb/product-master/'
tblproperties ("parquet.compress"="SNAPPY");

CREATE EXTERNAL TABLE tblpq_types_offer
(
  clase_to     varchar(100),
  grupo_to     varchar(100),
  c_to           varchar(100),
  comb_to      varchar(100)
)
STORED AS PARQUET
LOCATION 's3://hdata-belcorp/parquet-campaign-manager/top/lb/types-offer/'
tblproperties ("parquet.compress"="SNAPPY");

set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.optimize.sort.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.load.dynamic.partitions.thread=20;
set hive.mv.files.thread=25;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.blobstore.optimizations.enabled=false;

INSERT INTO TABLE tblpq_product_master
SELECT
cuc,
grupo_tipo,
tipo,
linea,
grupo_producto,
pais,
campana_i,
campana_f
FROM tmp_product_master;

INSERT INTO TABLE tblpq_types_offer
SELECT
clase_to,
grupo_to,
c_to,
comb_to
FROM tmp_types_offer;
