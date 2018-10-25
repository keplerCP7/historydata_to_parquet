CREATE EXTERNAL TABLE corp_det_tactica_condicion(
  codcanalventa         varchar(2),
  codpais               varchar(2),
  aniocampana           varchar(6),
  numoferta             integer,
  codestrategia         varchar(3),
  codcondicion          varchar(9),
  codproducto           varchar(9),
  codventa              varchar(5),
  codtipooferta         varchar(3),
  unidades              integer,
  flagdigitable         integer,
  destipooferta         varchar(50),
  factorcuadre          decimal(34,4),
  nrogrupo              integer,
  indcuadre             varchar(40),
  indpadre              integer,
  preciooferta          decimal(34,4),
  preciovtapropuestomn  decimal(34,4),
  precionormalmn        decimal(34,4),
  descomercialoferta    varchar(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/DET_TACTICA_CONDICION/';

msck repair table corp_det_tactica_condicion;

CREATE EXTERNAL TABLE corp_det_tactica_oferta(
  codcanalventa         varchar(2),
  codpais               varchar(2),
  aniocampana           varchar(6),
  numoferta             integer,
  codestrategia         varchar(3),
  codproducto           varchar(9),
  codventa              varchar(5),
  codcatalogo           varchar(2),
  descatalogo           varchar(50),
  codtipooferta         varchar(3),
  unidades              integer,
  destipooferta         varchar(52),
  flagdigitable         smallint,
  factorcuadre          decimal(34,4),
  indcuadre             varchar(40),
  indpadre              smallint,
  nrogrupo              integer,
  preciooferta          decimal(34,4),
  preciovtapropuestomn  decimal(34,4),
  precionormalmn        decimal(34,4),
  descomercialoferta    varchar(100))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/DET_TACTICA_OFERTA/';

msck repair table corp_det_tactica_oferta;

CREATE EXTERNAL TABLE corp_mae_tactica_condicion(
  codcanalventa      varchar(2),
  codpais            varchar(2),
  aniocampana        varchar(6),
  numoferta          integer,
  codestrategia      varchar(3),
  codcondicion       varchar(9),
  codcatalogo        varchar(2),
  descatalogo        varchar(50),
  unidadescondicion  integer,
  rangoinfuu         integer,
  rangosupuu         integer,
  montocondicion     decimal(34,4),
  rangoinfmn         decimal(34,4),
  rangosupmn         decimal(34,4),
  preciorango        decimal(34,4))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/MAE_TACTICA_CONDICION/';

msck repair table corp_mae_tactica_condicion;

CREATE EXTERNAL TABLE corp_mae_tactica_vinculada(
  codcanalventa      varchar(2),
  codpais            varchar(2),
  aniocampana        varchar(6),
  numoferta          integer,
  codestrategia      varchar(3),
  nropagina          integer,
  nropaginadigital   integer,
  tipocuadre         integer,
  desestrategia      varchar(50),
  codtipomedioventa  varchar(3),
  codtipotactica     integer)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/MAE_TACTICA_VINCULADA/';

msck repair table corp_mae_tactica_vinculada;

CREATE EXTERNAL TABLE corp_par_tipo_cuadre(
  tipocuadre  integer,
  descuadre   varchar(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/PAR_TIPO_CUADRE/';

msck repair table corp_par_tipo_cuadre;

CREATE EXTERNAL TABLE corp_par_tipo_tactica(
  codtipotactica    integer,
  codestrategia     varchar(3),
  destipotactica    varchar(50),
  destacticasicc    varchar(50),
  destacticaplanit  varchar(50))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI/PAR_TIPO_TACTICA/';

msck repair table corp_par_tipo_tactica;

--partition tables

set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.load.dynamic.partitions.thread=20;
set hive.mv.files.thread=25;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE tblpq_corp_det_tactica_condicion(
  codcanalventa         varchar(2),
  numoferta             integer,
  codestrategia         varchar(3),
  codcondicion          varchar(9),
  codproducto           varchar(9),
  codventa              varchar(5),
  codtipooferta         varchar(3),
  unidades              integer,
  flagdigitable         integer,
  destipooferta         varchar(50),
  factorcuadre          decimal(34,4),
  nrogrupo              integer,
  indcuadre             varchar(40),
  indpadre              integer,
  preciooferta          decimal(34,4),
  preciovtapropuestomn  decimal(34,4),
  precionormalmn        decimal(34,4),
  descomercialoferta    varchar(100))
PARTITIONED BY(codpais varchar(2), aniocampana varchar(6))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/DET_TACTICA_CONDICION/'
tblproperties ("parquet.compress"="SNAPPY");

CREATE EXTERNAL TABLE tblpq_corp_det_tactica_oferta(
  codcanalventa         varchar(2),
  numoferta             integer,
  codestrategia         varchar(3),
  codproducto           varchar(9),
  codventa              varchar(5),
  codcatalogo           varchar(2),
  descatalogo           varchar(50),
  codtipooferta         varchar(3),
  unidades              integer,
  destipooferta         varchar(52),
  flagdigitable         smallint,
  factorcuadre          decimal(34,4),
  indcuadre             varchar(40),
  indpadre              smallint,
  nrogrupo              integer,
  preciooferta          decimal(34,4),
  preciovtapropuestomn  decimal(34,4),
  precionormalmn        decimal(34,4),
  descomercialoferta    varchar(100))
PARTITIONED BY(codpais varchar(2), aniocampana varchar(6))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/det_tactica_oferta/'
tblproperties ("parquet.compress"="SNAPPY");


CREATE EXTERNAL TABLE tblpq_corp_mae_tactica_condicion(
  codcanalventa      varchar(2),
  numoferta          integer,
  codestrategia      varchar(3),
  codcondicion       varchar(9),
  codcatalogo        varchar(2),
  descatalogo        varchar(50),
  unidadescondicion  integer,
  rangoinfuu         integer,
  rangosupuu         integer,
  montocondicion     decimal(34,4),
  rangoinfmn         decimal(34,4),
  rangosupmn         decimal(34,4),
  preciorango        decimal(34,4))
PARTITIONED BY(codpais varchar(2), aniocampana varchar(6))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/mae_tactica_condicion/'
tblproperties ("parquet.compress"="SNAPPY");

CREATE EXTERNAL TABLE tblpq_corp_mae_tactica_vinculada(
  codcanalventa      varchar(2),
  numoferta          integer,
  codestrategia      varchar(3),
  nropagina          integer,
  nropaginadigital   integer,
  tipocuadre         integer,
  desestrategia      varchar(50),
  codtipomedioventa  varchar(3),
  codtipotactica     integer)
PARTITIONED BY(codpais varchar(2), aniocampana varchar(6))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/mae_tactica_vinculada/'
tblproperties ("parquet.compress"="SNAPPY");

CREATE EXTERNAL TABLE tblpq_corp_par_tipo_cuadre(
  tipocuadre  integer,
  descuadre   varchar(50))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/par_tipo_cuadre/'
tblproperties ("parquet.compress"="SNAPPY");

CREATE EXTERNAL TABLE tblpq_corp_par_tipo_tactica(
  codtipotactica    integer,
  codestrategia     varchar(3),
  destipotactica    varchar(50),
  destacticasicc    varchar(50),
  destacticaplanit  varchar(50))
STORED AS PARQUET
LOCATION 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/HANABI-parquet/par_tipo_tactica/'
tblproperties ("parquet.compress"="SNAPPY");


--insert data into parquet tables
INSERT INTO TABLE tblpq_corp_det_tactica_condicion partition(codpais, aniocampana)
SELECT
  codcanalventa,
  numoferta,
  codestrategia,
  codcondicion,
  codproducto,
  codventa,
  codtipooferta,
  unidades,
  flagdigitable,
  destipooferta,
  factorcuadre,
  nrogrupo,
  indcuadre,
  indpadre,
  preciooferta,
  preciovtapropuestomn,
  precionormalmn,
  descomercialoferta,
  codpais,
  aniocampana
from corp_det_tactica_condicion;

INSERT INTO TABLE tblpq_corp_det_tactica_oferta partition(codpais, aniocampana)
SELECT
  codcanalventa,
  numoferta,
  codestrategia,
  codproducto,
  codventa,
  codcatalogo,
  descatalogo,
  codtipooferta,
  unidades,
  destipooferta,
  flagdigitable,
  factorcuadre,
  indcuadre,
  indpadre,
  nrogrupo,
  preciooferta,
  preciovtapropuestomn,
  precionormalmn,
  descomercialoferta,
  codpais,
  aniocampana
from corp_det_tactica_oferta;

INSERT INTO TABLE tblpq_corp_mae_tactica_condicion partition(codpais, aniocampana)
SELECT
  codcanalventa,
  numoferta,
  codestrategia,
  codcondicion,
  codcatalogo,
  descatalogo,
  unidadescondicion,
  rangoinfuu,
  rangosupuu,
  montocondicion,
  rangoinfmn,
  rangosupmn,
  preciorango,
  codpais,
  aniocampana
from corp_mae_tactica_condicion;

INSERT INTO TABLE tblpq_corp_mae_tactica_vinculada partition(codpais, aniocampana)
SELECT
  codcanalventa,
  numoferta,
  codestrategia,
  nropagina,
  nropaginadigital,
  tipocuadre,
  desestrategia,
  codtipomedioventa,
  codtipotactica,
  codpais,
  aniocampana
from corp_mae_tactica_vinculada;

INSERT INTO TABLE tblpq_corp_par_tipo_cuadre
SELECT
  tipocuadre,
  descuadre
from corp_par_tipo_cuadre;

INSERT INTO TABLE tblpq_corp_par_tipo_tactica
SELECT
  codtipotactica,
  codestrategia,
  destipotactica,
  destacticasicc,
  destacticaplanit
from corp_par_tipo_tactica;
