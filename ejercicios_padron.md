# 1- Creación de tablas en formato texto.
***1.1)*** Crear Base de datos "datos_padron"  

> create database datos_padron;

***1.2)***  Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los
datos mediante el comando LOAD DATA LOCAL INPATH. La tabla tendrá formato
texto y tendrá como delimitador de campo el caracter ';' y los campos que en el
documento original están encerrados en comillas dobles '"' no deben estar
envueltos en estos caracteres en la tabla de Hive (es importante indicar esto
utilizando el serde de OpenCSV, si no la importación de las variables que hemos
indicado como numéricas fracasará ya que al estar envueltos en comillas los toma
como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.  

> CREATE EXTERNAL TABLE IF NOT EXISTS padron_txt  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ('separatorChar' = '\073', 'quoteChar' = '\"', "escapeChar" = '\\')  
STORED AS TEXTFILE;  

> LOAD DATA LOCAL INPATH "/mnt/hgfs/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_txt;

***1.3)*** Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la
tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla
con una sentencia CTAS.)  

>create table padron_txt_2 as  
select cod_distrito as cod_distrito,
trim(desc_distrito) as desc_distrito,
cod_dist_barrio as cod_dist_barrio,
trim(desc_barrio) as desc_barrio,
cod_barrio as cod_barrio,
cod_dist_seccion as cod_dist_seccion,
cod_seccion as cod_seccion,
cod_edad_int as cod_edad_int,
espanoleshombres as espanoleshombres,
espanolesmujeres as espanolesmujeres,
extranjeroshombres as extranjeroshombres,
extranjerosmujeres as extranjerosmujeres
from padron_txt; 

***1.4)*** Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD
DATA.  

> Si ponemos Local significa que vamos a cargar de nuestro sistema de ficheros local , sino lo ponemos estaremos diciendo que queremos cargar de HDFS

***1.5)*** En este momento te habrás dado cuenta de un aspecto importante, los datos nulos
de nuestras tablas vienen representados por un espacio vacío y no por un
identificador de nulos comprensible para la tabla. Esto puede ser un problema para
el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva
tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para
esto primero comprobaremos que solo hay espacios en blanco en las variables
numéricas correspondientes a las últimas 4 variables de nuestra tabla (podemos
hacerlo con alguna sentencia de HiveQL) y luego aplicaremos las sentencias case
when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que
un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla
padron_txt.  

> Primero comprobamos que existen espacios en blanco en las últimas 4 variables de la tabla  

> select length(espanoleshombres), length(espanolesmujeres), length(extranjeroshombres), length(extranjerosmujeres) from padron_txt limit 20;

> Cambiamos de nombre a la tabla padron_txt

> alter table padron_txt rename to padron_original

> Ahora vamos a sustituir por 0 los valores en blanco, lo crearemos en la tabla padron_txt

>create table padron_txt as
select cod_distrito, desc_distrito,cod_dist_barrio,desc_barrio,cod_dist_seccion,cod_seccion,cod_edad_int,
     case when length(espanoleshombres)=0 then 0 else espanoleshombres end as espanoleshombres,
     case when length(espanolesmujeres)=0 then 0 else espanolesmujeres end as espanolesmujeres,
     case when length(extranjeroshombres)=0 then 0 else extranjeroshombres end as extranjeroshombres,
     case when length(extranjerosmujeres)=0 then 0 else extranjerosmujeres end as extranjerosmujeres
     from padron_original;

***1.6)*** Una manera tremendamente potente de solucionar todos los problemas previos
(tanto las comillas como los campos vacíos que no son catalogados como null y los
espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona
OpenCSV.
Para ello utilizamos :
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
 WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')
 Donde XXXXXX representa una expresión regular que debes completar y que
identifique el formato exacto con el que debemos interpretar cada una de las filas de
nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método
para crear de nuevo la tabla padron_txt_2.
Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que
conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos
nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin
comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta
tabla ha sido creada con las regex de OpenCSV.

> CREATE EXTERNAL TABLE IF NOT EXISTS padron_txt_limpio  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'  
WITH SERDEPROPERTIES ('input.regex'='\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"')
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

>LOAD DATA LOCAL INPATH "/mnt/hgfs/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_txt_limpio;  


# 2-Investigamos el formato columnar Parquet

***2.1)***  ¿Qué es CTAS?   
>Es la creación de tabla mediante una consulta. "Create table as select"  

***2.2)***  Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS.  

>create table padron_parquet
STORED AS PARQUET
as
select * from padron_txt;  

***2.3)*** Crear tabla Hive  padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios innecesarios) y otras dos tablas en formato parquet (padron_parquet y padron_parquet_2, la primera con espacios y la segunda sin ellos).

>create table padron_parquet_2  
stored as parquet   
as   
select cod_distrito as cod_distrito, 
trim(desc_distrito) as desc_distrito, 
cod_dist_barrio as cod_dist_barrio, 
trim(desc_barrio) as desc_barrio, 
cod_barrio as cod_barrio, 
cod_dist_seccion as cod_dist_seccion, 
cod_seccion as cod_seccion, 
cod_edad_int as cod_edad_int, 
espanoleshombres as espanoleshombres, 
espanolesmujeres as espanolesmujeres, 
extranjeroshombres as extranjeroshombres, 
extranjerosmujeres as extranjerosmujeres  
from padron_parquet;

***2.4)*** Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones posteriores que la tabla padron_parquet conserve los espacios innecesarios y la tabla padron_parquet_2 no los tenga. Dejo a tu elección cómo hacerlo.  

> CREATE TABLE IF NOT EXISTS padron_parquet_2  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ('separatorChar' = '\073', 'quoteChar' = '\"', "escapeChar" = '\\')  
STORED AS PARQUET; --esta es la diferencia, lo guardamos como parquet

> LOAD DATA LOCAL INPATH "/mnt/hgfs/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_parquet_2;  

***2.5)*** Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos.  
>Es el formato por defecto en Apache Spark, es soportado y muy usado por muchos frameworks y plataformas de Big Data
Es un formato de datos de columnar, ofrece optimizaciones en la E/S como la compresión que guarda los datos permitiendo un rápido acceso.   

***2.6)*** Comparar el tamaño de los ficheros de los datos de las tablas padron_txt (txt), padron_txt_2 (txt pero no incluye los espacios innecesarios), padron_parquet y padron_parquet_2 (alojados en hdfs cuya ruta se puede obtener de la propiedad location de cada tabla por ejemplo haciendo "show create table").  
>padron_txt: 17247076  
padron_txt_2: 12493163  
padron_parquet: 880973  
padron_parquet_2: 878932 

# Impala  
***3.1)***  ¿Qué es Impala?  

>Es una herramienta escalable de procesamiento masivo en paralelo, realiza consultas SQL interactivas con muy baja latencia. Soporta muchos formatos como Parquet, ORC, JSON, Avro...  
Utiliza los mismos metadatos, sintáxis y dirver que Hive  
Se puede usar desde la intefaz de Hue
Se utiliza para consultas explotarios y de descubrimiento de datos ya que nos proporciona baja latenica.  


***3.2)*** ¿En qué se diferencia de Hive?  
>La principal ventaja es la latencia, puede ser más rápida que Hive ya que éste tiene una alta latencia debido al MapReduce  

***3.3)*** Comando INVALIDATE METADATA, ¿en qué consiste?
>Hace ue los metadatos de la ase de datos/tabla especificada queden obsoletos
Se usa cuando:
>* Los metadatos han cambiado
>* Los cambios se han hecho desde otra instancia de Impala en el clúster  

***3.4)*** Hacer invalidate metadata en Impala de la base de datos datos_padron  

***3.5)*** Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.  

> select desc_distrito, desc_barrio, count(espanoleshombres) as 'n_esp_hombres', count(espanolesmujeres) as 'n_esp_mujeres', count(extranjeroshombres) as 'n_ex_hombres', count(extranjerosmujeres) as 'n_ex_mujeres' 
from padron_txt group by desc_distrito,desc_barrio  

***3.6)***  Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2 (No deberían incluir espacios innecesarios). ¿Alguna conclusión?

>select desc_distrito, desc_barrio, count(espanoleshombres) as 'n_esp_hombres', count(espanolesmujeres) as 'n_esp_mujeres', count(extranjeroshombres) as 'n_ex_hombres', count(extranjerosmujeres) as 'n_ex_mujeres' 
from padron_txt_2 group by desc_distrito,desc_barrio  

>select desc_distrito, desc_barrio, count(espanoleshombres) as 'n_esp_hombres', count(espanolesmujeres) as 'n_esp_mujeres', count(extranjeroshombres) as 'n_ex_hombres', count(extranjerosmujeres) as 'n_ex_mujeres' 
from padron_parquet_2 group by desc_distrito,desc_barrio  

> Han tardado lo mismo

***3.7)*** Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Alguna conclusión?  

> La consulta en padron_txt_2 no se nota respecto a padron_txt pero en padron_parquet_2 se hace mucho más rápido

***3.8)*** ¿Se percibe alguna diferencia de rendimiento entre Hive e Impala?  
>Sí la latencia es mucho menor que Hive, las consultas son más rápidas  

# 4 - Tablas particionadas  

***4.1)*** Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.  
>CREATE TABLE padron_particionado( 
COD_DISTRITO INT, 
COD_DIST_BARRIO INT, 
COD_BARRIO INT, 
COD_DIST_SECCION INT, 
COD_SECCION INT, 
COD_EDAD_INT INT, 
EspanolesHombres INT, 
EspanolesMujeres INT, 
ExtranjerosHombres INT, 
ExtranjerosMujeres INT) 
PARTITIONED BY (desc_distrito string,desc_barrio string)  

***4.2)*** Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2.  
>SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=non-strict;  
SET hive.exec.max.dynamic.partitions = 10000;  
SET hive.exec.max.dynamic.partitions.pernode = 1000;  
SET mapreduce.map.memory.mb = 2048;  
SET mapreduce.reduce.memory.mb = 2048;  
SET mapreduce.map.java.opts=-Xmx1800m;  


>FROM datos_padron.padron_parquet_2  
INSERT OVERWRITE TABLE datos_padron.padron_particionado partition  (desc_distrito, desc_barrio)
SELECT
CAST(cod_distrito AS INT),
CAST(cod_dist_barrio AS INT),
CAST(cod_barrio AS INT), 
CAST(cod_dist_seccion AS INT),
CAST(cod_seccion AS INT), 
CAST(cod_edad_int AS INT),
CAST(espanoleshombres AS INT),
CAST(espanolesmujeres AS INT),
CAST(extranjeroshombres AS INT),
CAST(extranjerosmujeres AS INT),
desc_distrito,
desc_barrio  

***4.3)*** Hacer invalidate metadata en Impala de la base de datos padron_particionado.  
>INVALIDATE METADATA datos_padron.padron_particionado  

***4.4)*** Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.  
>select desc_distrito, desc_barrio, count(espanoleshombres) as 'n_esp_hombres', count(espanolesmujeres) as 'n_esp_mujeres', count(extranjeroshombres) as 'n_ex_hombres', count(extranjerosmujeres) as 'n_ex_mujeres'  
from padron_txt_2  
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')  
group by desc_distrito,desc_barrio  

***4.5)*** Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_partitionado. ¿Alguna conclusión?  
> select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio  

> select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio

>Con padron_parquet devuelve 0 resultados, con padron_particionado devuelve 34 filas, la consulta se ejecuta más rápido debido a las particiones  

***4.6)*** Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?  
> select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio  

> select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio

>Con padron_parquet devuelve 0 resultados, con padron_particionado devuelve 34 filas, la consulta se ejecuta más rápido debido a las particiones  

***4.7)***Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos tanto en Hive como en Impala y sacar conclusiones.  
_Hive_
>select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio

>select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_txt_2
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio

>select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet_2
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio

> Impala ha sido mucho más rápido en las 3 tablas. La tabla más rápida fue la particionada. Si hacemos una consulta en Impala en la tabla particionada tardará entorno a un segundo  

# 5 - Tablas en HDFS  

***5.1)*** 5.1)Crear un documento de texto en el almacenamiento local que contenga una secuencia de números distribuidos en filas y separados por columnas, llámalo datos1 y que sea por ejemplo:  
>vim datos.txt  
1,2,3  
4,5,6  
7,8,9  

***5.2)*** Crear un segundo documento (datos2) con otros números pero la misma estructura.  
>vim datos2.txt   

***5.3)*** Crear un directorio en HDFS con un nombre a placer, por ejemplo, /test. Si estás en una máquina Cloudera tienes que asegurarte de que el servicio HDFS está activo ya que puede no iniciarse al encender la máquina (puedes hacerlo desde el Cloudera Manager). A su vez, en las máquinas Cloudera es posible (dependiendo de si usamos Hive desde consola o desde Hue) que no tengamos permisos para crear directorios en HDFS salvo en el directorio /user/cloudera.  
> hdfs dfs -mkdir /home/cloudera/test  

***5.4)*** Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando desde consola.  

>hdfs dfs -copyFromLocal /home/cloudera/ejercicios/datos1.txt /home/cloudera/test/  

***5.5)*** Desde Hive, crea una nueva database por ejemplo con el nombre numeros. Crea una tabla que no sea externa y sin argumento location con tres columnas numéricas, campos separados por coma y delimitada por filas. La llamaremos por ejemplo numeros_tbl.  
> create database numeros;
> use numeros;
> create table if not exists numeros_tbl
(
    n1 int,
    n2 int,
    n3 int
)
ROW FORMAT DELIMITED fields terminated by ',';  

***5.6)*** Carga los datos de nuestro fichero de texto datos1 almacenado en HDFS en la tabla de Hive. Consulta la localización donde estaban anteriormente los datos almacenados. ¿Siguen estando ahí? ¿Dónde están?. Borra la tabla, ¿qué ocurre con los datos almacenados en HDFS?  
>LOAD DATA LOCAL INPATH "/home/cloudera/test/datos1.txt" INTO TABLE numeros_tbl;
>Los datos1.txt en HDFS no están.
>Ahora están aquí: /user/hive/warehouse/numeros.db/numeros_tbl/datos1.txt
>Al borrar la tabla, los datos no están en ninguna lugar  

***5.7)*** Vuelve a mover el fichero de texto datos1 desde el almacenamiento local al directorio anterior en HDFS.  
>hdfs dfs -copyFromLocal /home/cloudera/ejercicios/datos1.txt /home/cloudera/test/  

***5.8)*** Desde Hive, crea una tabla externa sin el argumento location. Y carga datos1 (desde HDFS) en ella. ¿A dónde han ido los datos en HDFS? Borra la tabla ¿Qué ocurre con los datos en hdfs?  
>create external table if not exists numeros_tbl
(
    n1 int,
    n2 int,
    n3 int
)
ROW FORMAT DELIMITED fields terminated by ',';  

> LOAD DATA INPATH "hdfs:/home/cloudera/test/datos1.txt" INTO TABLE numeros_tbl
> Desaparecen de la carpeta donde se guardarn inicialmente, y se guardan en warehouse de hive
> drop table datos1.txt
> Los datos se mantienen en warehouse de Hive

***5.9)*** Borra el fichero datos1 del directorio en el que estén. Vuelve a insertarlos en el directorio que creamos inicialmente (/test). Vuelve a crear la tabla numeros desde hive pero ahora de manera externa y con un argumento location que haga referencia al directorio donde los hayas situado en HDFS (/test). No cargues los datos de ninguna manera explícita. Haz una consulta sobre la tabla que acabamos de crear que muestre todos los registros. ¿Tiene algún contenido?  
> hdfs dfs -rm /user/hive/warehouse/numeros.db/numeros_tbl/datos1.txt  

> hdfs dfs -copyFromLocal /home/cloudera/ejercicios/datos1.txt /home/cloudera/test/  

>create external table if not exists numeros_tbl
(
    n1 int,
    n2 int,
    n3 int
)
ROW FORMAT DELIMITED fields terminated by ','
location "/home/cloudera/test"  
> Si, los datos se han cargado automáticamente al estar en la misma ruta

***5.10)*** Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra?  
> hdfs dfs -copyFromLocal /home/cloudera/ejercicios/datos2.txt /home/cloudera/test/
> select * from numeros_tbl
> Al tener la misma estructura, los datos de datos1.txt y datos2.txt se han unido  

***5.11)*** Extrae conclusiones de todos estos anteriores apartados.

# 6 - Spark con Scala

***6.1)*** Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.

>import spark.implicits._
import org.apache.spark.sql.functions._
val padron = spark.read.format("csv")
.option("header","true")
.option("inferschema","true")
.option("emptyValue", 0)
.option("delimiter",";")
.load("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/Rango_Edades_Seccion_202204.csv")


>val padron2 = padron.withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
.withColumn("DESC_BARRIO",trim(col("desc_barrio")))

***6.2)*** De manera alternativa también se puede importar el csv con menos tratamiento en la importación y hacer todas  las modificaciones para alcanzar el mismo estado de limpieza delos datos con funciones de Spark.


>val padron_mal = spark.read.format("csv")
.option("header","true")
.option("inferschema","true")
.option("delimiter",";")
.load("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/Rango_Edades_Seccion_202204.csv")

>val padron_cambiado = padron_mal.na.fill(value=0)
.withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
.withColumn("DESC_BARRIO",trim(col("desc_barrio")))

***6.3)*** Enumera todos los barrios diferentes.

>display(padron2.select(countDistinct("desc_barrio")).alias("n_barrios"))

***6.4)*** Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barriosdiferentes que hay.

>padron_cambiado.createOrReplaceTempView("padron")
spark.sql("select count(distinct(desc_barrio)) from padron").show()

***6.5)*** Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".

>val padron3 = padron_cambiado.withColumn("longitud",length(col("desc_distrito")))
padron3.show()

***6.6)**** Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla. 

>val padron4 = padron3.withColumn("valor5",lit(5))
padron4.show()

***6.7)*** Borra esta columna.

>val padron5 = padron4.drop(col("valor5"))
padron5.show()

***6.8)*** Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.

>val padron_particionado = padron5.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

***6.9)*** Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estadode los rdds almacenados.

>padron_particionado.cache()

***6.10)*** Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

>padron_particionado.groupBy(col("desc_barrio"),col("desc_distrito"))
.agg(count(col("espanolesHombres")).alias("espanolesHombres"),
     count(col("espanolesMujeres")).alias("espanolesMujeres"),
     count(col("extranjerosHombres")).alias("extranjerosHombres"),
     count(col("extranjerosMujeres")).alias("extranjerosMujeres"))
.orderBy(desc("extranjerosMujeres"),desc("extranjerosHombres"))
.show()

***6.11)*** Elimina el registro en caché.

>spark.catalog.clearCache()

***6.12)*** Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.

>val df1 = padron_particionado.select(col("DESC_BARRIO"),col("DESC_DISTRITO"),col("ESPANOLESHOMBRES"))
.groupBy(col("DESC_BARRIO"),col("DESC_DISTRITO"))
.agg(sum(col("ESPANOLESHOMBRES")).alias("ESPANOLESHOMBRES"))
.join(padron, padron_particionado("desc_barrio") === padron("desc_barrio") &&
padron_particionado("desc_distrito") === padron("desc_distrito"),"inner").show()
val res = df1.join(padron,  padron("DESC_BARRIO") === df1("DESC_BARRIO") &&
padron("DESC_DISTRITO") === df1("DESC_DISTRITO"), "inner")

>display(res)

***6.13)*** Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).

>import org.apache.spark.sql.expressions.Window
val padron_ventana = padron.withColumn("TotalEspHom", sum(col("espanoleshombres")).over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))
display(padron_ventana)

***6.14)*** Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a este:

>val distritos = Seq("BARAJAS","CENTRO","RETIRO")
val padron_pivot = padron_particionado.groupBy("cod_edad_int").pivot("desc_distrito", distritos).sum("espanolesMujeres").orderBy(col("cod_edad_int"))
padron_pivot.show()

***6.15)*** Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.

>val padron_porcen = padron_pivot.withColumn("porcentaje_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))
.withColumn("porcentaje_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))
.withColumn("porcentaje_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

>padron_porcen.show()

***6.16)*** Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.

>padron.write.format("csv")
.option("header","true")
.mode("overwrite")
.partitionBy("desc_distrito","desc_barrio")
.save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com")

***6.17)*** Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.

>padron.write.format("parquet")
.mode("overwrite")
.partitionBy("desc_distrito","desc_barrio")
.save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com")

# 6 - Spark con Python
***6.1)*** Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema.
>padron = spark.read.format("csv")\
.option("header","true")\
.option("inferschema","true")\
.option("emptyValue", 0)\
.option("delimiter",";")\
.load("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/Rango_Edades_Seccion_202204.csv")

>from pyspark.sql import SQLContext
from pyspark.sql.functions import *
    
>padron2 = padron.withColumn("DESC_DISTRITO",trim(col("desc_distrito")))\
.withColumn("DESC_BARRIO",trim(col("desc_barrio")))

***6.2)*** De manera alternativa también se puede importar el csv con menos tratamiento en la importación y hacer todas  las modificaciones para alcanzar el mismo estado de limpieza delos datos con funciones de Spark.
>padron_mal = spark.read.format("csv")\
.option("header","true")\
.option("inferschema","true")\
.option("delimiter",";")\
.load("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/Rango_Edades_Seccion_202204.csv")

>padron_cambiado = padron_mal.na.fill(value=0)\
.withColumn("DESC_DISTRITO",trim(col("desc_distrito")))\
.withColumn("DESC_BARRIO",trim(col("desc_barrio")))

***6.3)*** Enumera todos los barrios diferentes.
>padron2.select(countDistinct("desc_barrio").alias("n_barrios")).show()

***6.4)*** Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barriosdiferentes que hay.
>padron_cambiado.createOrReplaceTempView("padron")
spark.sql("select count(distinct(desc_barrio)) as n_barrios from padron").show()

***6.5)*** Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
>padron3 = padron_cambiado.withColumn("longitud",length(col("desc_distrito")))
padron3.show()

***6.6)**** Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla. 
>padron4 = padron3.withColumn("valor5",lit(5))
padron4.show()

***6.7)*** Borra esta columna.
>padron5 = padron4.drop(col("valor5"))
padron5.show()

***6.8)*** Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.
>padron_particionado = padron5.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

***6.9)*** Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estadode los rdds almacenados.
>padron_particionado.cache()

***6.10)*** Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".
>padron_particionado.groupBy(col("desc_barrio"),col("desc_distrito"))\
.agg(count(col("espanolesHombres")).alias("espanolesHombres"),\
     count(col("espanolesMujeres")).alias("espanolesMujeres"),\
     count(col("extranjerosHombres")).alias("extranjerosHombres"),\
     count(col("extranjerosMujeres")).alias("extranjerosMujeres"))\
.orderBy(desc("extranjerosMujeres"),desc("extranjerosHombres"))\
.show()

***6.11)*** Elimina el registro en caché.
>spark.catalog.clearCache()

***6.12)*** Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.
>df1 = padron_particionado.select(col("DESC_BARRIO"),col("DESC_DISTRITO"),col("ESPANOLESHOMBRES"))\
.groupBy(col("DESC_BARRIO"),col("DESC_DISTRITO"))\
.agg(sum(col("ESPANOLESHOMBRES")).alias("ESPANOLESHOMBRES"))

>res = df1.join(padron,  (padron.DESC_BARRIO == df1.DESC_BARRIO) &\
       (padron.DESC_DISTRITO == df1.DESC_DISTRITO), "inner")\

>display(res)

***6.13)*** Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
>from pyspark.sql.window import Window
padron_ventana = padron.withColumn("TotalEspHom", sum(col("espanoleshombres")).over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO")))
display(padron_ventana)

***6.14)*** Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas . El aspecto debe ser similar a este:
>distritos = ("BARAJAS","CENTRO","RETIRO")
padron_pivot = padron_particionado.groupBy("cod_edad_int").pivot("desc_distrito", distritos).sum("espanolesMujeres").orderBy(col("cod_edad_int"))
padron_pivot.show()

***6.15)*** Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.
>padron_porcen = padron_pivot.withColumn("porcentaje_barajas",round(col("barajas")/(col("barajas")+col("centro")+col("retiro"))*100,2))\
.withColumn("porcentaje_centro",round(col("centro")/(col("barajas")+col("CENTRO")+col("RETIRO"))*100,2))\
.withColumn("porcentaje_retiro",round(col("retiro")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2))

***6.16)*** Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.
>padron.write.format("csv")
.option("header","true")
.mode("overwrite")
.partitionBy("desc_distrito","desc_barrio")
.save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com")

***6.17)*** Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.
>padron.write.format("parquet")\
.mode("overwrite")\
.partitionBy("desc_distrito","desc_barrio")\
.save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com")

# 7 - Spark y Hive

***7.1)*** Por último, prueba a hacer los ejercicios sugeridos en la parte de Hive con el csv "Datos Padrón" (incluyendo la importación con Regex) utilizando desde Spark EXCLUSIVAMENTE sentencias spark.sql, es decir, importar los archivos desde local directamente como tablasde Hive y haciendo todas las consultas sobre estas tablas sin transformarlas en ningún momento en DataFrames ni DataSets.  

>import org.apache.spark.sql.SparkSession

***7.1.1)*** Crear Base de datos "datos_padron"  

> spark.sql("create database datos_padron")

***7.1.2)***  Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los
datos mediante el comando LOAD DATA LOCAL INPATH. La tabla tendrá formato
texto y tendrá como delimitador de campo el caracter ';' y los campos que en el
documento original están encerrados en comillas dobles '"' no deben estar
envueltos en estos caracteres en la tabla de Hive (es importante indicar esto
utilizando el serde de OpenCSV, si no la importación de las variables que hemos
indicado como numéricas fracasará ya que al estar envueltos en comillas los toma
como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.  

> spark.sql("use datos_padron")

> spark.sql("""
CREATE TABLE IF NOT EXISTS padron_txt  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ('separatorChar' = '\073', 'quoteChar' = '\"', "escapeChar" = '\\')  
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");
""")

> spark.sql("""LOAD DATA LOCAL INPATH "C:/Users/alejandro.ibanez/Desktop/padron madrid/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_txt""")  

***7.1.3)*** Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la
tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla
con una sentencia CTAS.)  

>spark.sql("""create table padron_txt_2 as  
select cod_distrito as cod_distrito,
trim(desc_distrito) as desc_distrito,
cod_dist_barrio as cod_dist_barrio,
trim(desc_barrio) as desc_barrio,
cod_barrio as cod_barrio,
cod_dist_seccion as cod_dist_seccion,
cod_seccion as cod_seccion,
cod_edad_int as cod_edad_int,
espanoleshombres as espanoleshombres,
espanolesmujeres as espanolesmujeres,
extranjeroshombres as extranjeroshombres,
extranjerosmujeres as extranjerosmujeres
from padron_txt""")

***7.1.5)*** En este momento te habrás dado cuenta de un aspecto importante, los datos nulos
de nuestras tablas vienen representados por un espacio vacío y no por un
identificador de nulos comprensible para la tabla. Esto puede ser un problema para
el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva
tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para
esto primero comprobaremos que solo hay espacios en blanco en las variables
numéricas correspondientes a las últimas 4 variables de nuestra tabla (podemos
hacerlo con alguna sentencia de HiveQL) y luego aplicaremos las sentencias case
when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que
un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla
padron_txt.  

> Primero comprobamos que existen espacios en blanco en las últimas 4 variables de la tabla  

> spark.sql("""select length(espanoleshombres), length(espanolesmujeres), length(extranjeroshombres), length(extranjerosmujeres) from padron_txt limit 20;""").show()

> Cambiamos de nombre a la tabla padron_txt

> spark.sql("""alter table padron_txt rename to padron_original""")

> Ahora vamos a sustituir por 0 los valores en blanco, lo crearemos en la tabla padron_txt

>spark.sql("""create table padron_txt as
select cod_distrito, desc_distrito,cod_dist_barrio,desc_barrio,cod_dist_seccion,cod_seccion,cod_edad_int,
     case when length(espanoleshombres)=0 then 0 else espanoleshombres end as espanoleshombres,
     case when length(espanolesmujeres)=0 then 0 else espanolesmujeres end as espanolesmujeres,
     case when length(extranjeroshombres)=0 then 0 else extranjeroshombres end as extranjeroshombres,
     case when length(extranjerosmujeres)=0 then 0 else extranjerosmujeres end as extranjerosmujeres
     from padron_original;""")

***7.1.6)*** Una manera tremendamente potente de solucionar todos los problemas previos
(tanto las comillas como los campos vacíos que no son catalogados como null y los
espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona
OpenCSV.
Para ello utilizamos :
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
 WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')
 Donde XXXXXX representa una expresión regular que debes completar y que
identifique el formato exacto con el que debemos interpretar cada una de las filas de
nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método
para crear de nuevo la tabla padron_txt_2.
Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que
conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos
nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin
comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta
tabla ha sido creada con las regex de OpenCSV.

> spark.sql("""CREATE TABLE IF NOT EXISTS padron_txt_limpio  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'  
WITH SERDEPROPERTIES ('input.regex'='\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"\\;*\\"(\\w*)\\s*\\"')
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");""")

>spark.sql("""LOAD DATA LOCAL INPATH "C:/Users/alejandro.ibanez/Desktop/padron madrid/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_txt_limpio;""")

***7.2.2)***  Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS.  

>spark.sql("""create table padron_parquet
STORED AS PARQUET
as
select * from padron_txt;""")

***7.2.3)*** Crear tabla Hive  padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios innecesarios) y otras dos tablas en formato parquet (padron_parquet y padron_parquet_2, la primera con espacios y la segunda sin ellos).

>spark.sql("""create table padron_parquet_2  
stored as parquet   
as   
select cod_distrito as cod_distrito, 
trim(desc_distrito) as desc_distrito, 
cod_dist_barrio as cod_dist_barrio, 
trim(desc_barrio) as desc_barrio, 
cod_dist_seccion as cod_dist_seccion, 
cod_seccion as cod_seccion, 
cod_edad_int as cod_edad_int, 
espanoleshombres as espanoleshombres, 
espanolesmujeres as espanolesmujeres, 
extranjeroshombres as extranjeroshombres, 
extranjerosmujeres as extranjerosmujeres  
from padron_parquet""")

***7.2.4)*** Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones posteriores que la tabla padron_parquet conserve los espacios innecesarios y la tabla padron_parquet_2 no los tenga. Dejo a tu elección cómo hacerlo.  

> spark.sql("""CREATE TABLE IF NOT EXISTS padron_parquet_2  
(  
    cod_distrito INT,   
    desc_distrito STRING,   
    cod_dist_barrio INT,  
    desc_barrio STRING,  
    cod_barrio INT,  
    cod_dist_seccion INT,  
    cod_seccion INT,  
    cod_edad_int INT,  
    EspanolesHombres INT,  
    EspanolesMujeres INT,  
    ExtranjerosHombres INT,  
    ExtranjerosMujeres INT 
)   
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES ('separatorChar' = '\073', 'quoteChar' = '\"', "escapeChar" = '\\')  
STORED AS PARQUET; --esta es la diferencia, lo guardamos como parquet""")

> LOAD DATA LOCAL INPATH "/mnt/hgfs/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_parquet_2;  

***7.4.1)*** Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.  
>spark.sql("""CREATE TABLE padron_particionado( 
COD_DISTRITO INT, 
COD_DIST_BARRIO INT, 
COD_DIST_SECCION INT, 
COD_SECCION INT, 
COD_EDAD_INT INT, 
EspanolesHombres INT, 
EspanolesMujeres INT, 
ExtranjerosHombres INT, 
ExtranjerosMujeres INT) 
PARTITIONED BY (desc_distrito string,desc_barrio string)""")

***7.4.2)*** Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2.  
>spark.sql("""SET hive.exec.dynamic.partition=true
SET hive.exec.dynamic.partition.mode=non-strict
SET hive.exec.max.dynamic.partitions = 10000
SET hive.exec.max.dynamic.partitions.pernode = 1000  
SET mapreduce.map.memory.mb = 2048
SET mapreduce.reduce.memory.mb = 2048  
SET mapreduce.map.java.opts=-Xmx1800m""")  


>spark.sql("""FROM datos_padron.padron_parquet_2  
INSERT OVERWRITE TABLE datos_padron.padron_particionado partition  (desc_distrito, desc_barrio)
SELECT
CAST(cod_distrito AS INT),
CAST(cod_dist_barrio AS INT),
CAST(cod_dist_seccion AS INT),
CAST(cod_seccion AS INT), 
CAST(cod_edad_int AS INT),
CAST(espanoleshombres AS INT),
CAST(espanolesmujeres AS INT),
CAST(extranjeroshombres AS INT),
CAST(extranjerosmujeres AS INT),
desc_distrito,
desc_barrio""")

***7.4.3)*** Hacer invalidate metadata en Impala de la base de datos padron_particionado.  
>spark.sql("""INVALIDATE METADATA datos_padron.padron_particionado""")

***7.4.4)*** Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.  
>spark.sql("""select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres  
from padron_txt_2  
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')  
group by desc_distrito,desc_barrio""").show()

***7.4.5)*** Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_partitionado. ¿Alguna conclusión?  
> spark.sql("""select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

> spark.sql("""select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

>Con padron_parquet devuelve 0 resultados, con padron_particionado devuelve 34 filas, la consulta se ejecuta más rápido debido a las particiones  

***7.4.6)*** Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?  
> spark.sql("""select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

> spark.sql("""select desc_distrito, desc_barrio, count(espanoleshombres) as n_esp_hombres, count(espanolesmujeres) as n_esp_mujeres, count(extranjeroshombres) as n_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

>Con padron_parquet devuelve 0 resultados, con padron_particionado devuelve 34 filas, la consulta se ejecuta más rápido debido a las particiones  

***7.4.7)***Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos tanto en Hive como en Impala y sacar conclusiones.  
_Hive_
>spark.sql("""select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_particionado
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

>spark.sql("""select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_txt_2
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

>spark.sql("""select desc_distrito, desc_barrio, avg(espanoleshombres) as media_esp_hombres, max(espanolesmujeres) as max_esp_mujeres, min(extranjeroshombres) as min_ex_hombres, count(extranjerosmujeres) as n_ex_mujeres 
from padron_parquet_2
where desc_distrito in ('CENTRO','LATINA','CHAMARTIN','TETUAN','VICALVARO','BARAJAS')
group by desc_distrito,desc_barrio""").show()

> Impala ha sido mucho más rápido en las 3 tablas. La tabla más rápida fue la particionada. Si hacemos una consulta en Impala en la tabla particionada tardará entorno a un segundo

# Algunas diferencias Scala y Python
* Scala utiliza el val, Python no  
* En Python en los saltos de línea hay que poner \  
* La igualdad en Python es ==, en Scala ===  
* En Python &, en Scala &&
* En Python el & se resuelve antes que ==
* En Python podemos declarar un array sólo con [1,2,3] ó (1,2,3)