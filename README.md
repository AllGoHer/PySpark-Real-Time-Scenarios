# PySpark-Real-Time-Scenarios
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/243c7835-4180-4222-9d2d-bee10d10bb73)  ![image](https://github.com/user-attachments/assets/38300f25-036a-4fcd-8f5c-9483f242aad7)  ![image](https://github.com/user-attachments/assets/1adbe62e-4c2c-4d74-85d8-02bbb343404c)  ![image](https://github.com/user-attachments/assets/4c14690a-0e2a-4eac-8c3b-e5dab0f08443)

## 🎯 Descripción del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________
La ingesta de datos en tiempo real desde APIs o sistemas transaccionales suele generar archivos JSON altamente anidados. Hacer un df.write.mode("overwrite") destruiría el historial, y hacer un append sin lógica generaría filas duplicadas infinitamente.

Este proyecto implementa un pipeline ETL robusto que simula el comportamiento de un Data Warehouse en producción. Toma un JSON anidado complejo (Structs dentro de Structs y Arrays), lo aplanada a un formato tabular relacional, y lo carga incrementalmente en un Data Warehouse usando MERGE / Upserts, sin afectar datos históricos existentes.

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura del Pipeline (Bronze → Silver)
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/8dc82719-b05d-4aab-9cc7-2c3c8dca8e91)

________________________________________________________________________________________________________________________________________________________________________________________________________________
🧠 Decisiones Arquitectónicas 
________________________________________________________________________________________________________________________________________________________________________________________________________________

**1. Flattening Inteligente de JSON Anidado**
Un error común en Data Engineering es usar explode sin cuidado, lo cual genera una explosión de filas (Row Explosion).
Este proyecto demuestra cómo extraer profundo en Structs anidados (ej. customer.location.city) y aplicar explode solo en los Arrays reales (items, delivery_updates), evitando la creación de miles de filas nulas o corruptas.

**2. Carga Incremental (SCD Tipo 1 - Upsert)**
El problema: Si haces un append(), la orden ORD001 aparecerá dos veces si el evento llega dos veces.
La solución: El uso de MERGE INTO... ON order_id WHEN MATCHED THEN UPDATE. Si la clave ya existe en el Warehouse, actualiza la fila en lugar de crear una nueva. Si es nueva, la inserta. Garantiza la integridad de los datos históricos.

**3. Modularidad con Programación Orientada a Objetos (OOP)**
En lugar de escribir un script gigante e ilegible de 300 líneas, el código utiliza Clases de Python (DataValidation, DataTransformer). Esto demuestra separación de responsabilidades (Single Responsibility), facilidad de pruebas unitarias y un código limpio listo para producción.
________________________________________________________________________________________________________________________________________________________________________________________________________________

📊 Prueba Visual: Incremental Load en Acción
________________________________________________________________________________________________________________________________________________________________________________________________________________

La mejor manera de demostrar un Upsert es ver cómo se comportan los datos a través del tiempo. Aquí está la evolución de la Orden ORD001 en el Data Warehouse:

| run_id | order_id | order_date | amount | ¿Qué pasó en el Data Warehouse? |
|:---:|:---|:---|:---|:---|
| 1 (Inicial) | 1 | 2025-08-02 | 246.84 | Se insertó por primera vez. |
| 2, 3, 4 | 1 | 2025-08-05 | 246.84 | No cambió. El evento fue recibido, pero el monto era igual. Se ignora el UPSERT.
| **6 (Actualización)** | 1 | 2026-08-11 | **248.69** | **Se ACTUALIZÓ el monto de $246.84 a $248.69 sin tocar el historial.**

________________________________________________________________________________________________________________________________________________________________________________________________________________
📂 Estructura del Proyecto
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/cc072a23-d68b-4cd3-9e3c-4bfcffa508fb)

________________________________________________________________________________________________________________________________________________________________________________________________________________
## 🧠 DESARROLLO
________________________________________________________________________________________________________________________________________________________________________________________________________________

1.	En la pestaña workspace de Databricks, creamos un archivo llamado PySparkScenarios.
2.	Luego en la pestaña de catalog creamos un cátalogo llamado pyspark_catalogo.

![image](https://github.com/user-attachments/assets/d3f022e6-4a5c-4002-b2a9-9234f647ca9e)

3.	Ahora creamos un esquema llamado source.

![image](https://github.com/user-attachments/assets/470f63a3-59bc-434e-a00e-ebbbe1b26ddd)

4.	Ahora vamos a SQL Editor y crearemos una tabla.

![image](https://github.com/user-attachments/assets/c92541dd-4ca8-427a-b79f-0e47614507d4)

Código:

       CREATE TABLE products
        (
         id INT,
         name STRING,
         price INT,
         category STRING,
         updatedDate TIMESTAMP
        );
       INSERT INTO products
        VALUES
        (1, 'iPhone', 1000, 'electronics', current_timestamp()),
        (2, 'Macbook', 2000, 'electronics', current_timestamp()),
        (3, 'T-Shirt', 50, 'clothing', current_timestamp()),
        (4, 'Shirt', 100, 'clothing', current_timestamp()),
        (5, 'Pants', 150, 'clothing', current_timestamp())

        
![image](https://github.com/user-attachments/assets/0ea0dafd-c437-4d75-985a-30ff6e6ee3b2)

•	Querying Source

Código:

        SELECT * FROM pyspark_catalogo.source.products

![image](https://github.com/user-attachments/assets/6c64eb8b-aa70-4ff4-8ce6-7ea1e19f072b)

Ahora creamos un volumen llamado db_volume.

![image](https://github.com/user-attachments/assets/472f5dc9-e5a6-4421-8490-eba709acf666)

![image](https://github.com/user-attachments/assets/1fde6b69-b7d2-44c4-98c8-b6e2c970fc67)

Ahora creamos un DataFrame para lectura incremental de datos.

Código:

       df = spark.sql('select * from pyspark_catalogo.source.products')
       display(df)

![image](https://github.com/user-attachments/assets/d20f05be-470c-4bb6-9d8a-8d8e94dfd8db)

**•	UPSERTS**

Creamos un objeto delta con el siguiente código.

Código:

          # Creating Delta Object
          
         from delta.tables import DeltaTable

        try:
            dlt_obj = DeltaTable.forPath(spark, "/Volumes/pyspark_catalogo/source/db_volume/products_sink/")

            dlt_obj.alias("trg").merge(
                df.alias("src"),
                "src.id = trg.id")\
                .whenMatchedUpdatedAll(condition="src.updateDate >= trg.updateDate")\
                .whenNotMatchedInsertAll()\
                .execute()
            print("This is upserting now")

        except:
            df.write.format("delta")\
                .mode("Overwrite")\
                .save("/Volumes/pyspark_catalogo/source/db_volume/products_sink/")


Lo que estás viendo aquí se conoce en la industria como un patrón "Bootstrap & Upsert" (Inicializar y Actualizar). Es la forma correcta de manejar tablas en tiempo real (Streaming o micro-lotes) donde no sabes si la tabla ya existe o si es la primera vez que corres el código.

**•	En el siguiente bloque explico a detalle que es lo hace cada línea de código para mayor comprensión**

#### Creating Delta Object

**[HASHTAG 1]** Importamos la clase específica de Delta Lake que nos permite usar la función MERGE (Upsert).
from delta.tables import DeltaTable

try
    # [HASHTAG 2] Intentamos apuntar a un directorio que YA debería contener una tabla Delta existente.
    # Si es la primera vez que corres esto y la carpeta está vacía, esta línea FALLARÁ y saltará al 'except'.
    dlt_obj = DeltaTable.forPath(spark, "/Volumes/pyspark_catalogo/source/db_volume/products_sink/")

    # [HASHTAG 3] Iniciamos la instrucción MERGE. 
    # "trg" es un alias (Target) para referirse a los datos que YA están guardados en el disco.
    # "src" es un alias (Source) para referirse al DataFrame 'df' que acaba de llegar con datos nuevos.
    dlt_obj.alias("trg").merge(
        
        # [HASHTAG 4] Esta es la CLAVE PRIMARIA. Le decimos a Spark: "Empareja la fila nueva con la vieja si el 'id' es igual".
        df.alias("src"),
        "src.id = trg.id")\
        
        # [HASHTAG 5] CONDICIÓN DE ACTUALIZACIÓN (Update). 
        # Si el ID ya existe (Matched), SOLO actualiza TODAS las columnas SI la fecha del dato nuevo es mayor o igual a la vieja.
        # ESTO ES CRÍTICO EN TIEMPO REAL: Evita que un dato atrasado (Late Data) sobrescriba un dato más reciente.
        .whenMatchedUpdatedAll(condition="src.updateDate >= trg.updateDate")\
        
        # [HASHTAG 6] CONDICIÓN DE INSERCIÓN (Insert).
        # Si el ID NO existe en la tabla del disco (Not Matched), inserta la fila completamente nueva.
        .whenNotMatchedInsertAll()\
        
        # [HASHTAG 7] .execute() es obligatorio. Hasta aquí solo le hemos dicho a Spark "el plan" de lo que quiere hacer. 
        # Esta línea es la que realmente dispara el trabajo en el clúster y ejecuta el Upsert.
        .execute()
    
    # [HASHTAG 8] Si todo lo de arriba funcionó, imprime esto.
    print("This is upserting now")

# [HASHTAG 9] El bloque 'except' es el "Plan B". Se ejecuta SOLO si la línea [HASHTAG 2] falló.
except:
    
    # [HASHTAG 10] Como la tabla no existía (era la primera vez), simplemente tomamos el DataFrame actual 
    # y lo guardamos desde cero sobreescribiendo la carpeta. Esto "inicializa" o "crea" la tabla Delta.
    df.write.format("delta")\
        .mode("Overwrite")\
        .save("/Volumes/pyspark_catalogo/source/db_volume/products_sink/")


### 🧠 NOTA (Contexto del Scenario)

Si estuvieras haciendo un batch normal, simplemente harías df.write.mode("overwrite") todo el tiempo. Pero en un Escenario de Tiempo Real (Streaming), hacer overwrite cada 5 minutos es un desastre: borras todo y vuelves a escribir, lo cual es lentísimo y destruye el historial.

Éste código:

**1.	Es incremental:** Si llegan 10 productos nuevos, solo escribe 10 filas. No toca las otras 100,000 que ya existen. (Ultra rápido).
   
**2.	Protege contra datos desordenados:** Esa condición de src.updateDate >= trg.updateDate es oro puro. En streaming, a veces llegan eventos del pasado. Esta línea le dice a Delta Lake: "Si me llega un evento del martes, pero ya tengo un evento del miércoles para ese mismo ID, ignóralo". Eso es implementar semántica Idempotente a nivel de base de datos.
   
**3.	Auto-curación:** El try/except asegura que tu pipeline no se rompa el día 1 cuando la tabla aún no ha sido creada.


### •	STREAMING QUERY

Ahora creamos un nuevo escenario llamado scenario_2 , y pasamos el siguiente código:

Schema:

        my_schema = """
                        order_id INT,
                        customer_id INT,
                        order_date DATE,
                        amount DOUBLE
         """

![image](https://github.com/user-attachments/assets/5aef4fd9-f970-430d-8f89-d522e153b96d)

df_batch:

          df_batch = spark.read.format("csv")\
                      .option("header", "true")\
                      .schema(my_schema)\
                      .load("/Volumes/pyspark_catalogo/source/db_volume/Streaming/")
          display(df_batch)


![image](https://github.com/user-attachments/assets/2615b344-9783-4ffd-89ce-69c1f3bc14a4)

### Streaming Output

Código:

        df.writeStream.format("delta")\
                   .option("checkpointLocation", "/Volumes/pyspark_catalogo/source/db_volume/streamSink/checkpoint")\
                   .option("mergeSchema",True)\
                   .trigger(once=True)\
                   .start("/Volumes/pyspark_catalogo/source/db_volume/streamSink/data")

Código:

        df.writeStream.format("delta")\
                    .option("checkpointLocation", "/Volumes/pyspark_catalogo/source/db_volume/streamSink/checkpoint")\
                    .option("mergeSchema",True)\
                    .trigger(once=True)\
                    .start("/Volumes/pyspark_catalogo/source/db_volume/streamSink/data")


![image](https://github.com/user-attachments/assets/2bfc688a-0992-4f52-904e-db5f179d5117)

*Ahora creamos otro escenario y cargamos en Volumes el archivo apidata.json

1. creación del scenario_3

![image](https://github.com/user-attachments/assets/592bff82-39b5-4d84-b6b3-30fc5550ef89)

2. Creamos en Catalog / Volumes la carpeta jsonSource.

![image](https://github.com/user-attachments/assets/416b2018-8392-43eb-b928-6759f745f1d9)

![image](https://github.com/user-attachments/assets/2e7b8e2b-65a5-4049-9dc8-c22bcd7c8dee)

Apidata.json:

              [
                {
                  "order_id": "ORD001",
                  "order_timestamp": "2025-08-15T10:45:30Z",
                  "customer": {
                    "customer_id": "CUST101",
                    "name": "John Doe",
                    "email": "john.doe@example.com",
                    "location": {
                      "city": "Toronto",
                      "country": "Canada"
                    }
                   },
                 "payment": {
                     "method": "Credit Card",
                     "amount": 250.75,
                     "currency": "CAD"
                  },
                  "items": [
                    {
                      "item_id": "ITEM1001",
                      "product_name": "Wireless Mouse",
                      "quantity": 2,
                      "price_per_unit": 25.50
                    },
                    {
                      "item_id": "ITEM1002",
                      "product_name": "Mechanical Keyboard",
                      "quantity": 1,
                      "price_per_unit": 199.75
                    }
                  ],
                  "delivery_updates": [
                    "Order Placed",
                    "Packed",
                    "Shipped",
                    "Out for Delivery"
                  ]
               },
                {
                  "order_id": "ORD002",
                  "order_timestamp": "2025-08-15T11:10:15Z",
                   "customer": {
                    "customer_id": "CUST102",
                    "name": "Jane Smith",
                    "email": "jane.smith@example.com",
                    "location": {
                    "city": "Vancouver",
                    "country": "Canada"
                    }
                  },
                  "payment": {
                    "method": "PayPal",
                    "amount": 89.99,
                    "currency": "CAD"
                  },
                  "items": [
                    {
                      "item_id": "ITEM1003",
                      "product_name": "USB-C Hub",
                      "quantity": 1,
                      "price_per_unit": 89.99
                    }
                  ],
                  "delivery_updates": [
                    "Order Placed",
                     "Packed",
                    "Shipped"
                   ]
                }
              ]


Ahora pasamos el siguiente código a la celda del scenario_3.

Código:

        df = spark.read.format("json")\
              .option("inferSchema", "true")\
              .option("multiline", "true")\
              .load("/Volumes/pyspark_catalogo/source/db_volume/jsonSource/")  

Resultado:

![image](https://github.com/user-attachments/assets/c852b0ea-53c9-48c3-ba6a-70524fefdadb)

Código:

         from pyspark.sql.functions import *

código:

        df_cust = df.select("customer.customer_id", "customer.email", "customer.location.city", "customer.location.country", "*").drop("customer")

       df_cust_upd = df_cust.withColumn("delivery_updates", explode("delivery_updates"))\
            .withColumn("items", explode("items"))\
            .select("*")

        display(df_cust)


![image](https://github.com/user-attachments/assets/973d5f13-bd00-4143-97ba-a60035bbe9a8)

### PYTHON CLASS

•	Ahora creamos un nuevo escenario.

Código:

        from pyspark.sql.functions import *
        from pyspark.sql.window import Window


Código:

        class DataValidation:
    
            def __init__(self, df):
                self.df = df

            def dedup(self, keyCol, cdcCol):
                df = self.df.withColumn("dedup", row_number().over(Window.partitionBy("keyCol").orderBy(desc(cdcCol))))
                df = df.filter(col('dedup') == 1).drop("dedup")
                return df
    
            def removeNulls(self, nullCol):
               df = self.df.filter(col(nullCol).isNotNull())
               return df 

**•	Explicación del código línea por línea:**

La Clase Principal y el Constructor

Código:

        class DataValidation:

        
•	Qué hace: Define el "molde" (la plantilla) de tu objeto de validación.
•	Por qué se usa una clase: En lugar de tener funciones sueltas (def quitar_nulos(), def quitar_duplicados()), agruparlas en una clase te permite mantener el estado del DataFrame a medida que le vas aplicando filtros secuenciales sin tener que reescribir el DataFrame en cada paso.

Código:
    
        def __init__(self, df):
            self.df = df

•	Qué hace: Es el constructor. Cuando creas el objeto (validator = DataValidation(df)), este método toma el DataFrame crudo de entrada y lo guarda internamente dentro de la variable self.df.
•	Por qué es clave: Al guardarlo en self, los métodos siguientes (dedup y removeNulls) pueden actuar sobre los datos limpios del método anterior, encadenando las validaciones sin perder datos en el proceso.

El Método dedup (Deduplicación inteligente basada en tiempo)

Este método resuelve un problema muy específico y peligroso: Las filas duplicadas en bases de datos transaccionales.

Código:
    
        def dedup(self, keyCol, cdcCol):

        
•	Qué hace: Define los dos parámetros que necesita para funcionar.
o	keyCol: La columna que identifica la entidad (ej. order_id).
o	cdcCol: La columna que marca el tiempo de la última actualización (ej. update_timestamp). Nota: "cdc" significa Change Data Capture, la técnica de rastrear cambios.

Código:
        df = self.df.withColumn("dedup", row_number().over(Window.partitionBy("keyCol").orderBy(desc(cdcCol))))

•	Línea por línea:
o	Window.partitionBy("keyCol"): Le dice a Spark: "Agrupa todos los registros que tengan el mismo order_id en la misma partición".
o	.orderBy(desc(cdcCol)): Dentro de esa partición, ordena los registros de forma descendente (del más reciente al más viejo) usando la columna de tiempo.
o	row_number(): Asigna el número 1 a la fila más reciente, el 2 a la siguiente, etc.
o	.withColumn("dedup", ...): Crea una nueva columna temporal llamada dedup con esos números.

Código:
        df = df.filter(col('dedup') == 1).drop("dedup")
        
•	Qué hace: Filtra el DataFrame para solo conservar las filas donde el número asignado sea 1 (las más recientes). Luego borra la columna temporal dedup porque ya no la necesitas.
•	El resultado: Tu DataFrame quedará limpio, sin duplicados, conservando el registro más actualizado de cada entidad.

Código:

        return df

•	Qué hace: Devuelve el DataFrame limpio y sobrescribe self.df (nota: en realidad debería ser self.df = df... return self.df para que el siguiente método use los datos deduplicados, pero asumo que es un error de tu código original. Te lo explico abajo).

El Método removeNulls (Limpieza de datos faltantes)

Código:

        def removeNulls(self, nullCol):

•	Qué hace: Recibe el nombre de una columna específica a revisar.

Código:

        df = self.df.filter(col(nullCol).isNotNull())
        
•	Línea por línea:
o	col(nullCol): Apunta a la columna (ej. col("email")).
o	.isNotNull(): Evalúa cada fila. Devuelve True si la celda tiene un valor, y False si es null.
o	.filter(...): Elimina todas las filas donde la condición sea False (las que tienen nulos).

Código:

        return df
        
•	Qué hace: Devuelve el DataFrame sin las filas que tenían nulos en esa columna.


Código:

        df = spark.createDataFrame([("1", "2020-01-01", 100), ("2", "2020-01-02", 200), ("3", "2020-01-03", 300)], ["order_id", "order_date", "amount"])
        display(df)




![image](https://github.com/user-attachments/assets/cfddd174-533e-4990-8fc6-74a97d561bc7)


•	Ahora creamos un notebook scenario_5 y creamos una tabla con el siguiente código 

Código:

        CREATE TABLE pyspark_catalogo.source.customers
        (
          id STRING,
          email STRING,
          city STRING,
         country STRING
        )


Código:

        INSERT INTO pyspark_catalogo.source.customers
        VALUES ('1', 'john.smith@example.com', 'New York', 'USA'),
               ('2', 'jane.doe@example.com', 'London', 'UK'),
               ('3', 'mike.william@example.com', 'Paris', 'France'),
               ('4', 'sara.johnson@example.com', 'Tokyo', 'Japan'),
               ('5', 'alex.miller@example.com', 'Sydney', 'Australia')



Código:

        SELECT * FROM pyspark_catalogo.source.customers

![image](https://github.com/user-attachments/assets/bd9fdd74-87ec-45d1-997f-9b1d5cec0ba3)

Código:

        if spark.catalog.tableExists("pyspark_catalogo.source.DimCustomers"):

            pass
        else:

            spark.sql("""
                      CREATE TABLE pyspark_catalogo.source.DimCustomers 
                      SELECT *,
                           current_timestamp() as startTime,
                          CAST('3000-01-01' AS TIMESTAMP) as endTime,
                          'Y' as isActive
                      FROM pyspark_catalogo.source.Customers
                    """)


![image](https://github.com/user-attachments/assets/ed1ed0be-6c6f-4abc-8c2d-087f7f4692ae)

![image](https://github.com/user-attachments/assets/9d7bc0d4-2c2e-44fa-bcee-763d11378352)

### SCD TYPE-2

Primero importamos las librerías necesarias.

Código:

        from pyspark.sql.functions import *
        from pyspark.sql.window import Window
        from delta.tables import DeltaTable

segundo, eliminamos la tabla creada anteriormente y creamos una nueva.

Código:

        DROP TABLE pyspark_catalogo.source.customers

Código:

        CREATE TABLE pyspark_catalogo.source.customers
        (
          id STRING,
          email STRING,
          city STRING,
          country STRING,
          modifiedDate TIMESTAMP
          )

Ahora insertamos elementos a la tabla

Código:

        INSERT INTO pyspark_catalogo.source.customers
        VALUES ('1', 'john.smith@example.com', 'New York', 'USA', current_timestamp()),
               ('2', 'jane.doe@example.com', 'London', 'UK', current_timestamp()),
               ('3', 'mike.william@example.com', 'Paris', 'France', current_timestamp()),
               ('4', 'sara.johnson@example.com', 'Tokyo', 'Japan', current_timestamp()),
               ('5', 'alex.miller@example.com', 'Sydney', 'Australia', current_timestamp())

Luego observamos la carga de datos con tiempo actual.

Código:

        SELECT * FROM pyspark_catalogo.source.customers


![image](https://github.com/user-attachments/assets/08ae3864-f64c-4530-aa85-9e7896355b86)

Ahora dejamos caer la tabla anterior.

Código:

        DROP TABLE pyspark_catalogo.source.dimcustomers

Código:

        if spark.catalog.tableExists("pyspark_catalogo.source.DimCustomers"):

            pass
        else:

            spark.sql("""
                      CREATE TABLE pyspark_catalogo.source.DimCustomers 
                      SELECT *,
                          current_timestamp() as startTime,
                          CAST('3000-01-01' AS TIMESTAMP) as endTime,
                          'Y' as isActive
                      FROM pyspark_catalogo.source.customers
                    """)

Ahora comprobamos la modificación de la tabla.

Código:

       SELECT * FROM pyspark_catalogo.source.Dimcustomers


![image](https://github.com/user-attachments/assets/0b58bf10-229e-42e7-9ff7-acec8d6c935c)

Código:

        df = spark.sql("""
                       SELECT *
                       FROM pyspark_catalogo.source.customers
                     """)
        df = df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))

        df.createOrReplaceTempView("src")


### Marking the updated records as expired

Código:

        %sql
        MERGE INTO pyspark_catalogo.source.DimCustomers as trg
        USING src as src
        ON trg.id = src.id
        AND trg.isActive = 'Y'

        WHEN MATCHED AND src.email <> trg.email
        OR src.city <> trg.city
        OR src.country <> trg.country
        OR src.modifiedDate <> trg.modifiedDate

        THEN UPDATE SET 
        trg.endTime = current_timestamp(),
        trg.isActive = 'N'


### MERGE-2 INSERTING NEW + UPDATE RECORDS

Código:

        df = spark.sql("""
                      SELECT *
                      FROM pyspark_catalogo.source.customers
                    """)
        df = df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))

        df.createOrReplaceTempView("srctemp")

        df = spark.sql("""
                      SELECT *,
                          current_timestamp() as startTime,
                          CAST('3000-01-01' AS TIMESTAMP) as endTime,
                          'Y' as isActive
                      FROM srctemp
                    """)

        df.createOrReplaceTempView("src")



código:

        SELECT * FROM src


![image](https://github.com/user-attachments/assets/51057cee-c865-49d4-91d1-0176a67c704e)

Ahora modificamos el código anterior.

Código:

        df = spark.sql("""
                      SELECT *
                      FROM pyspark_catalogo.source.customers
                    """)
        df = df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))\
                            .drop('dedup')

        df = df.filter(col('dedup') == 1)

        df.createOrReplaceTempView("srctemp")

        df = spark.sql("""
                      SELECT *,
                          current_timestamp() as startTime,
                          CAST('3000-01-01' AS TIMESTAMP) as endTime,
                          'Y' as isActive
                      FROM srctemp
                    """)

        df.createOrReplaceTempView("src")

y corremos el siguiente código.

Código:

        SELECT * FROM src


![image](https://github.com/user-attachments/assets/76133a71-01e5-4f08-8b7b-b18c6c008388)

Código:

        %sql
        MERGE INTO pyspark_catalogo.source.DimCustomers as trg
        USING src as src
        ON trg.id = src.id
        AND trg.isActive = 'Y'

        WHEN MATCHED AND src.email <> trg.email
        OR src.city <> trg.city
        OR src.country <> trg.country
        OR src.modifiedDate <> trg.modifiedDate

        THEN UPDATE SET 
        trg.endTime = current_timestamp(),
        trg.isActive = 'N'



### MERGE-2 INSERTING NEW + UPDATE RECORDS

Código:

        MERGE INTO pyspark_catalogo.source.DimCustomers as trg

        USING src as src
        ON src.id = trg.id
        AND trg.isActive = 'Y'

        WHEN NOT MATCHED THEN INSERT * 


**• DLTNew**

Código:

        import dlt
        from pyspark.sql.functions import *
        from pyspark.sql.types import *
        from pyspark.sql.window import Window

        @dlt.table(
          name = "customers_raw"
        )
        def test():
            df = spark.readStream.table("pyspark_catalogo.source.customers")
            return df

        @dlt.table(
          name= "customers_enr"
        )
         def customers_enr():
            df = spark.readStream.table("customers_raw")
            df = df.withColumn("dedup", row_number().over(Window.partitionBy("id").orderBy(desc("modifiedDate"))))
            return df.where(col("dedup") == 1).drop("dedup")

        dlt.create_streaming_table(
            name = "customers_dim"
         )

        dlt.create_auto_cdc_flow(
            target = "customers_dim",
            source = "customers_enr",
             keys = ["id"],
            sequence_by = "modifiedDate",
            stored_as_scd_type = 2,
        )


![image](https://github.com/user-attachments/assets/71011a05-875e-4727-87b6-f52a6e6583a8)

![image](https://github.com/user-attachments/assets/aa9e1dfd-02e2-4414-a41c-8ad9068b4023)

