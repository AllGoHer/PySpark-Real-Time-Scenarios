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
La arquitectura aplica el patrón Medallion sobre un flujo de streaming continuo:

🔧 BRONZE (Raw)└── Estructura Original (JSON Anidado Complejo)    
├── order_id: "ORD001"    ├── customer: { location: { city: "Toronto", country: "Canada" }    
└── items: [ {item_id: "ITEM1001"}, {item_id: "ITEM1002"} ]            │            
▼ (Flattening / Aplanado con explode())│📋 SILVER (Staging Data)
├── order_id: "ORD001"├── city: "Toronto"├── country: "Canada"├── item_id: "ITEM1001"├── product_name: "Wireless Mouse"└── quantity: 2            │ 

▼ (Incremental Load / MERGE INTO)  │❄️ GOLD (Data Warehouse - Delta Table)
├── order_id: "ORD001" (Clave Primaria)├── city: "Toronto"├── country: "Canada"└── amount: 250.75


┌─────────────────────────────────────────────────────────────┐
│  📁 BRONZE: JSON Anidado crudo (Tal como llega de la API)  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  {                                                          │
│    "order_id": "ORD001",                                    │
│    "customer": {                                            │
│      "customer_id": "CUST101",                              │
│      "location": {                                          │
│        "city": "Toronto",                                   │
│        "country": "Canada"                                  │
│      }                                                      │
│    },                                                       │
│    "items": [                                               │
│      { "item_id": "ITEM1001", "product_name": "Mouse" },    │
│      { "item_id": "ITEM1002", "product_name": "Keyboard" }  │
│    ],                                                       │
│    "delivery_updates": ["Placed", "Packed", "Shipped"]      │
│  }                                                          │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
                             ▼
(Transformación: `explode()` de Arrays y Extracción de Structs)
                             │
┌──────────────────────────────────────────────────────────────────┐
│  📊 SILVER: Tabla Relacional Aplanada (Listo para el Warehouse)  │
├──────────────────────────────────────────────────────────────────┤
│                                                             │
│  order_id │ customer_id │ city     │ country │ item_id  │ product_name  │ delivery_updates  │
│ ─────────┼───────────┼──────────┼─────────┼────────┼──────────┼───────────────┼─────────────────┤
│  ORD001  │ CUST101    │ Toronto  │ Canada  │ ITEM1001  │ Wireless Mouse  │ Order Placed   │
│  ORD001  │ CUST101    │ Toronto  │ Canada  │ ITEM1002  │ Mech. Keyboard │ Packed         │
│  ORD001  │ CUST101    │ Toronto  │ Canada  │ ITEM1001  │ Wireless Mouse  │ Shipped        │
│  ORD002  │ CUST102    │ Vancouver│ Canada  │ ITEM1003  │ USB-C Hub      │ Order Placed   │
│  ORD002  │ CUST102    │ Vancouver│ Canada  │ ITEM1003  │ USB-C Hub      │ Packed         │
└─────────────────────────────────────────────────────────────┘

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

run_id
order_id
order_date
amount
¿Qué pasó en el Data Warehouse?
1 (Inicial)	1	2025-08-02	246.84	Se insertó por primera vez.
2, 3, 4	1	2025-08-05	246.84	No cambió. El evento fue recibido, pero el monto era igual. Se ignora el UPSERT.
6 (Actualización)	1	2026-08-11	248.69	Se ACTUALIZÓ el monto de $246.84 a $248.69 sin tocar el historial.

| run_id | order_id | order_date | amount | ¿Qué pasó en el Data Warehouse? |
|:---:|:---|:---|:---|:---|
| 1 (Inicial) | 1 | 2025-08-02 | 246.84 | Se insertó por primera vez. |
| 2, 3, 4 | 1 | 2025-08-05 | 246.84 | No cambió. El evento fue recibido, pero el monto era igual. Se ignora el UPSERT.
| **6 (Actualización)** | 1 | 2026-08-11 | **248.69** | **Se ACTUALIZÓ el monto de $246.84 a $248.69 sin tocar el historial.**


![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
