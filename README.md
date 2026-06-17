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

🔧 BRONZE (Raw)
└── Estructura Original (JSON Anidado Complejo)    ├── order_id: "ORD001"    ├── customer: { location: { city: "Toronto", country: "Canada" }    └── items: [ {item_id: "ITEM1001"}, {item_id: "ITEM1002"} ]            │            
▼ (Flattening / Aplanado con explode())            │📋 SILVER (Staging Data)├── order_id: "ORD001"├── city: "Toronto"├── country: "Canada"├── item_id: "ITEM1001"├── product_name: "Wireless Mouse"└── quantity: 2            │ 

▼ (Incremental Load / MERGE INTO)            │❄️ GOLD (Data Warehouse - Delta Table)├── order_id: "ORD001" (Clave Primaria)├── city: "Toronto"├── country: "Canada"└── amount: 250.75
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
