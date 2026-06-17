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

graph TD
    subgraph Bronze ["🟫 BRONZE (Raw Nested JSON)"]
        A["{ order_id: 'ORD001', customer: { location: { city: 'Toronto' }, items: [ {...}, {...} ] }"]
    end

    subgraph Silver ["🔵 SILVER (Flat Relational Table)"]
        B["order_id: ORD001 | CUST101 | Toronto | Canada | ITEM1001 | Wireless Mouse | Order Placed"]
        C["order_id: ORD001 | CUST101 | Toronto | Canada | ITEM1002 | Mech. Keyboard | Packed"]
        D["order_id: ORD001 | CUST101 | Toronto | Canada | ITEM1001 | Wireless Mouse | Shipped"]
    end

    A -->|Flattening & Explode Arrays| B
    B --> C
    C --> D
```

---

### ¿Por qué este diagrama le gusta a los reclutadores?

1. **Muestra el problema visualmente:** Al poner el JSON anidado arriba y la tabla limpia abajo, el reclutador ve de un vistazo cuál es el problema que estás resolviendo (datos altamente desordenados vs. datos ordenados).
2. **Muestra la "Row Explosion":** El hecho de que `ORD001` aparezca 3 veces en la tabla de abajo demuestra que entendes cómo funciona `explode()` internamente y cómo se maneja la cardinalidad de los Arrays.
3. **Muestra Extracción Profunda:** Al listar "city" y "country" separados en la tabla de abajo, demuestras que sabes navegar por Structs anidados (2 niveles de profundidad: `customer` $\rightarrow$ `location` $\rightarrow$ `city`), lo cual es un dolor de cabeza en Spark que solo los Intermedios/Seniors dominan.
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
