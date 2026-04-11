# Diccionario de datos del repositorio

Este documento describe el **stack de información** del proyecto: contexto histórico y económico (Our World in Data), intensidad de carbono en tiempo real (Electricity Maps), tablas maestras de hardware e impacto regional (MLCO2), **precios medios de electricidad por país**, **precio horario On-Demand de instancias EC2 (proxy TCO cómputo)**, **exportaciones de servicios TIC** (Banco Mundial / WDI), y sesiones de uso de IA generadas de forma sintética. Las rutas de archivo indican copias locales en el repositorio cuando aplica.

## Fuentes de datos (el stack de información)

| # | Fuente | Descripción | Acceso | Justificación de elección |
|---|--------|-------------|--------|---------------------------|
| 1 | **Our World in Data (Energy)** | Datos históricos por país: PIB, población y mezcla energética (renovables vs. fósiles). | [Dataset en GitHub (OWID)](https://github.com/owid/energy-data) | Contexto económico y base comparativa histórica para países de América. |
| 2 | **Electricity Maps API** | Intensidad de carbono (gCO₂eq/kWh) en tiempo real por zona geográfica; mix y catálogo de zonas. | [Electricity Maps](https://www.electricitymaps.com/) | Componente en tiempo real que justifica ingesta automatizada (p. ej. con Airflow). |
| 3 | **MLCO2 — hardware e impacto** | Especificaciones TDP de GPUs/CPUs y factores de emisión por región de cómputo (proyecto MLCO2 / *impact*). | [MLCO2 / impact (GitHub)](https://github.com/mlco2/impact) | Tabla maestra para pasar de uso de hardware a consumo energético y emisiones. |
| 4 | **Generador de logs (sintético)** | Registros de sesiones de uso de IA (usuario, GPU, región, horas, etc.). | Desarrollo propio | Control de volumen (p. ej. 100k+ filas) y simulación de movilidad entre regiones de cómputo. |
| 5 | **Precios de electricidad por país** | Tarifas medias residenciales y de negocio en USD/kWh (promedio 2023-2026) por país. | [GlobalPetrolPrices.com — Electricity prices](https://www.globalpetrolprices.com/electricity_prices/); copia local: `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` | Complementa el contexto energético con el **costo** de la electricidad a nivel país (distinto de intensidad de carbono o mix). |
| 6 | **Banco Mundial — WDI** | Exportaciones de servicios de tecnologías de la información y comunicación (TIC), balanza de pagos, US$ corrientes. | [Indicador BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD); archivos en `World_Bank_Group/` | Contexto macroeconómico del sector digital/TIC por economía y en el tiempo (series anuales). |
| 7 | **Dimensión geográfica de referencia** | Mapeo explícito región cloud AWS (y metadatos MLCO2) ↔ país ↔ ISO ↔ zona Electricity Maps ↔ nombre país para precios. | `bronze/reference/geo_cloud_to_country_and_zones.csv` (también `reference/` en bucket Bronze) | Evita joins frágiles entre logs (`region`), API EM (`zone`), OWID/BM (`iso_code` / `Country Code`) y Global Petrol (`Country`). |
| 8 | **Precios EC2 On-Demand (proxy TCO)** | Tarifa horaria USD por tipo de instancia y región AWS (Linux, tenancy compartida), además del modelo eléctrico kWh × USD/kWh. | `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv`; regenerar con `scripts/build_aws_ec2_pricing_reference.py` | Permite estimar **coste de cómputo** ≈ `duration_hours × price_usd_per_hour` en Gold; no sustituye facturación real (Spot, RI, EBS, egress). |

---
## 1. Our World in Data (Energy)

Copia local del dataset de energía de [Our World in Data](https://github.com/owid/energy-data): `data/Our_World_In_Data/owid-energy-data.csv`.

### 1.1 `data/Our_World_In_Data/owid-energy-data.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 23232 |
| Columnas | 130 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Panel anual: cada fila es una observación para una **entidad geográfica** (país o agregado regional publicado por OWID) en un **año** concreto. Los campos numéricos pueden ir vacíos cuando no hay dato en la fuente subyacente. Las 130 columnas del fichero se detallan en la tabla siguiente (nombre de campo, tipo inferido para uso en pipelines y descripción del indicador).

### Columnas

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `country` | texto | País. Ubicación geográfica. |
| `year` | entero | Año. Año de observación. |
| `iso_code` | texto | Código ISO. Códigos de países de tres letras ISO 3166-1 alfa-3. |
| `population` | numérico | Población. Población por país, disponible desde 10.000 a. C. hasta 2100, basada en datos y estimaciones de diferentes fuentes. Unidad: personas. |
| `gdp` | numérico | Producto interno bruto (PIB). Producción económica total de un país o región por año. Estos datos están ajustados por inflación y diferencias en el costo de vida entre países. Unidad: internacional-$ en precios de 2011 ($). |
| `biofuel_cons_change_pct` | numérico | Variación porcentual anual del consumo de biocombustibles. Incluye biogasolina (como etanol) y biodiesel. Los volúmenes se han ajustado según el contenido energético. Unidad: %. |
| `biofuel_cons_change_twh` | numérico | Variación anual del consumo de biocombustibles. Incluye biogasolina (como etanol) y biodiesel. Los volúmenes se han ajustado según el contenido energético. Unidad: teravatios-hora (TWh). |
| `biofuel_cons_per_capita` | numérico | Consumo de biocombustibles per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `biofuel_consumption` | numérico | Consumo de energía primaria procedente de biocombustibles. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `biofuel_elec_per_capita` | numérico | Generación de electricidad a partir de bioenergía por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `biofuel_electricity` | numérico | Generación de electricidad a partir de bioenergía. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `biofuel_share_elec` | numérico | Proporción de electricidad generada por bioenergía. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `biofuel_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de biocombustibles. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `carbon_intensity_elec` | numérico | Intensidad de carbono del ciclo de vida de la generación de electricidad. Gases de efecto invernadero emitidos por unidad de electricidad generada, medidos en gramos de equivalentes de CO₂ por kilovatio-hora. Unidad: gramos de CO₂ equivalentes por kilovatio-hora (gCO₂). |
| `coal_cons_change_pct` | numérico | Variación porcentual anual del consumo de carbón. Unidad: %. |
| `coal_cons_change_twh` | numérico | Variación anual del consumo de carbón. Unidad: teravatios-hora (TWh). |
| `coal_cons_per_capita` | numérico | Consumo de carbón per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `coal_consumption` | numérico | Consumo de energía primaria procedente del carbón. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `coal_elec_per_capita` | numérico | Generación de electricidad a partir de carbón por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `coal_electricity` | numérico | Generación de electricidad a partir del carbón. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `coal_prod_change_pct` | numérico | Variación anual de la producción de carbón. Medido como porcentaje de la producción del año anterior. Unidad: %. |
| `coal_prod_change_twh` | numérico | Variación anual de la producción de carbón. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `coal_prod_per_capita` | numérico | Producción de carbón per cápita. Medido en kilovatios-hora per cápita. Unidad: kilovatios-hora (kWh). |
| `coal_production` | numérico | Producción de carbón. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `coal_share_elec` | numérico | Proporción de electricidad generada por carbón. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `coal_share_energy` | numérico | Participación del consumo de energía primaria que proviene del carbón. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `electricity_demand` | numérico | Demanda de electricidad. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `electricity_demand_per_capita` | numérico | Demanda eléctrica total por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `electricity_generation` | numérico | Generación eléctrica total. Electricidad total generada en cada país o región, medida en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `electricity_share_energy` | numérico | Generación total de electricidad como proporción de la energía primaria. Medido como porcentaje del consumo total directo de energía primaria en el país o región. Unidad: %. |
| `energy_cons_change_pct` | numérico | Variación anual del consumo de energía primaria. Unidad: %. |
| `energy_cons_change_twh` | numérico | Variación anual del consumo de energía primaria. Unidad: teravatios-hora (TWh). |
| `energy_per_capita` | numérico | Consumo de energía primaria per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora por persona (kWh). |
| `energy_per_gdp` | numérico | Consumo de energía primaria por PIB. Medido en kilovatios-hora por dólar internacional. Unidad: kilovatios-hora por $ (kWh). |
| `fossil_cons_change_pct` | numérico | Variación porcentual anual del consumo de combustibles fósiles. Unidad: %. |
| `fossil_cons_change_twh` | numérico | Variación anual del consumo de combustibles fósiles. Unidad: teravatios-hora (TWh). |
| `fossil_elec_per_capita` | numérico | Generación de electricidad a partir de combustibles fósiles por persona. Generación de electricidad a partir de carbón, petróleo y gas, medida en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `fossil_electricity` | numérico | Generación de electricidad a partir de combustibles fósiles. Generación de electricidad a partir de carbón, petróleo y gas, medida en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `fossil_energy_per_capita` | numérico | Consumo de combustibles fósiles per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `fossil_fuel_consumption` | numérico | Consumo de energía primaria procedente de combustibles fósiles. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `fossil_share_elec` | numérico | Proporción de electricidad generada por combustibles fósiles. Generación de electricidad a partir de carbón, petróleo y gas, medida como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `fossil_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de combustibles fósiles. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `gas_cons_change_pct` | numérico | Variación porcentual anual del consumo de gas. Unidad: %. |
| `gas_cons_change_twh` | numérico | Variación anual del consumo de gas. Unidad: teravatios-hora (TWh). |
| `gas_consumption` | numérico | Consumo de energía primaria procedente del gas. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `gas_elec_per_capita` | numérico | Generación de electricidad a partir de gas por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `gas_electricity` | numérico | Generación de electricidad a partir de gas. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `gas_energy_per_capita` | numérico | Consumo de gas per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `gas_prod_change_pct` | numérico | Variación anual de la producción de gas. Medido como porcentaje de la producción del año anterior. Unidad: %. |
| `gas_prod_change_twh` | numérico | Variación anual de la producción de gas. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `gas_prod_per_capita` | numérico | Producción de gas per cápita. Medido en kilovatios-hora per cápita. Unidad: kilovatios-hora (kWh). |
| `gas_production` | numérico | Producción de gas. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `gas_share_elec` | numérico | Proporción de electricidad generada por gas. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `gas_share_energy` | numérico | Participación del consumo de energía primaria que proviene del gas. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `greenhouse_gas_emissions` | numérico | Emisiones del ciclo de vida procedentes de la generación de electricidad. Medido en megatoneladas de equivalentes de CO₂. Unidad: millones de toneladas equivalentes de CO₂ (Mt). |
| `hydro_cons_change_pct` | numérico | Cambio porcentual anual en el consumo de energía hidroeléctrica. Las cifras se basan en la generación hidroeléctrica primaria bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `hydro_cons_change_twh` | numérico | Cambio anual en el consumo de energía hidroeléctrica. La energía equivalente de entrada se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `hydro_consumption` | numérico | Consumo de energía primaria proveniente de la energía hidroeléctrica. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `hydro_elec_per_capita` | numérico | Generación de electricidad a partir de energía hidroeléctrica por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `hydro_electricity` | numérico | Generación de electricidad a partir de energía hidroeléctrica. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `hydro_energy_per_capita` | numérico | Consumo de energía hidroeléctrica per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `hydro_share_elec` | numérico | Proporción de electricidad generada por energía hidroeléctrica. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `hydro_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de la energía hidroeléctrica. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `low_carbon_cons_change_pct` | numérico | Variación porcentual anual del consumo de energía baja en carbono. Las cifras se basan en la generación bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `low_carbon_cons_change_twh` | numérico | Cambio anual en el consumo de energía baja en carbono. La energía equivalente de entrada se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `low_carbon_consumption` | numérico | Consumo de energía primaria procedente de fuentes bajas en carbono. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `low_carbon_elec_per_capita` | numérico | Generación de electricidad a partir de fuentes bajas en carbono por persona. Medido en kilovatios-hora por persona. Las fuentes bajas en carbono corresponden a las energías renovables y la energía nuclear, que producen significativamente menos emisiones de gases de efecto invernadero que los combustibles fósiles. Las energías renovables incluyen la solar, la eólica, la hidroeléctrica, la bioenergía, la geotérmica, la undimotriz y la mareomotriz. Unidad: kilovatios-hora (kWh). |
| `low_carbon_electricity` | numérico | Generación de electricidad a partir de fuentes bajas en carbono. Medido en teravatios-hora. Las fuentes bajas en carbono corresponden a las energías renovables y la energía nuclear, que producen significativamente menos emisiones de gases de efecto invernadero que los combustibles fósiles. Las energías renovables incluyen la solar, la eólica, la hidroeléctrica, la bioenergía, la geotérmica, la undimotriz y la mareomotriz. Unidad: teravatios-hora (TWh). |
| `low_carbon_energy_per_capita` | numérico | Consumo de energía baja en carbono per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `low_carbon_share_elec` | numérico | Proporción de electricidad generada por fuentes bajas en carbono. Medido como porcentaje de la electricidad total producida en el país o región. Las fuentes bajas en carbono corresponden a las energías renovables y la energía nuclear, que producen significativamente menos emisiones de gases de efecto invernadero que los combustibles fósiles. Las energías renovables incluyen la solar, la eólica, la hidroeléctrica, la bioenergía, la geotérmica, la undimotriz y la mareomotriz. Unidad: %. |
| `low_carbon_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de fuentes bajas en carbono. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `net_elec_imports` | numérico | Importaciones netas de electricidad. Importaciones de electricidad menos exportaciones, medidas en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `net_elec_imports_share_demand` | numérico | Importaciones netas de electricidad como porcentaje de la demanda. Importaciones de electricidad menos exportaciones, medidas como porcentaje de la demanda total de electricidad en el país o región. Unidad: %. |
| `nuclear_cons_change_pct` | numérico | Variación porcentual anual del consumo de energía nuclear. Las cifras se basan en la generación bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `nuclear_cons_change_twh` | numérico | Variación anual del consumo de energía nuclear. La energía equivalente de entrada se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `nuclear_consumption` | numérico | Consumo de energía primaria procedente de la energía nuclear. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `nuclear_elec_per_capita` | numérico | Generación de electricidad a partir de energía nuclear por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `nuclear_electricity` | numérico | Generación de electricidad a partir de energía nuclear. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `nuclear_energy_per_capita` | numérico | Consumo de energía nuclear per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `nuclear_share_elec` | numérico | Proporción de electricidad generada por energía nuclear. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `nuclear_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de la energía nuclear. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `oil_cons_change_pct` | numérico | Variación porcentual anual del consumo de petróleo. Unidad: %. |
| `oil_cons_change_twh` | numérico | Variación anual del consumo de petróleo. Unidad: teravatios-hora (TWh). |
| `oil_consumption` | numérico | Consumo de energía primaria procedente del petróleo. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `oil_elec_per_capita` | numérico | Generación de electricidad a partir de petróleo por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `oil_electricity` | numérico | Generación de electricidad a partir de petróleo. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `oil_energy_per_capita` | numérico | Consumo de petróleo per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `oil_prod_change_pct` | numérico | Variación anual de la producción de petróleo. Medido como porcentaje de la producción del año anterior. Unidad: %. |
| `oil_prod_change_twh` | numérico | Variación anual de la producción de petróleo. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `oil_prod_per_capita` | numérico | Producción de petróleo per cápita. Medido en kilovatios-hora per cápita. Unidad: kilovatios-hora (kWh). |
| `oil_production` | numérico | Producción de petróleo. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `oil_share_elec` | numérico | Proporción de electricidad generada por petróleo. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `oil_share_energy` | numérico | Proporción del consumo de energía primaria que proviene del petróleo. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `other_renewable_consumption` | numérico | Consumo de energía primaria procedente de otras renovables. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `other_renewable_electricity` | numérico | Generación de electricidad a partir de otras energías renovables, incluida la bioenergía. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `other_renewable_exc_biofuel_electricity` | numérico | Generación de electricidad a partir de otras energías renovables, excluida la bioenergía. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `other_renewables_cons_change_pct` | numérico | Variación porcentual anual del consumo de otras energías renovables. Las cifras se basan en la generación bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `other_renewables_cons_change_twh` | numérico | Variación anual del consumo de otras renovables. La energía equivalente de entrada, en teravatios-hora, se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `other_renewables_elec_per_capita` | numérico | Generación de electricidad a partir de otras energías renovables, incluida la bioenergía, por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `other_renewables_elec_per_capita_exc_biofuel` | numérico | Generación de electricidad a partir de otras energías renovables, excluida la bioenergía, por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `other_renewables_energy_per_capita` | numérico | Otros consumos de energías renovables per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `other_renewables_share_elec` | numérico | Proporción de electricidad generada por otras energías renovables, incluida la bioenergía. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `other_renewables_share_elec_exc_biofuel` | numérico | Proporción de electricidad generada por otras energías renovables, excluida la bioenergía. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `other_renewables_share_energy` | numérico | Participación del consumo de energía primaria que proviene de otras energías renovables. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `per_capita_electricity` | numérico | Generación eléctrica total por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `primary_energy_consumption` | numérico | Consumo de energía primaria. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `renewables_cons_change_pct` | numérico | Variación porcentual anual del consumo de renovables. Unidad: %. |
| `renewables_cons_change_twh` | numérico | Variación anual del consumo de energías renovables. Unidad: teravatios-hora (TWh). |
| `renewables_consumption` | numérico | Consumo de energía primaria procedente de renovables. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `renewables_elec_per_capita` | numérico | Generación de electricidad a partir de energías renovables por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `renewables_electricity` | numérico | Generación de electricidad a partir de energías renovables. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `renewables_energy_per_capita` | numérico | Consumo de energías renovables per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `renewables_share_elec` | numérico | Proporción de electricidad generada por energías renovables. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `renewables_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de energías renovables. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `solar_cons_change_pct` | numérico | Variación porcentual anual en el consumo de energía solar. Las cifras se basan en la generación bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `solar_cons_change_twh` | numérico | Variación anual del consumo de energía solar. La energía equivalente de entrada, en teravatios-hora, se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `solar_consumption` | numérico | Consumo de energía primaria procedente de energía solar. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `solar_elec_per_capita` | numérico | Generación de electricidad a partir de energía solar por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `solar_electricity` | numérico | Generación de electricidad a partir de energía solar. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `solar_energy_per_capita` | numérico | Consumo de energía solar per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `solar_share_elec` | numérico | Proporción de electricidad generada por energía solar. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `solar_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de la energía solar. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |
| `wind_cons_change_pct` | numérico | Variación porcentual anual del consumo de energía eólica. Las cifras se basan en la generación bruta y no tienen en cuenta el suministro de electricidad transfronterizo. Unidad: %. |
| `wind_cons_change_twh` | numérico | Variación anual del consumo de energía eólica. La energía equivalente de entrada, en teravatios-hora, se basa en la generación bruta y no tiene en cuenta el suministro de electricidad transfronterizo. Unidad: teravatios-hora (TWh). |
| `wind_consumption` | numérico | Consumo de energía primaria procedente de la energía eólica. Medido en teravatios-hora, mediante el método de sustitución. Unidad: teravatios-hora (TWh). |
| `wind_elec_per_capita` | numérico | Generación de electricidad a partir de energía eólica por persona. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `wind_electricity` | numérico | Generación de electricidad a partir de energía eólica. Medido en teravatios-hora. Unidad: teravatios-hora (TWh). |
| `wind_energy_per_capita` | numérico | Consumo de energía eólica per cápita. Medido en kilovatios-hora por persona. Unidad: kilovatios-hora (kWh). |
| `wind_share_elec` | numérico | Proporción de electricidad generada por energía eólica. Medido como porcentaje de la electricidad total producida en el país o región. Unidad: %. |
| `wind_share_energy` | numérico | Proporción del consumo de energía primaria que proviene de la energía eólica. Medida como porcentaje de la energía primaria total, mediante el método de sustitución. Unidad: %. |

---

## 2. Electricity Maps API

Especificación de la **capa Silver** para respuestas JSON: intensidad de carbono (Latest / Past / History), mix eléctrico (Latest) y catálogo de zonas. Los endpoints de intensidad comparten la misma estructura base; en History se recibe una lista de objetos con ese esquema.

### 2.1 Intensidad de carbono (Latest / Past / History)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zone` | String | Código identificador de la zona eléctrica. | N/A | `"DE"` (Alemania) |
| `carbonIntensity` | Integer | Métrica clave: emisiones por unidad de energía. | gCO₂eq/kWh | `302` |
| `datetime` | Timestamp | Fecha y hora de la medición (ISO 8601). | UTC | `2026-04-06T21:00Z` |
| `emissionFactorType` | String | Metodología de cálculo (ciclo de vida o directo). | N/A | `"lifecycle"` |
| `isEstimated` | Boolean | Indica si el dato es real o una estimación. | N/A | `false` |
| `temporalGranularity` | String | Frecuencia del dato. | N/A | `"hourly"` |

Para **History**, en Spark conviene usar `explode()` sobre la lista de registros antes de materializar filas en almacenamiento.

### 2.2 Mix eléctrico (Latest)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zone` | String | Identificador de la zona geográfica. | N/A | `"FR"` (Francia) |
| `datetime` | Timestamp | Momento de la observación del mix. | UTC | `2026-03-18T12:00Z` |
| `nuclear` | Float | Energía generada por fuentes nucleares. | MW | `38906.35` |
| `solar` | Float | Energía generada por radiación solar. | MW | `13062.17` |
| `wind` | Float | Energía generada por fuerza eólica. | MW | `6866.17` |
| `coal` | Float | Energía generada por combustión de carbón. | MW | `0` |
| `gas` | Float | Energía generada por gas natural. | MW | `1486.62` |

### 2.3 Catálogo de zonas (helpers)

| Campo | Tipo | Descripción | Unidad | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `zoneKey` | String | Código único de la zona (ID para joins). | N/A | `"US-CAL-CISO"` |
| `zoneName` | String | Nombre del operador de red o región. | N/A | `"CAISO"` |
| `countryName` | String | País al que pertenece la zona. | N/A | `"USA"` |
| `countryCode` | String | Código de país estándar (2 letras). | N/A | `"US"` |

Nota: en la respuesta de **Zones**, la API puede devolver un objeto cuya llave es el nombre de la zona (p. ej. `"US-CAL-BANC": { ... }`). Conviene aplanar ese mapa a tabla con columna de zona explícita para cruces posteriores.

### 2.4 Rol en el lakehouse (resumen)

| Capa | Contenido |
| :--- | :--- |
| Bronze | JSON crudo tal cual llega de la API (anidado). |
| Silver | JSON aplanado según los diccionarios anteriores (p. ej. `mix.nuclear` → columna `nuclear`). |
| Gold | Cruce de intensidad de carbono con logs de uso de IA usando `zone` como clave. |

---

## 3. MLCO2 — tablas de hardware e impacto

Copias locales usadas como **tabla maestra**: instancias cloud, factores por región, especificaciones de chips y promedios anuales por zona. Proyecto MLCO2 (*impact*): ver enlace en la tabla de fuentes.

### 3.1 `MLCO2/instances.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 14 |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |

### Descripción

Relación de tipos de instancia de proveedores cloud (AWS) con modelo de GPU asociado y enlace a la fuente del fabricante o del proveedor.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `provider` | texto | Código corto del proveedor en minúsculas (p. ej. `aws`). |
| `id` | texto | Identificador del tipo de instancia (p. ej. `p3.2xlarge`). |
| `gpu` | texto | Modelo de GPU instalada en esa instancia. |
| `source` | texto (URL) | Enlace a la documentación del tipo de instancia. |

---


### 3.2 `MLCO2/impact.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 82 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Intensidad de carbono del suministro eléctrico por región de cómputo y proveedor (datos usados en la calculadora MLCO2). Fuentes: eGRID, Electricity Map, literatura, Carbon Footprint, etc. (véase `MLCO2/Readme.md`).

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `provider` | texto | Código corto en minúsculas del proveedor (obligatorio). |
| `providerName` | texto | Nombre completo del proveedor (obligatorio). |
| `offsetRatio` | entero | Porcentaje de emisiones de carbono compensadas por el proveedor en esa región (0–100) (obligatorio). |
| `region` | texto | Código corto en minúsculas de la región (obligatorio). |
| `regionName` | texto | Nombre legible de la región; por defecto igual a `name` si falta (obligatorio). |
| `country` | texto | País (puede estar vacío). |
| `state` | texto | Estado/provincia (puede estar vacío). |
| `city` | texto | Ciudad o área del datacenter (puede estar vacío). |
| `impact` | numérico | Gramos de CO₂ equivalente por kWh (gCO₂eq/kWh) (obligatorio). |
| `source` | texto | Referencia o URL de la fuente del factor de emisión. |
| `PUE` | numérico | Power Usage Effectiveness del datacenter cuando aplica (puede estar vacío). |
| `PUE source` | texto | Enlace o referencia para el valor PUE. |
| `comment` | texto | Notas adicionales (p. ej. datacenter específico, promedios por periodo). |

---


### 3.3 `MLCO2/gpus.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 48 |
| Formato | CSV, separador coma |
| Nota | La cabecera incluye un campo con espacio inicial: ` TFLOPS16` |

### Descripción

Especificaciones de rendimiento y consumo de GPUs y CPUs para estimación energética. Incluye modelos de consumo (TDP), TFLOPS, eficiencia, memoria y fuente.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `name` | texto | Nombre comercial del chip (GPU o CPU). |
| `type` | texto | Categoría: `gpu` o `cpu`. |
| `tdp_watts` | numérico | TDP o potencia de referencia en vatios. |
| `TFLOPS32` | numérico | Rendimiento en TFLOPS en precisión FP32 (puede ser NaN). |
| ` TFLOPS16` | numérico | Rendimiento en TFLOPS en FP16 (nombre de columna con espacio inicial; puede ser NaN). |
| `GFLOPS32/W` | numérico | Eficiencia GFLOPS/W en FP32. |
| `GFLOPS16/W` | numérico | Eficiencia GFLOPS/W en FP16. |
| `memory` | numérico | Memoria en GB cuando aplica (puede estar vacío). |
| `source` | texto (URL) | Enlace a ficha técnica o fuente de especificaciones. |

---


### 3.4 `MLCO2/2021-10-27yearly_averages.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 642 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Promedios anuales de intensidad de carbono por zona eléctrica, con cobertura horaria indicada.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `zone_key` | texto | Identificador de zona (p. ej. prefijo país-región como `AUS-NSW`). |
| `year` | entero | Año del promedio. |
| `Country` | texto | País. |
| `Zone Name` | texto | Nombre legible de la zona eléctrica o región. |
| `carbon_intensity_avg` | numérico | Intensidad media de carbono de la electricidad (valor numérico según metodología de la fuente). |
| `no_hours_with_data` | entero | Número de horas con datos disponibles usadas en el promedio del año. |

---

## 4. Generador de logs (sintético) — desarrollo propio

Tabla orientada a sesiones de cómputo; varios campos deben alinearse con las tablas MLCO2 de la sección 3 (GPUs, factores por región e instancias cloud).

### 4.0 Integridad referencial, movilidad y datos sucios (Bronze)

| Principio | Regla |
| :--- | :--- |
| **Match con MLCO2** | `region` se elige **solo** entre los códigos `region` de `impact.csv` con `provider = aws` (nunca valores inventados tipo `"Mexico-City"` si no existen en el catálogo, para que el join con factores de emisión no quede vacío). `gpu_model` debe resolverse a un `name` de `gpus.csv` coherente con el `gpu` de `instances.csv`; `instance_type` solo desde `instances.csv` (AWS). El script valida que cada instancia mapee a una GPU del catálogo. |
| **Movilidad regional** | Cada `user_id` tiene asignado un **subconjunto de varias regiones** muestreadas del mismo `impact.csv`. Las sesiones de ese usuario solo usan regiones de ese subconjunto, de modo que un mismo usuario aparece en **distintas regiones** a lo largo del tiempo (p. ej. `us-east-1` y `ca-central-1`) y se puede analizar desplazamiento de cómputo o ahorro de emisiones en Gold/dashboards. |
| **Casos borde (Bronze)** | Aprox. **1 %** de las filas llevan `duration_hours` **inválido**: celda vacía (NULL al ingerir) o **valor negativo**. En esas filas `energy_consumed_kwh` va **vacío** (no se calcula kWh con datos basura). La **Silver** debe filtrar o corregir estas filas (Spark, Great Expectations, etc.). |

### 4.1 Identificación, tiempo y recursos

| Columna | Tipo | Descripción | Lógica de generación | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `session_id` | String | ID único de la sesión de cómputo. | UUID o correlativo del script. | `a1b2-c3d4-e5f6` |
| `user_id` | String | Usuario o cliente. | Entre 500 usuarios ficticios (`USER_000`…`USER_499`); cada uno con varias regiones posibles (§4.0). | `USER_123` |
| `timestamp` | Timestamp | Inicio de la tarea de IA. | Fecha aleatoria entre 2024-01-01 y la fecha actual. | `2026-03-15 14:30:05` |
| `gpu_model` | String | Modelo de GPU de la sesión. | Nombre canónico del catálogo MLCO2 de GPUs (sección 3.3), alineado con la instancia elegida. | `Tesla V100-SXM2-16GB` |
| `region` | String | Región cloud del job. | Solo códigos presentes en factores MLCO2 AWS en `impact.csv` (sección 3.2), acotados al pool de movilidad del usuario. | `us-east-1` |
| `duration_hours` | Float o vacío | Horas con GPU activa. | En ~99 %: aleatorio (normal acotada) entre 0.1 y 24.0. En ~1 %: vacío o negativo (Bronze sucio; ver §4.0). | `2.5` |
| `instance_type` | String | Tipo de instancia AWS. | Debe coincidir con identificadores del catálogo MLCO2 de instancias (sección 3.1). | `p3.2xlarge` |

### 4.2 Utilización, tipo de job y energía

| Columna | Tipo | Lógica de generación | ¿Por qué es útil? |
| :--- | :--- | :--- | :--- |
| `gpu_utilization` | Float | Aleatorio entre 0.1 y 1.0 (10 % a 100 %). | Una GPU al 20 % no consume como al 100 %; ajusta la fórmula de emisiones. |
| `job_type` | String | `"Training"`, `"Inference"` o `"Fine-tuning"`. | Permite comparar impacto por tipo de carga (p. ej. entrenamiento vs inferencia). |
| `energy_consumed_kwh` | Float o vacío | Si `duration_hours` es válido: `(duration_hours × TDP × utilization) / 1000`. Si no, vacío. | Valor listo para Silver/Gold cuando la duración es confiable. |
| `execution_status` | String | `"Success"` (~95 %) o `"Failed"` (~5 %). | Mide energía asociada a ejecuciones fallidas. |

---

## 5. Precios de electricidad por país (`Global_Petrol_Prices`)

**Fuente original:** [GlobalPetrolPrices.com — Electricity prices around the world](https://www.globalpetrolprices.com/electricity_prices/). En esa página, la tabla *Compare Electricity Prices by Country* publica las medias de tarifas residenciales y de negocio en USD/kWh para el periodo **2023-2026** (promedios pensados para comparar países sin el ruido trimestral). Los precios por kWh incluyen partidas típicas de la factura (distribución, energía, cargos e impuestos según metodología del sitio).

El CSV del repositorio es una **copia local** con **una fila por país** y las mismas magnitudes. Útil para análisis económico del costo energético frente a consumo o emisiones.

### 5.1 `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv`

| Metadato | Valor |
|----------|-------|
| Fuente | [globalpetrolprices.com/electricity_prices/](https://www.globalpetrolprices.com/electricity_prices/) |
| Registros (filas de datos) | 145 |
| Columnas | 3 |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |
| Nota | En parte de los países la tarifa de negocio puede ir vacía en la fuente; en el archivo actual son 11 filas con el tercer campo vacío. |

### Descripción

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country` | texto | Nombre del país o territorio (orden del archivo no implica ranking; conviene ordenar en análisis por la métrica deseada). |
| `Residential electricity rate USD per kWh (2023-2026 average)` | numérico | Tarifa media **residencial** en USD por kWh, promediada sobre 2023-2026 según el criterio con el que se construyó el archivo. |
| `Business electricity rate USD per kWh (2023-2026 average)` | numérico | Tarifa media de **negocio** en USD por kWh, mismo periodo; puede estar vacío para algunos países. |

Los nombres de columna son largos; en pipelines (p. ej. Spark o pandas) suele renombrarse a algo como `residential_usd_per_kwh` y `business_usd_per_kwh` para facilitar consultas.

---

## 6. Banco Mundial — exportaciones de servicios TIC (`World_Bank_Group`)

**Fuente:** [World Development Indicators (WDI) — Indicator BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD). Los archivos CSV del repositorio reproducen la descarga estándar del API/dataset del Banco Mundial para ese indicador (nombres de archivo con sufijo `API_BX.GSR.CCIS.CD_...`).

**Indicador (código `BX.GSR.CCIS.CD`):** *ICT service exports (BoP, current US$)* — exportaciones de servicios de TIC según la balanza de pagos, en **dólares estadounidenses corrientes**. Incluye servicios de informática y comunicaciones (telecomunicaciones, postal/mensajería) y servicios de información (datos y transacciones vinculadas a noticias). **Organización de la fuente (metadatos):** Balance of Payments Statistics Yearbook y archivos de datos, Fondo Monetario Internacional (FMI).

### 6.1 Serie principal — `World_Bank_Group/API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Fuente | [data.worldbank.org — BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD) |
| Registros (filas de datos) | 266 |
| Columnas | 71 |
| Formato | CSV; las primeras filas son metadatos del extracto (`Data Source`, `Last Updated Date`); la fila de cabecera de datos empieza en `Country Name`. |
| Codificación | UTF-8 |
| Periodo (columnas año) | 1960 a 2025 (una columna por año; celdas vacías = sin dato) |

#### Descripción de columnas

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country Name` | texto | Nombre de la economía (país o agregado regional, p. ej. *Arab World*, *Europe & Central Asia*). |
| `Country Code` | texto | Código de 3 letras (p. ej. `ARG`, `USA`) o código de grupo cuando aplica. |
| `Indicator Name` | texto | Nombre del indicador; en todos los registros: *ICT service exports (BoP, current US$)*. |
| `Indicator Code` | texto | Código único del indicador: `BX.GSR.CCIS.CD`. |
| `1960` … `2025` | numérico | Valor del indicador en USD corrientes para ese año; vacío si no existe observación. |

---

### 6.2 Metadatos por economía — `World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 265 |
| Columnas | 5 campos de datos (la cabecera exportada puede incluir BOM UTF-8 y una columna vacía por coma final) |

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `Country Code` | texto | Identificador de economía (alineable con `Country Code` del archivo principal). |
| `Region` | texto | Región del Banco Mundial (puede estar vacío en agregados). |
| `IncomeGroup` | texto | Clasificación por nivel de ingreso (puede estar vacío en agregados). |
| `SpecialNotes` | texto | Notas metodológicas, calendario fiscal, tipos de cambio, etc. (texto largo; puede ocupar varias líneas en el CSV). |
| `TableName` | texto | Nombre corto de la economía para tablas. |

Sirve para enriquecer la serie principal con **región**, **grupo de ingreso** y **notas** al hacer joins por `Country Code`.

---

## 7. Dimensión geográfica de referencia (mapeo)

Archivo mantenido por el equipo: **`bronze/reference/geo_cloud_to_country_and_zones.csv`**. Una fila por región **AWS** presente en `MLCO2/impact.csv` (mismo universo que el generador de logs sintéticos cuando filtra por `provider=aws`). Sirve como **tabla puente** en Bronze para Silver/Gold.

### 7.1 `bronze/reference/geo_cloud_to_country_and_zones.csv`

| Metadato | Valor |
|----------|-------|
| Registros | 22 (una por región AWS en `impact.csv` al momento de creación) |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |

### Descripción de columnas

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `cloud_provider` | texto | Proveedor (`aws` en esta versión). |
| `cloud_region` | texto | Código de región cloud (p. ej. `us-east-1`); join con `usage_logs.region` y `impact.csv` (`region` con `provider=aws`). |
| `region_name_mlco2` | texto | Nombre legible MLCO2 (`regionName` en `impact.csv`). |
| `country_name_mlco2` | texto | País tal como figura en `impact.csv` (trazabilidad con la fuente). |
| `country_name_global_petrol` | texto | Nombre de país alineado a la columna `Country` de `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` cuando difiere (p. ej. `United Kingdom` → `UK`). |
| `iso_alpha2` | texto | ISO 3166-1 alpha-2 (p. ej. `GB`, `US`). |
| `iso_alpha3` | texto | ISO 3166-1 alpha-3 (p. ej. `GBR`, `USA`); alinear con OWID `iso_code` y Banco Mundial `Country Code` donde el código sea de país. |
| `electricity_maps_zone` | texto | Identificador de zona compatible con Electricity Maps / extractos MLCO2 `zone_key` (p. ej. `US-PJM`, `JP-TK`). Puede ir **vacío** si la zona debe resolverse solo desde el catálogo en vivo de la API. |
| `is_primary_zone` | booleano (`true`/`false`) | En esta versión siempre `true` (una fila por región). Reservado para filas adicionales mismo `cloud_region` con varias zonas EM. |
| `mapping_notes` | texto | Supuestos, alternativas y advertencias (p. ej. zonas US aproximadas, GovCloud, India sin zona local en extracto anual). |

**Uso en pipelines:** en Silver, unir logs por `(cloud_provider, cloud_region)`; precios por `country_name_global_petrol` o por `iso_alpha3`; intensidad EM por `electricity_maps_zone` cuando no esté vacío. OWID suele requerir `iso_alpha3` + `year`.

---

## 8. Precios EC2 On-Demand — proxy de TCO de cómputo

Catálogo **Bronze** generado o actualizado con **`scripts/build_aws_ec2_pricing_reference.py`**: cruza cada región AWS del archivo geográfico §7 con cada `instance_type` AWS de `MLCO2/instances.csv`. Los valores base en **US East (N. Virginia)** provienen de referencia pública ([instances.vantage.sh](https://instances.vantage.sh), consulta abril 2026); el precio por región es `precio_base × multiplicador_regional` (multiplicadores aproximados documentados en el script).

### 8.1 `bronze/reference/aws_ec2_on_demand_usd_per_hour.csv`

| Metadato | Valor |
|----------|-------|
| Registros | 308 (22 regiones × 14 tipos instancia MLCO2 AWS) |
| Formato | CSV, separador coma, cabecera en línea 1 |
| Codificación | UTF-8 |

### Descripción de columnas

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `cloud_provider` | texto | `aws`. |
| `cloud_region` | texto | Región (p. ej. `eu-west-1`); join con `usage_logs.region` y §7. |
| `instance_type` | texto | Tipo EC2 (p. ej. `p3.2xlarge`); join con `usage_logs.instance_type` y `instances.csv`. |
| `operating_system` | texto | `linux` (precios list orientados a cargas ML típicas). |
| `tenancy` | texto | `shared`. |
| `pricing_model` | texto | `on_demand`. |
| `currency` | texto | `USD`. |
| `price_usd_per_hour` | numérico | Tarifa horaria estimada en la región. |
| `price_basis_region` | texto | Región de referencia del precio base (`us-east-1`). |
| `regional_multiplier` | numérico | Factor aplicado respecto a `price_basis_region`. |
| `as_of_date` | texto | Fecha del snapshot (`YYYY-MM-DD`). |
| `source` | texto | Procedencia resumida (Vantage + script). |
| `pricing_notes` | texto | Límites del modelo (sin EBS, egress, Spot, RI, etc.). |

**Uso en pipelines:** en Silver/Gold, join logs con `(cloud_provider, cloud_region, instance_type)` y opcionalmente filtrar por `pricing_model`. **Coste cómputo (proxy):** `duration_hours * price_usd_per_hour` para sesiones exitosas o análisis acordado. Complementa el **coste eléctrico** de la pregunta de negocio 2 (TDP/kWh × tarifa eléctrica), no lo reemplaza.
