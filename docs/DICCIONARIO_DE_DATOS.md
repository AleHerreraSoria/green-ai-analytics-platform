# Diccionario de datos del repositorio

Este documento describe el **stack de información** del proyecto: contexto histórico y económico (Our World in Data), intensidad de carbono en tiempo real (Electricity Maps), tablas maestras de hardware e impacto regional (MLCO2), **precios medios de electricidad por país**, **exportaciones de servicios TIC** (Banco Mundial / WDI), y sesiones de uso de IA generadas de forma sintética. Las rutas de archivo indican copias locales en el repositorio cuando aplica.

## Fuentes de datos (el stack de información)

| # | Fuente | Descripción | Acceso | Justificación de elección |
|---|--------|-------------|--------|---------------------------|
| 1 | **Our World in Data (Energy)** | Datos históricos por país: PIB, población y mezcla energética (renovables vs. fósiles). | [Dataset en GitHub (OWID)](https://github.com/owid/energy-data) | Contexto económico y base comparativa histórica para países de América. |
| 2 | **Electricity Maps API** | Intensidad de carbono (gCO₂eq/kWh) en tiempo real por zona geográfica; mix y catálogo de zonas. | [Electricity Maps](https://www.electricitymaps.com/) | Componente en tiempo real que justifica ingesta automatizada (p. ej. con Airflow). |
| 3 | **MLCO2 — hardware e impacto** | Especificaciones TDP de GPUs/CPUs y factores de emisión por región de cómputo (proyecto MLCO2 / *impact*). | [MLCO2 / impact (GitHub)](https://github.com/mlco2/impact) | Tabla maestra para pasar de uso de hardware a consumo energético y emisiones. |
| 4 | **Generador de logs (sintético)** | Registros de sesiones de uso de IA (usuario, GPU, región, horas, etc.). | Desarrollo propio | Control de volumen (p. ej. 100k+ filas) y simulación de movilidad entre regiones de cómputo. |
| 5 | **Precios de electricidad por país** | Tarifas medias residenciales y de negocio en USD/kWh (promedio 2023-2026) por país. | [GlobalPetrolPrices.com — Electricity prices](https://www.globalpetrolprices.com/electricity_prices/); copia local: `Global_Petrol_Prices/electricity_prices_by_country_2023_2026_avg.csv` | Complementa el contexto energético con el **costo** de la electricidad a nivel país (distinto de intensidad de carbono o mix). |
| 6 | **Banco Mundial — WDI** | Exportaciones de servicios de tecnologías de la información y comunicación (TIC), balanza de pagos, US$ corrientes. | [Indicador BX.GSR.CCIS.CD](https://data.worldbank.org/indicator/BX.GSR.CCIS.CD); archivos en `World_Bank_Group/` | Contexto macroeconómico del sector digital/TIC por economía y en el tiempo (series anuales). |

---
## 1. Our World in Data (Energy)

Datos históricos por país o región agregada. En el repositorio: `OWID/owid-energy-data.csv` y el codebook asociado.

### 1.1 Dataset principal (`OWID/owid-energy-data.csv`)

| Metadato | Valor |
|----------|-------|
| Registros (filas de datos) | 23232 |
| Columnas | 130 |
| Formato | CSV, separador coma |
| Codificación | UTF-8 |

### Descripción

Serie temporal de energía por país o región agregada (Our World in Data). Cada fila es un par (entidad geográfica, año). Los valores numéricos pueden estar vacíos donde no hay dato.

La definición completa de cada variable (título, descripción, unidad y fuente) está en `OWID/owid-energy-codebook.csv`, reproducido en la sección 1.2 siguiente.

### 1.2 Codebook de variables (`OWID/owid-energy-codebook.csv`)

| Metadato | Valor |
|----------|-------|
| Registros (una fila por variable del dataset principal) | 130 |
| Formato | CSV: column, title, description, unit, source |

### Contenido completo del codebook

| column | title | description | unit | source |
|--------|-------|-------------|------|--------|
| country | Country | Geographic location. |  | Our World in Data - Regions (2025) |
| year | Year | Year of observation. |  | Our World in Data - Regions (2025) |
| iso_code | ISO code | ISO 3166-1 alpha-3 three-letter country codes. |  | International Organization for Standardization - Regions (2023) |
| population | Population | Population by country, available from 10,000 BCE to 2100, based on data and estimates from different sources. | people | Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| gdp | Gross domestic product (GDP) | Total economic output of a country or region per year. This data is adjusted for inflation and differences in living costs between countries. | international-$ in 2011 prices ($) | Bolt and van Zanden – Maddison Project Database 2023 [https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023] |
| biofuel_cons_change_pct | Annual percentage change in biofuel consumption | Includes biogasoline (such as ethanol) and biodiesel. Volumes have been adjusted for energy content. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| biofuel_cons_change_twh | Annual change in biofuel consumption | Includes biogasoline (such as ethanol) and biodiesel. Volumes have been adjusted for energy content. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| biofuel_cons_per_capita | Biofuel consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| biofuel_consumption | Primary energy consumption from biofuels | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| biofuel_elec_per_capita | Electricity generation from bioenergy per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| biofuel_electricity | Electricity generation from bioenergy | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| biofuel_share_elec | Share of electricity generated by bioenergy | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| biofuel_share_energy | Share of primary energy consumption that comes from biofuels | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| carbon_intensity_elec | Lifecycle carbon intensity of electricity generation | Greenhouse gases emitted per unit of generated electricity, measured in grams of CO₂ equivalents per kilowatt-hour. | grams of CO₂ equivalents per kilowatt-hour (gCO₂) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| coal_cons_change_pct | Annual percentage change in coal consumption |  | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| coal_cons_change_twh | Annual change in coal consumption |  | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| coal_cons_per_capita | Coal consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| coal_consumption | Primary energy consumption from coal | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| coal_elec_per_capita | Electricity generation from coal per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| coal_electricity | Electricity generation from coal | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| coal_prod_change_pct | Annual change in coal production | Measured as a percentage of the previous year's production. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| coal_prod_change_twh | Annual change in coal production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| coal_prod_per_capita | Coal production per capita | Measured in kilowatt-hours per capita. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| coal_production | Coal production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| coal_share_elec | Share of electricity generated by coal | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| coal_share_energy | Share of primary energy consumption that comes from coal | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| electricity_demand | Electricity demand | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| electricity_demand_per_capita | Total electricity demand per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| electricity_generation | Total electricity generation | Total electricity generated in each country or region, measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| electricity_share_energy | Total electricity generation as share of primary energy | Measured as a percentage of total, direct primary energy consumption in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| energy_cons_change_pct | Annual change in primary energy consumption |  | % | U.S. Energy Information Administration - International Energy Data (2025) [https://www.eia.gov/opendata/bulkfiles.php]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| energy_cons_change_twh | Annual change in primary energy consumption |  | terawatt-hours (TWh) | U.S. Energy Information Administration - International Energy Data (2025) [https://www.eia.gov/opendata/bulkfiles.php]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| energy_per_capita | Primary energy consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours per person (kWh) | U.S. Energy Information Administration - International Energy Data (2025) [https://www.eia.gov/opendata/bulkfiles.php]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| energy_per_gdp | Primary energy consumption per GDP | Measured in kilowatt-hours per international-$. | kilowatt-hours per $ (kWh) | U.S. Energy Information Administration - International Energy Data (2025) [https://www.eia.gov/opendata/bulkfiles.php]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Bolt and van Zanden – Maddison Project Database 2023 [https://www.rug.nl/ggdc/historicaldevelopment/maddison/releases/maddison-project-database-2023] |
| fossil_cons_change_pct | Annual percentage change in fossil fuel consumption |  | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| fossil_cons_change_twh | Annual change in fossil fuel consumption |  | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| fossil_elec_per_capita | Electricity generation from fossil fuels per person | Electricity generation from coal, oil, and gas, measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| fossil_electricity | Electricity generation from fossil fuels | Electricity generation from coal, oil, and gas, measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| fossil_energy_per_capita | Fossil fuel consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| fossil_fuel_consumption | Primary energy consumption from fossil fuels | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| fossil_share_elec | Share of electricity generated by fossil fuels | Electricity generation from coal, oil, and gas, measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| fossil_share_energy | Share of primary energy consumption that comes from fossil fuels | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_cons_change_pct | Annual percentage change in gas consumption |  | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_cons_change_twh | Annual change in gas consumption |  | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_consumption | Primary energy consumption from gas | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_elec_per_capita | Electricity generation from gas per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| gas_electricity | Electricity generation from gas | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_energy_per_capita | Gas consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| gas_prod_change_pct | Annual change in gas production | Measured as a percentage of the previous year's production. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| gas_prod_change_twh | Annual change in gas production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| gas_prod_per_capita | Gas production per capita | Measured in kilowatt-hours per capita. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| gas_production | Gas production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| gas_share_elec | Share of electricity generated by gas | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| gas_share_energy | Share of primary energy consumption that comes from gas | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| greenhouse_gas_emissions | Lifecycle emissions from electricity generation | Measured in megatonnes of CO₂ equivalents. | million tonnes CO₂ equivalents (Mt) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| hydro_cons_change_pct | Annual percentage change in hydropower consumption | Figures are based on gross primary hydroelectric generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| hydro_cons_change_twh | Annual change in hydropower consumption | Input-equivalent energy is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| hydro_consumption | Primary energy consumption from hydropower | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| hydro_elec_per_capita | Electricity generation from hydropower per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| hydro_electricity | Electricity generation from hydropower | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| hydro_energy_per_capita | Hydropower consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| hydro_share_elec | Share of electricity generated by hydropower | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| hydro_share_energy | Share of primary energy consumption that comes from hydropower | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_cons_change_pct | Annual percentage change in low-carbon energy consumption | Figures are based on gross generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_cons_change_twh | Annual change in low-carbon energy consumption | Input-equivalent energy is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_consumption | Primary energy consumption from low-carbon sources | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_elec_per_capita | Electricity generation from low-carbon sources per person | Measured in kilowatt-hours per person. Low-carbon sources correspond to renewables and nuclear power, that produce significantly less greenhouse-gas emissions than fossil fuels. Renewables include solar, wind, hydropower, bioenergy, geothermal, wave, and tidal. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| low_carbon_electricity | Electricity generation from low-carbon sources | Measured in terawatt-hours. Low-carbon sources correspond to renewables and nuclear power, that produce significantly less greenhouse-gas emissions than fossil fuels. Renewables include solar, wind, hydropower, bioenergy, geothermal, wave, and tidal. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_energy_per_capita | Low-carbon energy consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| low_carbon_share_elec | Share of electricity generated by low-carbon sources | Measured as a percentage of total electricity produced in the country or region. Low-carbon sources correspond to renewables and nuclear power, that produce significantly less greenhouse-gas emissions than fossil fuels. Renewables include solar, wind, hydropower, bioenergy, geothermal, wave, and tidal. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| low_carbon_share_energy | Share of primary energy consumption that comes from low-carbon sources | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| net_elec_imports | Net electricity imports | Electricity imports minus exports, measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| net_elec_imports_share_demand | Net electricity imports as a share of demand | Electricity imports minus exports, measured as a percentage of total electricity demand in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| nuclear_cons_change_pct | Annual percentage change in nuclear power consumption | Figures are based on gross generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| nuclear_cons_change_twh | Annual change in nuclear power consumption | Input-equivalent energy is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| nuclear_consumption | Primary energy consumption from nuclear power | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| nuclear_elec_per_capita | Electricity generation from nuclear power per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| nuclear_electricity | Electricity generation from nuclear | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| nuclear_energy_per_capita | Nuclear power consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| nuclear_share_elec | Share of electricity generated by nuclear power | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| nuclear_share_energy | Share of primary energy consumption that comes from nuclear power | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_cons_change_pct | Annual percentage change in oil consumption |  | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_cons_change_twh | Annual change in oil consumption |  | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_consumption | Primary energy consumption from oil | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_elec_per_capita | Electricity generation from oil per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| oil_electricity | Electricity generation from oil | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_energy_per_capita | Oil consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| oil_prod_change_pct | Annual change in oil production | Measured as a percentage of the previous year's production. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| oil_prod_change_twh | Annual change in oil production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| oil_prod_per_capita | Oil production per capita | Measured in kilowatt-hours per capita. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| oil_production | Oil production | Measured in terawatt-hours. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; The Shift Data Portal - Energy production from fossil fuels (2019) [https://www.theshiftdataportal.org/] |
| oil_share_elec | Share of electricity generated by oil | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| oil_share_energy | Share of primary energy consumption that comes from oil | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewable_consumption | Primary energy consumption from other renewables | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewable_electricity | Electricity generation from other renewables, including bioenergy | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewable_exc_biofuel_electricity | Electricity generation from other renewables, excluding bioenergy | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/] |
| other_renewables_cons_change_pct | Annual percentage change in other renewables consumption | Figures are based on gross generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewables_cons_change_twh | Annual change in other renewables consumption | Input-equivalent energy, in terawatt-hours, is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewables_elec_per_capita | Electricity generation from other renewables, including bioenergy, per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| other_renewables_elec_per_capita_exc_biofuel | Electricity generation from other renewables, excluding bioenergy, per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| other_renewables_energy_per_capita | Other renewables consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| other_renewables_share_elec | Share of electricity generated by other renewables, including bioenergy | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewables_share_elec_exc_biofuel | Share of electricity generated by other renewables, excluding bioenergy | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| other_renewables_share_energy | Share of primary energy consumption that comes from other renewables | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| per_capita_electricity | Total electricity generation per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| primary_energy_consumption | Primary energy consumption | Measured in terawatt-hours. | terawatt-hours (TWh) | U.S. Energy Information Administration - International Energy Data (2025) [https://www.eia.gov/opendata/bulkfiles.php]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_cons_change_pct | Annual percentage change in renewables consumption |  | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_cons_change_twh | Annual change in renewables consumption |  | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_consumption | Primary energy consumption from renewables | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_elec_per_capita | Electricity generation from renewables per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| renewables_electricity | Electricity generation from renewables | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_energy_per_capita | Renewables consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| renewables_share_elec | Share of electricity generated by renewables | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| renewables_share_energy | Share of primary energy consumption that comes from renewables | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_cons_change_pct | Annual percentage change in solar power consumption | Figures are based on gross generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_cons_change_twh | Annual change in solar power consumption | Input-equivalent energy, in terawatt-hours, is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_consumption | Primary energy consumption from solar power | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_elec_per_capita | Electricity generation from solar power per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| solar_electricity | Electricity generation from solar power | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_energy_per_capita | Solar power consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| solar_share_elec | Share of electricity generated by solar power | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| solar_share_energy | Share of primary energy consumption that comes from solar power | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_cons_change_pct | Annual percentage change in wind power consumption | Figures are based on gross generation and do not account for cross-border electricity supply. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_cons_change_twh | Annual change in wind power consumption | Input-equivalent energy, in terawatt-hours, is based on gross generation and does not account for cross-border electricity supply. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_consumption | Primary energy consumption from wind power | Measured in terawatt-hours, using the substitution method. | terawatt-hours (TWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_elec_per_capita | Electricity generation from wind power per person | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| wind_electricity | Electricity generation from wind power | Measured in terawatt-hours. | terawatt-hours (TWh) | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_energy_per_capita | Wind power consumption per capita | Measured in kilowatt-hours per person. | kilowatt-hours (kWh) | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/]; Population based on various sources (2024) [https://ourworldindata.org/population-sources] |
| wind_share_elec | Share of electricity generated by wind power | Measured as a percentage of total electricity produced in the country or region. | % | Ember - Yearly Electricity Data Europe (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Ember - Yearly Electricity Data (2026) [https://ember-energy.org/data/yearly-electricity-data/]; Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |
| wind_share_energy | Share of primary energy consumption that comes from wind power | Measured as a percentage of the total primary energy, using the substitution method. | % | Energy Institute - Statistical Review of World Energy (2025) [https://www.energyinst.org/statistical-review/] |

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

### 4.1 Identificación, tiempo y recursos

| Columna | Tipo | Descripción | Lógica de generación | Ejemplo |
| :--- | :--- | :--- | :--- | :--- |
| `session_id` | String | ID único de la sesión de cómputo. | UUID o correlativo del script. | `a1b2-c3d4-e5f6` |
| `user_id` | String | Usuario o cliente. | Selección aleatoria entre 500 usuarios ficticios. | `USER_942` |
| `timestamp` | Timestamp | Inicio de la tarea de IA. | Fecha aleatoria entre 2024-01-01 y la fecha actual. | `2026-03-15 14:30:05` |
| `gpu_model` | String | Modelo de GPU de la sesión. | Debe coincidir con el catálogo MLCO2 de GPUs (sección 3.3). | `Tesla V100` |
| `region` | String | Región cloud del job. | Debe coincidir con códigos de región en factores MLCO2 (sección 3.2). | `us-east-1` |
| `duration_hours` | Float | Horas con GPU activa. | Aleatorio (distribución normal) entre 0.1 y 24.0. | `2.5` |
| `instance_type` | String | Tipo de instancia AWS. | Debe coincidir con identificadores del catálogo MLCO2 de instancias (sección 3.1). | `p3.2xlarge` |

### 4.2 Utilización, tipo de job y energía

| Columna | Tipo | Lógica de generación | ¿Por qué es útil? |
| :--- | :--- | :--- | :--- |
| `gpu_utilization` | Float | Aleatorio entre 0.1 y 1.0 (10 % a 100 %). | Una GPU al 20 % no consume como al 100 %; ajusta la fórmula de emisiones. |
| `job_type` | String | `"Training"`, `"Inference"` o `"Fine-tuning"`. | Permite comparar impacto por tipo de carga (p. ej. entrenamiento vs inferencia). |
| `energy_consumed_kwh` | Float | `(duration_hours × TDP × utilization) / 1000` | Valor listo para capas Silver/Gold y visualización. |
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

### 6.2 Metadatos del indicador — `World_Bank_Group/Metadata_Indicator_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

| Metadato | Valor |
|----------|-------|
| Registros (definición del indicador) | 1 |
| Columnas | 4 |

| Campo | Descripción |
|-------|-------------|
| `INDICATOR_CODE` | `BX.GSR.CCIS.CD` |
| `INDICATOR_NAME` | Nombre largo del indicador (inglés). |
| `SOURCE_NOTE` | Definición ampliada: alcance de “servicios TIC” y moneda (USD corrientes). |
| `SOURCE_ORGANIZATION` | Entidad que provee la serie subyacente (p. ej. FMI / BoP). |

---

### 6.3 Metadatos por economía — `World_Bank_Group/Metadata_Country_API_BX.GSR.CCIS.CD_DS2_en_csv_v2_920.csv`

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
