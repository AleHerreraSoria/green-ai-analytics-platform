# Documento de Requerimientos de Infraestructura  
**Proyecto:** Green AI – Data Lakehouse  

---

## 1. Inventario de Servicios AWS

### ☁️ Almacenamiento – Amazon S3
Buckets:
- Bronze 
- Silver
- Gold

---

### 🔐 Seguridad – AWS IAM
Roles para:
- Airflow
- EC2
- Acceso a S3

---

### ⚙️ Cómputo – Amazon EC2
Instancias:
- 1 para Airflow
- 1 para Spark  

---

### 📊 Orquestación
- Apache Airflow (ejecutándose en EC2)

---

### 🧱 Infraestructura como Código
- Terraform

---

## 2. Justificación de Servicios (Free Tier)

### 🟢 Amazon S3
**Free Tier:**
- 5 GB de almacenamiento gratuito  
- Requests limitados (suficientes para uso académico)

**Justificación:**
- Ideal para arquitectura Data Lake (Bronze / Silver / Gold)
- Escalable
- Integración nativa con Spark

---

### 🟢 AWS IAM
**Costo:**
- Sin costo directo

**Justificación:**
- Control de acceso seguro
- Gestión de permisos por roles
- Buenas prácticas de la industria

---

### 🟡 Amazon EC2
**Free Tier:**
- 750 horas/mes de instancias t2.micro o t3.micro
- Disponible durante 12 meses

**Justificación:**
- Permite ejecutar Airflow y Spark
- Entorno flexible
- Más adecuado que alternativas serverless

---

### 🔴 AWS Lambda (Descartado)
**Ventajas:**
- Serverless

**Desventajas:**
- Límite de tiempo de ejecución
- No apto para workloads de Spark
- Complejidad para orquestar con Airflow

**Decisión:** Utilizar EC2

---

## 3. Configuración Técnica

### 🌎 Región                            
- us-east-1 (Norte de Virginia)       

---

### 🪣 Buckets S3
Nombres:
bronze_bucket_name = "green-ai-pf-bronze-a0e96d06"
gold_bucket_name = "green-ai-pf-gold-a0e96d06"
silver_bucket_name = "green-ai-pf-silver-a0e96d06"

**Configuración:**
- Versionado: desactivado (para evitar costos adicionales)
- Acceso público: bloqueado

---

### 🖥️ Configuración EC2
- Tipo: t2.micro / t3.micro  
- Sistema operativo: Ubuntu Server  
- Especificaciones:
  - 1 vCPU
  - 1 GB RAM  

---

### 🔐 Roles IAM

**role-airflow**
- Acceso de lectura/escritura a S3

**role-ec2**
- Permisos básicos + logging

**Políticas:**
- AmazonS3FullAccess (simplificado para entorno académico)

---

### 🔄 CI/CD
- GitHub Actions:
  - Linter
  - Despliegue automático

---

## 4. Estimación de Costos

| Servicio | Costo estimado |
|--------|--------------|
| S3 | $0 (dentro del Free Tier) |
| IAM | $0 |
| EC2 | $0 (Free Tier) |
| Transferencia de datos | $0 (uso mínimo) |

**Costo total estimado:** $0 USD

---

## 5. Criterios de Aceptación

- [x] Inventario de servicios AWS  
- [x] Justificación del uso dentro del Free Tier  
- [x] Configuración técnica mínima definida  

---

## 6. Riesgos y Mitigación

### ⚠️ Riesgos
- Superar los límites del Free Tier:
  - EC2 en ejecución continua
  - S3 excediendo 5 GB

### 🛡️ Mitigación
- Detener instancias EC2 cuando no estén en uso  
- Monitorear consumo con AWS Budgets  

---
