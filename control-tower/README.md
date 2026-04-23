# 🕹️ Control Tower - Guía de Desarrollo y Reglas

Este directorio contiene la aplicación de observabilidad y presentación del proyecto **Green AI**. Está construida con **Streamlit** para centralizar la visualización del pipeline, los buckets de AWS S3 y el estado de la orquestación en Airflow.

## ⚖️ Reglas de Oro para Colaboradores

Para mantener la coherencia y profesionalismo de la interfaz, todos los miembros del equipo deben seguir estas reglas:

### 1. Estructura de Páginas
* **Página Principal (`main.py`):** Reservada exclusivamente para el Storytelling, visión de la ONG y resumen de impacto. No se debe añadir lógica de procesamiento aquí.
* **Nuevas Páginas:** Deben crearse dentro de la carpeta `pages/`.
* **Nomenclatura:** Usa el prefijo numérico para el orden en el menú (ej: `1_Infraestructura.py`, `2_Calidad_Datos.py`). Usa guiones bajos para espacios; Streamlit los convertirá en espacios automáticamente.

### 2. Gestión de Secretos (SEGURIDAD)
* **PROHIBIDO** escribir credenciales de AWS (Access Keys) o API Keys directamente en el código.
* Usa el archivo `.streamlit/secrets.toml` (que debe estar en el `.gitignore`) o variables de entorno.
* Toda conexión a AWS debe pasar por el módulo centralizado en `utils/aws_client.py`.

### 3. Estilo y UI
* **Layout:** Todas las páginas deben iniciar con `st.set_page_config(layout="wide")` para aprovechar el ancho de pantalla.
* **Componentes:** Antes de crear un gráfico desde cero, verifica si existe un componente en `components/` para mantener la identidad visual del proyecto.
* **Colores:** Usa la paleta definida en el archivo de configuración (Verdes: `#deff9a`, Oscuros: `#111`).

### 4. Lógica vs. Visualización
* Mantén las páginas "limpias". La lógica pesada de Spark o consultas complejas a S3 deben vivir en la carpeta `utils/`. La página de Streamlit solo debe llamar a esas funciones y mostrar los resultados.

### 5. Documentación Interna
* Cada nueva página añadida debe incluir un breve comentario al inicio explicando qué pregunta de negocio resuelve o qué parte del pipeline está monitoreando.

---
*Este documento es dinámico. Si encuentras una mejor forma de optimizar la interfaz, propón el cambio en el canal de comunicación del equipo antes de implementarlo.*