# Kings-League

El proyecto de big data para descargar los resultados de la 'Kings League' consistiría en la creación de un pipeline de datos utilizando Apache Airflow para automatizar la descarga de los resultados de partidos, estadísticas y otros datos relacionados con la liga.

## Fuente de Datos 
La primera etapa del proyecto sería la configuración de los orígenes de datos, que incluirían sitios web oficiales de la liga, así como otras fuentes de información relevantes. Una vez configuradas las fuentes de datos, se crearían tareas en Airflow para automatizar la descarga de los datos en formato CSV o JSON.
<br/>
<img align='left' alt="Fuente" src="https://raw.githubusercontent.com/BonfantiMatias/Kings-League/main/assets/Fuente.png"/>
<br/>

## Airflow
Una vez descargados los datos, se utilizarían tareas de Airflow para limpiar y transformar los datos, eliminando datos irrelevantes y estandarizando los campos para facilitar su análisis.
<br/>
<img align='left' alt="Airflow" src="https://raw.githubusercontent.com/BonfantiMatias/Kings-League/main/assets/Airflow_.png"/>
<br/>
## Snowflake
Finalmente, los datos limpios y transformados se cargarían en Snowflake, una plataforma de almacenamiento de datos en la nube, para su análisis y visualización. Se crearían tablas en Snowflake para almacenar los datos, y se utilizarían herramientas de análisis como SQL y Python para extraer información valiosa de los datos.
<br/>
<img align='left' alt="Snowflake" src="https://raw.githubusercontent.com/BonfantiMatias/Kings-League/main/assets/Snowflake.png"/>
<br/>

El proyecto incluiría también un sistema de monitoreo y alertas para detectar posibles problemas o errores en el pipeline de datos, y un sistema de seguridad para proteger los datos almacenados en Snowflake. En resumen, este proyecto de big data permitiría a los interesados en la liga tener acceso a datos actualizados y precisos, y facilitaría el análisis y la toma de decisiones basadas en esos datos.