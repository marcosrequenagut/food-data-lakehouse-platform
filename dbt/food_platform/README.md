Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


# COSAS A AÑADIR EN EL README
- Para lanzar el proyecto, simplemente hay que lanzar el docker compose
- Luego nos metemos en la UI de AIRFLOW y lanzamos la pipeline
- Los datos se actualizan y se decargan automaticamente, haciendose las transfromaciones necesarias.
- Para crear las tablas de DBT y demás habrá que ejecutar los staggings o mars necesarios.
- Si se ejecuta en la carpeta dbt/food_platform el comando dbt docs generate --profiles-dir .
en bash, obtenemos documentación de las tablas y del lineage del modelo de dbt. dbt docs generate te da:

mapa visual del pipeline
documentación de modelos
columnas y tests
sources + freshness
lineage completo

Básicamente: documentación automática de todo tu data warehouse

Para mirar visualmente todo esto se usa el comando dbt docs serve --port 8081 (en nuestro caso hay
que indicar el puerto porque por defecto va al 8080 pero ese ya está ocupado por airflow,
entonces hay que darle otro puerto diferente) para ver una UI de DBT con toda la info
- Para ejecutar las views o tablas: dbt run --profiles-dir . --select dim_brand fact_products (en la carpeta dbt/food_platform)