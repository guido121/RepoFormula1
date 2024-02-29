
# Proyecto Delta Lakehouse: Formula 1

# Introduction
El proyecto consiste en la implementación de un Pipeline utilizando Data Factory, donde los datos se encuentran alojados en un ADLS. Se utilizaron notebooks de databricks para realizar las transformaciones hacias las capas de la arquitectura Medalion. 

## Datasets

![Logo](https://community.listopro.com/content/images/size/w1000/2023/04/image-4.png)

## Arquitectura

![Logo](https://learn.microsoft.com/en-us/azure/architecture/solution-ideas/media/ingest-etl-and-stream-processing-with-azure-databricks.svg)



## Instalación
Para poder realizar la ejecución del presente project se deberían se seguir los siguientes pasos:
1.	Desplegar los servicios ADLS, ADF, Databricks workspace.
2.	En el servicio ADLS habilitar la jerarquia de nombres de espacio (Data Lake).
3.  En databricks crear un cluster, clonar este repositorio.
4.	Ejecutar el notebook create_database.dbc
5.	Referenciar los notebooks en un Pipeline en ADF.

## Despliegue:
Crear un trigger en Data Factory y programar su ejecución.


## 🛠 Skills
1. Azure: Data Factory (ADF), Azure Storage Account (ADLS), Key Vault.
2. Databricks
3. Apache Spark

## Autores

- [@guido121](https://github.com/guido121)

