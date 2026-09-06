import logging
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def build_config(bucket_name: str) -> dict:
    return {
        "tb_clientes_bronze": {
            "input_path": f"s3://{bucket_name}/data-input/bronze/base-clientes/clientes.csv",
            "output_path": f"s3a://{bucket_name}/data-output/bronze/base-clientes/",
            "table_name": "tb_clientes_bronze",
            "database": "workspace",
            "separator": ";",
        },
        "tb_vendas_bronze": {
            "input_path": f"s3://{bucket_name}/data-input/bronze/base-vendas/vendas.csv",
            "output_path": f"s3://{bucket_name}/data-output/bronze/base-vendas/",
            "table_name": "tb_vendas_bronze",
            "database": "workspace",
            "separator": ",",
        },
    }


def read_csv(spark, input_path: str, separator: str):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", separator)
        .option("encoding", "utf-8")
        .load(input_path)
    )


def get_partition_value():
    """Retorna valor de partição anomesdia (YYYYMMDD) com a data de processamento"""
    return datetime.now().strftime("%Y%m%d")


def write_parquet_partitioned(spark, df, output_path: str, partition_value: str):
    """Escreve dados em Parquet com particionamento por anomesdia"""
    logger = configure_logging()
    
    # Adicionar coluna de partição temporariamente para o write
    df_com_particao = df.withColumn("anomesdia", lit(partition_value))
    
    # Escrever com particionamento - a coluna anomesdia ficará no caminho do diretório
    df_com_particao.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("anomesdia") \
        .parquet(output_path)
    
    logger.info("Dados particionados salvos: anomesdia=%s em %s", partition_value, output_path)


def register_table(spark, database: str, table_name: str, output_path: str):
    """Registra tabela no Glue Catalog com detecção automática de partições"""
    logger = configure_logging()
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Criar tabela apontando para o local dos dados (sem PARTITIONED BY)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING parquet
        LOCATION '{output_path}'
        """
    )
    
    # Detectar automaticamente as partições criadas pelo Spark
    spark.sql(f"MSCK REPAIR TABLE {database}.{table_name}")
    logger.info("Tabela %s registrada com partições detectadas automaticamente", table_name)


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
    bucket_name = args.get("BUCKET_NAME", "129264105405-lab")
    logger = configure_logging()

    logger.info("Inicializando Glue Job...")
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    config = build_config(bucket_name)
    
    # Obter valor da partição (dia de processamento)
    partition_value = get_partition_value()
    logger.info("Data de partição: %s", partition_value)

    for table_key, table_cfg in config.items():
        logger.info("Processando tabela: %s", table_key)

        df = read_csv(
            spark=spark,
            input_path=table_cfg["input_path"],
            separator=table_cfg["separator"],
        )

        logger.info("Registros lidos: %s", df.count())
        
        # Escrever com particionamento por anomesdia
        write_parquet_partitioned(spark, df, table_cfg["output_path"], partition_value)
        logger.info("Parquet gravado em: %s (particionado por anomesdia=%s)", table_cfg["output_path"], partition_value)

        # Registrar tabela
        register_table(
            spark=spark,
            database=table_cfg["database"],
            table_name=table_cfg["table_name"],
            output_path=table_cfg["output_path"],
        )
        logger.info("Tabela registrada no Glue Catalog: %s", table_cfg["table_name"])

    logger.info("Job concluído com sucesso.")
    job.commit()


if __name__ == "__main__":
    main()
