import logging
import re
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit


SOURCE_FILE = "Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv"


def configure_logging() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def build_config(bucket_name: str) -> dict:
    return {
        "tb_state_of_data_bronze": {
            "input_path": (
                f"s3://{bucket_name}/data-input/bronze/"
                f"state-of-data-2024/{SOURCE_FILE}"
            ),
            "output_path": (
                f"s3://{bucket_name}/data-output/bronze/"
                "state-of-data-2024/"
            ),
            "table_name": "tb_state_of_data_bronze",
            "database": "workspace",
            "separator": ",",
            "encoding": "ISO-8859-1",
        },
    }


def read_csv(spark, input_path: str, separator: str, encoding: str):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", separator)
        .option("encoding", encoding)
        .option("multiLine", "true")
        .load(input_path)
    )


def normalize_column_names(df, logger):
    """Converte nomes do CSV em identificadores aceitos pelo Hive Metastore."""
    used_names = {"anomesdia"}
    normalized_names = []

    for position, original_name in enumerate(df.columns, start=1):
        normalized_name = re.sub(r"[^A-Za-z0-9_]", "_", original_name.strip())
        normalized_name = re.sub(r"_+", "_", normalized_name).strip("_")

        if not normalized_name:
            normalized_name = f"col_{position}"
        elif normalized_name[0].isdigit():
            normalized_name = f"col_{normalized_name}"

        base_name = normalized_name
        suffix = 1
        while normalized_name in used_names:
            normalized_name = f"{base_name}_{suffix}"
            suffix += 1

        used_names.add(normalized_name)
        normalized_names.append(normalized_name)

        if original_name != normalized_name:
            logger.info("Coluna normalizada: %s -> %s", original_name, normalized_name)

    return df.toDF(*normalized_names)


def get_partition_value():
    return datetime.now().strftime("%Y%m%d")


def write_parquet_partitioned(spark, df, output_path: str, partition_value: str):
    logger = configure_logging()
    df_with_partition = df.withColumn("anomesdia", lit(partition_value))

    (
        df_with_partition.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("anomesdia")
        .parquet(output_path)
    )

    logger.info(
        "Dados particionados salvos: anomesdia=%s em %s",
        partition_value,
        output_path,
    )


def register_table(spark, database: str, table_name: str, output_path: str):
    logger = configure_logging()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    # Recria apenas os metadados para corrigir esquemas de execuções anteriores.
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}
        USING parquet
        LOCATION '{output_path}'
        """
    )
    spark.sql(f"MSCK REPAIR TABLE {database}.{table_name}")
    logger.info(
        "Tabela %s registrada com particoes detectadas automaticamente",
        table_name,
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
    bucket_name = args.get("BUCKET_NAME", "129264105405-lab")
    logger = configure_logging()

    logger.info("Inicializando Glue Job para a base State of Data Brazil 2024")
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    config = build_config(bucket_name)
    partition_value = get_partition_value()
    logger.info("Data de particao: %s", partition_value)

    for table_key, table_cfg in config.items():
        logger.info("Processando tabela: %s", table_key)

        df = read_csv(
            spark=spark,
            input_path=table_cfg["input_path"],
            separator=table_cfg["separator"],
            encoding=table_cfg["encoding"],
        )
        df = normalize_column_names(df, logger)

        logger.info("Registros lidos: %s", df.count())

        write_parquet_partitioned(
            spark=spark,
            df=df,
            output_path=table_cfg["output_path"],
            partition_value=partition_value,
        )
        logger.info(
            "Parquet gravado em %s (anomesdia=%s)",
            table_cfg["output_path"],
            partition_value,
        )

        register_table(
            spark=spark,
            database=table_cfg["database"],
            table_name=table_cfg["table_name"],
            output_path=table_cfg["output_path"],
        )
        logger.info("Tabela registrada no Glue Catalog: %s", table_cfg["table_name"])

    logger.info("Job concluido com sucesso.")
    job.commit()


if __name__ == "__main__":
    main()
