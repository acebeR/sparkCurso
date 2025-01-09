from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

# Inicializando a sessão do Spark
spark = SparkSession.builder.appName("Processa CSV dados viagem").getOrCreate()

# Carregando o DataFrame (ajuste o caminho do arquivo conforme necessário)
df = spark.read.csv("seu_arquivo.csv", header=True, inferSchema=True)

# Função para converter vírgula para ponto e transformar em float
def to_value(v):
    return float(v.replace(",", ".")) if v else None

# Registrando a função como UDF (User Defined Function) no Spark
udf_to_value = udf(to_value, FloatType())

# Limpando os nomes das colunas, removendo espaços e caracteres não-ASCII
clean_columns = [c.strip().replaceAll(r"[^\\x00-\\x7F]", "") for c in df.columns]

# Renomeando as colunas do DataFrame
df_clean = df.toDF(*clean_columns)

# Renomeando colunas específicas (mapeamento de nomes)
df_renamed = df_clean.withColumnRenamed("C?digo da Emenda", "Codigo_Emenda") \
                     .withColumnRenamed("Valor_Pago", "Valor Pago")

# Aplicando a UDF para transformar a coluna 'Valor_Pago' em um valor float com ponto no lugar da vírgula
df2 = df_renamed.withColumn("Valor Pago", udf_to_value(col("Valor Pago")))

# Exibindo o schema e as primeiras linhas do DataFrame
df2.printSchema()
df2.show()

# Selecionando colunas específicas
df_selected = df2.select("Nome_Funcao", "Valor Pago")
df_selected.show()
