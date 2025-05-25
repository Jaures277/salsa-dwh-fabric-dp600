from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
from pyspark.sql.utils import AnalysisException

# Lecture et nettoyage du fichier danseurs
df_staging = spark.read.option("header", "true").option("sep", ";").option("encoding", "UTF-8").csv("Files/raw/danseurs.csv")

def clean_columns(df):
    for col_name in df.columns:
        new_col = col_name.strip().lower().replace(" ", "_").replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a")
        df = df.withColumnRenamed(col_name, new_col)
    return df

df_staging = clean_columns(df_staging)

# Ajouter les colonnes SCD
df_staging_clean = df_staging.withColumn("date_debut", current_timestamp()) \
    .withColumn("date_fin", lit(None).cast("timestamp")) \
    .withColumn("version", lit(1)) \
    .withColumn("is_actif", lit(True))

# Charger ou créer df_existing
try:
    df_existing = spark.table("dw_dim_danseur")
except AnalysisException:
    df_existing = spark.createDataFrame([], df_staging_clean.schema)

# Ajouter colonnes SCD manquantes si besoin
for col_name, dtype in [("date_debut", "timestamp"), ("date_fin", "timestamp"), ("version", "int"), ("is_actif", "boolean")]:
    if col_name not in df_existing.columns:
        df_existing = df_existing.withColumn(col_name, lit(None).cast(dtype))

# Clé et colonnes suivies
cle = "noclient"
champs_suivis = ["adresse", "cp", "ville", "pub"]

# Jointure pour détection
df_joined = df_staging_clean.alias("new").join(
    df_existing.alias("old").filter(col("old.is_actif") == True),
    on=cle,
    how="left"
)

# Condition de changement
condition_diff = " OR ".join([f"new.{c} != old.{c}" for c in champs_suivis])

# Séparation des cas
df_changed = df_joined.filter(condition_diff).select("new.*")
df_unchanged = df_joined.filter(f"NOT ({condition_diff}) AND old.{cle} IS NOT NULL").select("old.*")

# Fermer les versions précédentes → ne garder QUE les colonnes de df_existing
df_closed_old = df_existing.alias("old").join(df_changed.select(cle).distinct(), cle, "inner") \
    .filter(col("old.is_actif") == True) \
    .drop("date_fin", "is_actif") \
    .withColumn("date_fin", current_timestamp()) \
    .withColumn("is_actif", lit(False))

# Créer les nouvelles versions
df_new_versions = df_changed.join(
    df_existing.groupBy(cle).agg(spark_max("version").alias("max_version")),
    cle, "left"
).withColumn("version", col("max_version") + 1).drop("max_version")

# Union de toutes les lignes
df_final = df_existing.filter("is_actif = false") \
    .unionByName(df_unchanged, allowMissingColumns=True) \
    .unionByName(df_closed_old, allowMissingColumns=True) \
    .unionByName(df_new_versions, allowMissingColumns=True)

# Sauvegarde
df_final.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_danseur")

from pyspark.sql.functions import col, lit

# Charger les données sources
df_formules = spark.table("stg_formules")

# Nettoyer les colonnes (comme pour les autres)
def clean_columns(df):
    for col_name in df.columns:
        new_col = col_name.strip().lower().replace(" ", "_").replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a")
        df = df.withColumnRenamed(col_name, new_col)
    return df

df_formules = clean_columns(df_formules)

# SCD Type 1 = remplacement pur
df_formules.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_formule")
