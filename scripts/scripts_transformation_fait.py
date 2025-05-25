from pyspark.sql.functions import col, when, expr

# Chargement des tables staging
df_inscriptions = spark.table("stg_inscriptions")
df_tarifs = spark.table("stg_tarifs")
df_formules = spark.table("stg_formules")
df_durees = spark.table("stg_durees")
df_danses = spark.table("stg_danses")
df_danseurs = spark.table("stg_danseurs")

# Création des dimensions brutes
df_danses.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_danse")
df_formules.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_formule")
df_durees.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_duree")
df_danseurs.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_danseur")

# Enrichissement des tarifs
df_tarifs_enrichis = df_tarifs \
    .join(df_formules, "codeformule", "left") \
    .join(df_durees, df_tarifs["codeduree"] == df_durees["code_duree"], "left") \
    .join(df_danses, "codedanse", "left")

# Sauvegarde des tarifs enrichis
df_tarifs_enrichis.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_tarif")

# Enrichissement des inscriptions avec les tarifs
df = df_inscriptions.join(df_tarifs_enrichis, "idtarif", "left")

# Nettoyage et typage
df = df.withColumn("nbcourslimite", col("nbcourslimite").cast("int")) \
       .withColumn("nb_semaines", col("nb_semaines").cast("int")) \
       .withColumn("tarif", col("tarif").cast("double")) \
       .withColumn("dateinscription", col("dateinscription").cast("date"))

# Détermination du type de formule
df = df.withColumn(
    "typeformule",
    when(col("libelleformule").rlike("(?i)carnet"), "Carnet")
    .when(col("libelleformule").rlike("(?i)volonte"), "Hebdo")
    .otherwise("Hebdo")
)

# Calcul du nombre de séances
df = df.withColumn(
    "nbseances",
    when(col("typeformule") == "Carnet", col("nbcourslimite"))
    .when(col("libelleformule").rlike("(?i)volonte"), col("nb_semaines") * 5)
    .otherwise(col("nb_semaines") * col("nbcourslimite"))
)

# Calcul du chiffre d'affaires et des colonnes temporelles
df = df.withColumn("ca", col("tarif")) \
       .withColumn("mois", expr("month(dateinscription)")) \
       .withColumn("annee", expr("year(dateinscription)")) \
       .withColumn("tarifparseance", (col("tarif") / col("nbseances")).cast("double"))

# Sauvegarde dans la table de faits
df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_fait_inscriptions")


from pyspark.sql.functions import col, expr, sequence, explode, to_date, date_format, dayofweek

# Définir la plage de dates
start_date = "2024-08-01"
end_date = "2025-07-31"

# Créer une séquence de dates
df_dates = spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
""")

# Ajout des colonnes utiles
df_dates = df_dates.withColumn("jour", expr("day(date)")) \
                   .withColumn("mois", expr("month(date)")) \
                   .withColumn("annee", expr("year(date)")) \
                   .withColumn("semaine", expr("weekofyear(date)")) \
                   .withColumn("jour_semaine", date_format(col("date"), "EEEE")) \
                   .withColumn("mois_nom", date_format(col("date"), "MMMM")) \
                   .withColumn("trimestre", expr("quarter(date)")) \
                   .withColumn("annee_mois", date_format(col("date"), "yyyy-MM")) \
                   .withColumn("jour_semaine_num", dayofweek(col("date"))) \
                   .withColumn("est_weekend", expr("case when dayofweek(date) in (1, 7) then true else false end"))

# Sauvegarde dans la zone Gold
df_dates.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("dw_dim_date")
