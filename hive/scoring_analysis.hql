-- ============================================
-- Script Hive : Analyse de scoring clients
-- ============================================

-- Étape 1 : Créer la table pour charger les données brutes
DROP TABLE IF EXISTS bank_data;

CREATE TABLE bank_data (
    age INT,
    job INT,
    marital INT,
    education INT,
    default_ INT,
    housing INT,
    loan INT,
    contact INT,
    month INT,
    day_of_week INT,
    duration INT,
    campaign INT,
    y INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Étape 2 : Charger les données depuis HDFS
LOAD DATA INPATH '/user/cloudera/input/new_test.csv' 
INTO TABLE bank_data;

-- Étape 3 : Créer une table avec le score calculé
DROP TABLE IF EXISTS bank_scored;

CREATE TABLE bank_scored AS
SELECT 
    age,
    job,
    marital,
    housing,
    loan,
    y,
    -- Calcul du score selon les critères
    (CASE WHEN age >= 25 AND age <= 40 THEN 1 ELSE 0 END +
     CASE WHEN job >= 0 AND job <= 10 THEN 1 ELSE 0 END +
     CASE WHEN housing = 1 THEN 1 ELSE 0 END +
     CASE WHEN loan = 0 THEN 1 ELSE 0 END +
     CASE WHEN marital IN (0, 1) THEN 0.5 ELSE 0 END) AS score
FROM bank_data
WHERE age IS NOT NULL AND age > 0;

-- Étape 4 : Calculer les statistiques par score
DROP TABLE IF EXISTS bank_score_stats;

CREATE TABLE bank_score_stats AS
SELECT 
    score,
    COUNT(*) AS total_clients,
    SUM(y) AS yes_count,
    ROUND(SUM(y) / COUNT(*), 4) AS conversion_rate
FROM bank_scored
GROUP BY score
ORDER BY score;

-- Étape 5 : Afficher les résultats
SELECT 
    score AS Score,
    total_clients AS Total_Clients,
    yes_count AS Yes_Count,
    conversion_rate AS Conversion_Rate
FROM bank_score_stats
ORDER BY score;

-- Optionnel : Exporter les résultats vers HDFS
INSERT OVERWRITE DIRECTORY '/user/cloudera/output_hive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT score, total_clients, yes_count, conversion_rate
FROM bank_score_stats
ORDER BY score;
