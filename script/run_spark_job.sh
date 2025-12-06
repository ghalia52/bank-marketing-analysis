#!/bin/bash
# ============================================
# Script d'exécution du Job Spark
# ============================================

echo "=========================================="
echo "  SPARK JOB: Bank Marketing Analysis"
echo "=========================================="

# Variables de configuration
SPARK_APP="SparkScoringAnalysis.py"
INPUT_PATH="/user/cloudera/input/new_test.csv"
OUTPUT_PATH="/user/cloudera/output_spark"

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo -e "${YELLOW}[1/4] Vérification du fichier d'entrée...${NC}"
if hdfs dfs -test -e $INPUT_PATH; then
    echo -e "${GREEN}✓ Fichier trouvé: $INPUT_PATH${NC}"
    LINES=$(hdfs dfs -cat $INPUT_PATH | wc -l)
    echo "  Nombre de lignes: $LINES"
else
    echo -e "${RED}✗ Erreur: Fichier introuvable: $INPUT_PATH${NC}"
    exit 1
fi

echo ""
echo -e "${YELLOW}[2/4] Nettoyage du répertoire de sortie...${NC}"
if hdfs dfs -test -e $OUTPUT_PATH; then
    echo "  Suppression de l'ancien output..."
    hdfs dfs -rm -r $OUTPUT_PATH
    echo -e "${GREEN}✓ Ancien output supprimé${NC}"
else
    echo "  Pas d'ancien output à supprimer"
fi

echo ""
echo -e "${YELLOW}[3/4] Lancement du Job Spark...${NC}"
echo "  Application: $SPARK_APP"
echo "  Mode: YARN"
echo ""
echo "----------------------------------------"

# Exécution du job Spark
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 2 \
    --num-executors 2 \
    $SPARK_APP

# Vérifier le code de retour
if [ $? -eq 0 ]; then
    echo ""
    echo "----------------------------------------"
    echo -e "${GREEN}✓ Job Spark terminé avec succès!${NC}"
    
    echo ""
    echo -e "${YELLOW}[4/4] Affichage des résultats...${NC}"
    echo ""
    echo "Score  Total  Yes    Rate"
    echo "-----------------------------"
    hdfs dfs -cat $OUTPUT_PATH/part-* | head -20
    
    echo ""
    echo "=========================================="
    echo -e "${GREEN}✓ Analyse terminée avec succès!${NC}"
    echo "=========================================="
    echo ""
    echo "Résultats complets disponibles dans:"
    echo "  HDFS: $OUTPUT_PATH"
    echo ""
    echo "Pour voir tous les résultats:"
    echo "  hdfs dfs -cat $OUTPUT_PATH/part-*"
    echo ""
    
else
    echo ""
    echo "----------------------------------------"
    echo -e "${RED}✗ Erreur lors de l'exécution du job Spark${NC}"
    echo "----------------------------------------"
    echo ""
    echo "Consultez les logs pour plus de détails:"
    echo "  yarn logs -applicationId <app_id>"
    exit 1
fi
