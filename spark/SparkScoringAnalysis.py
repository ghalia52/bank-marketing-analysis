#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Analyse de Scoring Client avec Apache Spark
===========================================
Ce programme implémente la logique MapReduce en utilisant les RDD Spark
pour calculer le taux de conversion par score client.

Auteur: Projet Bank Marketing
Date: 2025-10-11
"""

from pyspark import SparkContext, SparkConf
import sys

def parse_line(line):
    """
    Parser pour transformer une ligne CSV en tuple (score, y)
    
    RDD Input: String (ligne brute du CSV)
    RDD Output: (float, int) ou None si erreur
    
    Pattern MapReduce: MAPPER - Phase de transformation
    """
    try:
        fields = line.strip().split(',')
        
        # Extraction des champs pertinents
        age = int(fields[0])
        job = int(fields[1])
        marital = int(fields[2])
        housing = int(fields[5])
        loan = int(fields[6])
        y = int(fields[12])
        
        # Calcul du score selon les critères métier
        score = 0.0
        
        # Critère 1: Âge optimal (25-40 ans)
        if 25 <= age <= 40:
            score += 1
        
        # Critère 2: Type de profession (tous acceptés dans version simplifiée)
        if 0 <= job <= 10:
            score += 1
        
        # Critère 3: Propriétaire (housing = 1)
        if housing == 1:
            score += 1
        
        # Critère 4: Pas de prêt (loan = 0)
        if loan == 0:
            score += 1
        
        # Critère 5: Statut marital (0 ou 1)
        if marital in [0, 1]:
            score += 0.5
        
        return (score, y)
    
    except (ValueError, IndexError):
        # Ignorer les lignes mal formées (header, lignes corrompues)
        return None


def aggregate_scores(y1, y2):
    """
    Fonction de combinaison pour agréger les statistiques
    
    RDD Input: (total_count, yes_count), (total_count, yes_count)
    RDD Output: (total_count, yes_count) agrégé
    
    Pattern MapReduce: COMBINER - Agrégation locale
    """
    return (y1[0] + y2[0], y1[1] + y2[1])


def calculate_rate(score_stats):
    """
    Calcule le taux de conversion à partir des statistiques
    
    RDD Input: (score, (total_count, yes_count))
    RDD Output: (score, total_count, yes_count, rate)
    
    Pattern MapReduce: Post-processing
    """
    score = score_stats[0]
    total_count = score_stats[1][0]
    yes_count = score_stats[1][1]
    rate = float(yes_count) / float(total_count) if total_count > 0 else 0.0
    
    return (score, total_count, yes_count, round(rate, 4))


def format_output(result):
    """
    Formate le résultat pour l'affichage/export
    
    RDD Input: (score, total_count, yes_count, rate)
    RDD Output: String formatée avec tabulations
    """
    return "%s\t%s\t%s\t%.4f" % (result[0], result[1], result[2], result[3])


def main():
    """
    Fonction principale du Job Spark
    
    Architecture RDD:
    -----------------
    RDD1 (textFile)      : String (lignes brutes)
    RDD2 (map)           : (score, y) - Données parsées
    RDD3 (filter)        : (score, y) - Données valides uniquement
    RDD4 (mapValues)     : (score, (1, y)) - Préparation pour agrégation
    RDD5 (reduceByKey)   : (score, (total, yes_count)) - Agrégation
    RDD6 (map)           : (score, total, yes_count, rate) - Calcul du taux
    RDD7 (sortByKey)     : (score, total, yes_count, rate) - Tri
    RDD8 (map)           : String formatée - Préparation export
    """
    
    # ===== CONFIGURATION SPARK =====
    conf = SparkConf().setAppName("Bank Marketing Scoring Analysis")
    sc = SparkContext(conf=conf)
    
    # Chemins d'entrée/sortie
    input_path = "/user/cloudera/input/new_test.csv"
    output_path = "/user/cloudera/output_spark"
    
    print("=" * 60)
    print("SPARK JOB: Bank Marketing Scoring Analysis")
    print("=" * 60)
    print("Input:  %s" % input_path)
    print("Output: %s" % output_path)
    print("=" * 60)
    
    # ===== PHASE 1: CHARGEMENT ET PARSING (MAP) =====
    print("\n[PHASE 1] Chargement et parsing des données...")
    
    # RDD1: Chargement du fichier CSV
    lines_rdd = sc.textFile(input_path)
    print("RDD1 (textFile): Lignes brutes chargées")
    
    # RDD2: Parser chaque ligne pour extraire (score, y)
    # Équivalent MapReduce: MAPPER
    parsed_rdd = lines_rdd.map(parse_line)
    print("RDD2 (map): Parsing avec fonction parse_line")
    
    # RDD3: Filtrer les lignes invalides (None)
    valid_rdd = parsed_rdd.filter(lambda x: x is not None)
    print("RDD3 (filter): Suppression des lignes invalides")
    
    # ===== PHASE 2: TRANSFORMATION POUR AGRÉGATION (MAP) =====
    print("\n[PHASE 2] Préparation pour l'agrégation...")
    
    # RDD4: Transformer (score, y) en (score, (1, y))
    # 1 = compteur pour le total, y = pour compter les "yes"
    # Équivalent MapReduce: Émission des paires clé-valeur
    prepared_rdd = valid_rdd.mapValues(lambda y: (1, y))
    print("RDD4 (mapValues): Format (score, (1, y))")
    
    # ===== PHASE 3: AGRÉGATION PAR SCORE (REDUCE) =====
    print("\n[PHASE 3] Agrégation par score...")
    
    # RDD5: Agréger par score pour obtenir (score, (total, yes_count))
    # Équivalent MapReduce: REDUCER avec COMBINER
    aggregated_rdd = prepared_rdd.reduceByKey(aggregate_scores)
    print("RDD5 (reduceByKey): Agrégation avec fonction aggregate_scores")
    
    # ===== PHASE 4: CALCUL DU TAUX ET TRI =====
    print("\n[PHASE 4] Calcul du taux de conversion...")
    
    # RDD6: Calculer le taux de conversion
    result_rdd = aggregated_rdd.map(calculate_rate)
    print("RDD6 (map): Calcul du taux avec fonction calculate_rate")
    
    # RDD7: Trier par score
    sorted_rdd = result_rdd.sortBy(lambda x: x[0])
    print("RDD7 (sortBy): Tri par score croissant")
    
    # ===== PHASE 5: FORMATAGE ET EXPORT =====
    print("\n[PHASE 5] Formatage et export des résultats...")
    
    # RDD8: Formater pour export
    formatted_rdd = sorted_rdd.map(format_output)
    print("RDD8 (map): Formatage avec fonction format_output")
    
    # Sauvegarder dans HDFS
    formatted_rdd.saveAsTextFile(output_path)
    print("\nResultats sauvegardes dans: %s" % output_path)
    
    # ===== AFFICHAGE DES RÉSULTATS =====
    print("\n" + "=" * 60)
    print("RESULTATS")
    print("=" * 60)
    print("Score\tTotal\tYes\tTaux")
    print("-" * 60)
    
    results = sorted_rdd.collect()
    for score, total, yes_count, rate in results:
        print("%s\t%s\t%s\t%.4f" % (score, total, yes_count, rate))
    
    print("=" * 60)
    print("Job Spark termine avec succes!")
    print("=" * 60)
    
    # Fermer le contexte Spark
    sc.stop()


if __name__ == "__main__":
    main()
