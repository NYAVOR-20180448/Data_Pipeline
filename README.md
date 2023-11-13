# Data_Pipeline
Vous êtes en relation avec une compagnie aérienne.
Vous avez pour mission d’effectuer une analyse de leur donnée de vols, ainsi que concevoir un modèle de données qui vous permet de déterminer si un vol aura du retard ou pas. 
Un exemple de données a été fourni pour un POC. Vous devez préparer les données disponibles publiquement et les exploiter afin de gagner l’appel d‘offre :
Apache Spark : Préparer les données (ingestion des données, nettoyage des données, suppression des doublons, valeurs aberrantes, jointure/croisement des sources de données, changement de type, nom des colonnes, exploitation de données ML etc …)
Apache Airflow : Orchestrer et automatiser les phases de collecte et de préparation des données

Les étapes à réaliser à minima : Créer un flux permettant de
1. Ingérer les données en tant que pyspark dataframe object
2. Effectuer des analyses des données tout en utilisant les opérations à disposition
3. Evaluer la qualité des données et la préparation nécessaires pour qu’elles soient le mieux exploitables (ex : valeurs manquantes, valeurs dupliquées.)
4. Concevoir un modèle de Machine Learning, en utilisant sparkML qui vous permettra de prédire si un vol a eu du retard ou pas (entrainement + Test)
5. Orchestrer tout le pipeline de données, de l’ingestion à l’exploitation, en utilisant apache airflow.


# Nom du Projet

Description brève du projet.

## Aperçu

Le projet vise à [décrire brièvement l'objectif du projet].

## Dataset

Le jeu de données utilisé est [nom du jeu de données]. Il contient des informations sur les vols, notamment les colonnes [liste des colonnes].

## Prétraitement des Données

Avant d'utiliser les données pour l'analyse ou la modélisation, plusieurs étapes de prétraitement ont été effectuées, notamment :

- Suppression des doublons
- Traitement des valeurs manquantes
- Conversion des types de données
- Création de la cible (colonne 'is_delayed')
- Encodage des variables catégorielles
- ...

## Modélisation

Dans ce projet, nous avons utilisé PySpark MLlib pour créer un modèle de prédiction basé sur la régression logistique. Les étapes comprennent :

1. Indexation des variables catégorielles
2. Assemblage des fonctionnalités
3. Création d'un modèle de régression logistique
4. Entraînement du modèle sur le jeu d'entraînement
5. Évaluation du modèle sur le jeu de test

D'autres modèles ou algorithmes peuvent être explorés en fonction des besoins spécifiques du projet.

## Évaluation du Modèle

L'évaluation du modèle a été réalisée en utilisant l'Aire sous la courbe ROC (Area Under ROC) comme métrique. Le modèle a donné une performance de [valeur de performance].

## Exécution du Code

Pour exécuter le code, assurez-vous d'avoir installé les dépendances nécessaires (voir le fichier `requirements.txt`). Vous pouvez exécuter le script principal avec la commande suivante :

```bash
python main.py
