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

