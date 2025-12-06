-- Charger les données CSV
data = LOAD '/user/cloudera/input/new_test.csv'
       USING PigStorage(',')
       AS (age:int,
           job:int,
           marital:int,
           education:int,
           default_:int,
           housing:int,
           loan:int,
           contact:int,
           month:int,
           day_of_week:int,
           duration:int,
           campaign:int,
           y:int);

-- Supprimer l'en-tête ou lignes invalides
data_no_header = FILTER data BY age IS NOT NULL AND age > 0;

-- Calcul du score
scored = FOREACH data_no_header GENERATE
    (
        (age >= 25 AND age <= 40 ? 1 : 0)
      + (job >= 0 AND job <= 10 ? 1 : 0)
      + (housing == 1 ? 1 : 0)
      + (loan == 0 ? 1 : 0)
      + (marital == 0 OR marital == 1 ? 0.5 : 0)
    ) AS score,
    y;

-- Grouper par score
grouped = GROUP scored BY score;

--

