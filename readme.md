# snatcher

Snatcher est un script python basique permettant de télécharger une ressource issue d'un site opendata gouvernemental et de l'injecter directement dans une base postgresql.

Snatcher intègre les fonctions suivantes:
- paramétrage par fichier .ini
- lecture des formats compatibles avec geopandas/pyogrio
- gestion automatique des versions dans postgres
- automatisation cron

# paramétrage

renommer snatcher_template.ini en snatcher.ini et compléter les paramètres:
- `host`: ip du serveur
- `database`: nom de la base de données
- `user`: utilisateur (sera propriétaire de la donnée, doit posséder des droits d'édition)
- `password`: mot de passe de l'utilisateur
- `url`: url complère de la ressource à injecter

# formats compatibles

le moteur d'importation utilisé est geopandas/pyogrio: tous les formats ORG vecteur sont supportés. Voir ici pour une liste complète: https://gdal.org/en/latest/drivers/vector/index.html

# automatisation cron

Pour un import hebdomadaire (le mardi à 01h00) utiliser la liste suivante dans le cron utilisateur (non root):

`0 1 * * 2 /local/path/to/bin/mamba run -n mamba_env python /local/path/to/snatcher.py > /local/path/to/snatcher_log.txt 2>&1`

`/local/path/to/bin/mamba run -n mamba_env python` permet de lancer le script dans un environnement mamba déterminé (`mamba_env`)

`/local/path/to/snatcher_log.txt` est un fichier log qui documente l'exécuton du script

# gestion automatisée de l'historique

Les tables stockées dans la base sont nommées avec un timestamp.
- pour le mois en cours, toutes les tables sont conservées (timestamp de format `YYYYmmdd`)
- pour le mois précédent de l'année en cours, seule la dernière table est conservée (timestamp de format `YYYYmm`)
- pour l'année précédente, seule la dernière table de l'année est concernée, elle devient la table de référence du millésime (timestamp au format `YYYY`)

Par ailleur une vue matérialisée "latest" est constamment branchée à la table la plus récente
