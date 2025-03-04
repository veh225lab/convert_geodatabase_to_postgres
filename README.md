## Convertir la Geodatabase en PostgreSQL

## Pour démarrer le projet

####  Création du fichier d'environnement
```
python3 -m venv .venv
```

#### Activer l'environnement
```
source .venv/bin/activate
```

#### Installer les dépendances du projet
```
pip install -r requirements.txt
```


#### Renseigner les différents paramètres dans le fichier(convert_geodatabase_to_postgres.py) 
 
 ***nom_de_la_geodatabase.gdb***: nom de la geodatabase

  ***nom_db_postgres***: Nom de la base de données

  ***utilisateur***: Nom de l'utilisateur de la base de données

  ***mot_passe***: Mot de passe de la base de données

  ***localhost***: Hôte sur lequel est installée la base de données


#### Lancez la commande de conversion
```
python convert_geodatabase_to_postgres.py
```
ou Python 3 si vous avez installé Python 3
```
python3 convert_geodatabase_to_postgres.py
```

