import fiona
import psycopg2
from shapely.geometry import shape

# Connexion à la .gdb
gdb_path = 'nom_de_la_geodatabase.gdb'
layers = fiona.listlayers(gdb_path)

# Connexion à PostgreSQL
conn = psycopg2.connect("dbname=nom_db_postgres user=utilisateur password=mot_passe host=localhost")
cur = conn.cursor()

for layer in layers:
    with fiona.open(gdb_path, layer=layer) as src:
        # Récupérer les informations sur les attributs
        attributs = src.schema['properties']  # Dictionnaire des attributs et leurs types

        # Créer la table PostgreSQL
        table_name = layer.lower()  # Nom de la table (en minuscules pour éviter les problèmes)

        # Supprimer la table si elle existe déjà
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        # Créer la table avec les colonnes correspondant aux attributs
        columns = []

        # Vérifier si la colonne 'id' existe dans les attributs
        has_id = 'id' in attributs

        # Ajouter la colonne 'id' si elle n'existe pas
        if not has_id:
            columns.append("id SERIAL PRIMARY KEY")

        for attr_name, attr_type in attributs.items():
            # Mapper les types de données Fiona vers PostgreSQL
            if attr_name == 'id' and has_id:
                continue
            if attr_type == 'int':
                pg_type = 'INTEGER'
            elif attr_type == 'float':
                pg_type = 'FLOAT'
            elif attr_type == 'str':
                pg_type = 'VARCHAR'
            else:
                pg_type = 'VARCHAR'
            columns.append(f"{attr_name} {pg_type}")

        # Ajouter une colonne pour la géométrie
        columns.append("geom GEOMETRY")

        # Créer la table
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cur.execute(create_table_query)

        # Insérer les données
        for feature in src:
            geometry = feature['geometry']
            properties = feature['properties']

            # Convertir la géométrie GeoJSON en WKT
            shapely_geom = shape(geometry)  # Convertir en objet Shapely
            wkt = shapely_geom.wkt  # Convertir en WKT

            # Préparer les noms des colonnes et les valeurs
            col_names = list(properties.keys()) + ['geom']
            col_values = list(properties.values()) + [wkt]

            # Si 'id' n'existe pas, ne pas l'inclure dans la requête d'insertion
            if not has_id:
                col_names = [name for name in col_names if name != 'id']
                col_values = [value for name, value in zip(col_names, col_values) if name != 'id']

            # Générer la requête d'insertion
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(col_names)})
                VALUES ({', '.join(['%s'] * len(col_values))});
            """
            cur.execute(insert_query, col_values)

# Valider les changements et fermer la connexion
conn.commit()
cur.close()
conn.close()
print(f"✅ Uploaded database")