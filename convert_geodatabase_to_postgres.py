import fiona
import psycopg2
from shapely.geometry import shape

# Connexion à la .gdb
gdb_path = "nom_de_la_geodatabase.gdb"
layers = fiona.listlayers(gdb_path)

# Connexion à PostgreSQL
conn = psycopg2.connect("dbname=nom_db_postgres user=utilisateur password=mot_passe host=localhost")
cur = conn.cursor()

for layer in layers:
    with fiona.open(gdb_path, layer=layer) as src:

        attributs = src.schema["properties"]

        table_name = layer.lower()

        cur.execute(f"DROP TABLE IF EXISTS {table_name};")

        columns = []

        has_id = "id" in attributs

        # Ajouter la colonne 'id' si elle n'existe pas
        if not has_id:
            columns.append("id SERIAL PRIMARY KEY")

        for attr_name, attr_type in attributs.items():
            # Mapper les types de données Fiona vers PostgreSQL
            if attr_name == "id" and has_id:
                continue
            if attr_type == "int":
                pg_type = "INTEGER"
            elif attr_type == "float":
                pg_type = "FLOAT"
            elif attr_type == "str":
                pg_type = "VARCHAR"
            elif attr_type == "bool":
                pg_type = "BOOLEAN"
            elif attr_type == "date":
                pg_type = "DATE"
            elif attr_type == "datetime":
                pg_type = "TIMESTAMP"
            elif attr_type in (list, dict):
                pg_type = "JSONB"
            else:
                pg_type = "VARCHAR"
            columns.append(f"{attr_name} {pg_type}")

        # Ajouter une colonne pour la géométrie (en tant que POINT)
        columns.append("geom GEOMETRY(POINT, 4326)")

        # Créer la table
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cur.execute(create_table_query)

        for feature in src:
            geometry = feature["geometry"]
            properties = feature["properties"]

            shapely_geom = shape(geometry)

            if shapely_geom.geom_type == "Point":
                point = shapely_geom
            else:
                point = shapely_geom.centroid

            # Convertir le point en WKT
            wkt = point.wkt

            col_names = list(properties.keys()) + ["geom"]
            col_values = list(properties.values()) + [wkt]

            if not has_id:
                col_names = [name for name in col_names if name != "id"]
                col_values = [
                    value for name, value in zip(col_names, col_values) if name != "id"
                ]

            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(col_names)})
                VALUES ({', '.join(['%s'] * len(col_values))});
            """
            cur.execute(insert_query, col_values)

conn.commit()
cur.close()
conn.close()
print(f"✅ Uploaded database")
