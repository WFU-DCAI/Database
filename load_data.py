import os
import pandas as pd
import sqlite3

#Script to load data from the csv into sqlite database

# load CSV
df = pd.read_csv('data/2015_NY_Tree_Census.csv')

# create SQLite connection
conn = sqlite3.connect('NY_Tree.db')
cur = conn.cursor()

#delete all existing rows from the tables in the database
# order matters: delete child tables first
tables_in_delete_order = [
    "tree_problems",
    "annotator_notes",

    "trees",
    "tree_issues",
    "location",

    "species",
    "problems",
    "steward_levels",
    "guard_types",
    "collector_types",
    "statuses",
    "health_levels",

    "annotators"
]

for table in tables_in_delete_order:
    cur.execute(f"DELETE FROM {table};")

print("existing data deleted")

# convert empty strings to NULL (used for all tables)
def empty_to_null(df):
    return df.replace('', pd.NA)

# species
species_df = (
    df[['spc_latin', 'spc_common']]
    .drop_duplicates()
    .reset_index(drop=True)
)
species_df = empty_to_null(species_df)

species_df.to_sql(
    'species',
    conn,
    if_exists='append',
    index=False
)

print("\n species data added")

# problems
problems_df = (
    df[['problems']]
    .drop_duplicates()
    .reset_index(drop=True)
)

#split and explose by commas
problems_df = problems_df.assign(
    problems=problems_df['problems'].str.split(',')
).explode('problems').reset_index(drop=True)

problems_df = problems_df.drop_duplicates().reset_index(drop=True)
problems_df = empty_to_null(problems_df)

problems_df.to_sql(
    'problems',
    conn,
    if_exists='append',
    index=False
)

print("\n problems data added.")

# steward_levels
steward_levels_df = (
    df[['steward']]
    .drop_duplicates()
    .reset_index(drop=True)
)
steward_levels_df = empty_to_null(steward_levels_df)

steward_levels_df.to_sql(
    'steward_levels',
    conn,
    if_exists='append',
    index=False
)

print("\n steward data added")

# guard_types
guard_types_df = (
    df[['guards']]
    .drop_duplicates()
    .reset_index(drop=True)
)
guard_types_df = empty_to_null(guard_types_df)

guard_types_df.to_sql(
    'guard_types',
    conn,
    if_exists='append',
    index=False
)

print("\n guard data added")

# collector_types
collector_types_df = (
    df[['user_type']]
    .drop_duplicates()
    .reset_index(drop=True)
)
collector_types_df = empty_to_null(collector_types_df)

collector_types_df.to_sql(
    'collector_types',
    conn,
    if_exists='append',
    index=False
)

print("\n collector data added")

# statuses
statuses_df = (
    df[['status']]
    .drop_duplicates()
    .reset_index(drop=True)
)
statuses_df = empty_to_null(statuses_df)

statuses_df.to_sql(
    'statuses',
    conn,
    if_exists='append',
    index=False
)

print("\n status data added")

# health_levels
health_levels_df = (
    df[['health']]
    .drop_duplicates()
    .reset_index(drop=True)
)
health_levels_df = empty_to_null(health_levels_df)

health_levels_df.to_sql(
    'health_levels',
    conn,
    if_exists='append',
    index=False
)

print("\n health data added")

# location
location_df = (
    df[['address', 'zipcode', 'zip_city', 'latitude', 'longitude', 'x_sp', 'y_sp',
        'cb_num', 'borocode', 'boroname', 'cncldist', 'st_assem', 'st_senate',
        'census_tract', 'nta', 'nta_name', 'boro_ct', 'state', 'bin', 'bbl']]
    .drop_duplicates()
    .reset_index(drop=True)
)
location_df = empty_to_null(location_df)

location_df.to_sql(
    'location',
    conn,
    if_exists='append',
    index=False
)

print("\n location data added")

# build Lookup mappings
def build_lookup(table, key_col, id_col):
    q = f"SELECT {id_col}, {key_col} FROM {table}"
    rows = pd.read_sql_query(q, conn)
    return dict(zip(rows[key_col], rows[id_col]))

species_map_lat = build_lookup("species", "spc_latin", "species_id")
species_map_com = build_lookup("species", "spc_common", "species_id")
problems_map = build_lookup("problems", "problems", "problem_id")
steward_map = build_lookup("steward_levels", "steward", "steward_id")
guard_map = build_lookup("guard_types", "guards", "guard_id")
collector_map = build_lookup("collector_types", "user_type", "collector_id")
status_map = build_lookup("statuses", "status", "status_id")
health_map = build_lookup("health_levels", "health", "health_id")

location_q = "SELECT * FROM location"
location_df_db = pd.read_sql_query(location_q, conn)
location_key_cols = [
    'address','zipcode','zip_city','latitude','longitude','x_sp','y_sp',
    'cb_num','borocode','boroname','cncldist','st_assem','st_senate',
    'census_tract','nta','nta_name','boro_ct','state','bin','bbl'
]
location_map = {
    tuple(row[col] for col in location_key_cols): row["location_id"]
    for _, row in location_df_db.iterrows()
}

#trees dataframe for final insert

trees_df = df.copy()
trees_df = empty_to_null(trees_df)

# map lookup values to foreign keys
trees_df["species_id"] = trees_df["spc_latin"].map(species_map_lat)
trees_df["status_id"] = trees_df["status"].map(status_map)
trees_df["health_id"] = trees_df["health"].map(health_map)
trees_df["steward_id"] = trees_df["steward"].map(steward_map)
trees_df["guard_id"] = trees_df["guards"].map(guard_map)
trees_df["collector_id"] = trees_df["user_type"].map(collector_map)

# insert tree_issues and capture issue_id per row
issue_cols = [
    'root_stone','root_grate','root_other','trunk_wire','trnk_light',
    'trnk_other','brch_light','brch_shoe','brch_other'
]

# convert Yes/No → 1/0 for trees_df
for col in issue_cols:
    trees_df[col] = trees_df[col].map({'Yes': 1, 'No': 0})

issues_unique = (
    trees_df[issue_cols]
    .drop_duplicates()
    .reset_index(drop=True)
)

# insert and retrieve issue_id list
issues_unique.to_sql("tree_issues", conn, if_exists="append", index=False)

# retrieve inserted issue_ids in same order
issue_ids_db = pd.read_sql_query(
    "SELECT issue_id, root_stone, root_grate, root_other, trunk_wire, "
    "trnk_light, trnk_other, brch_light, brch_shoe, brch_other FROM tree_issues",
    conn
)

issue_map = {
    tuple(row[col] for col in issue_cols): row["issue_id"]
    for _, row in issue_ids_db.iterrows()
}

trees_df["issue_id"] = trees_df.apply(
    lambda r: issue_map[tuple(r[col] for col in issue_cols)], axis=1
)

#map location recoreds to location_id
def get_location_key(row):
    return tuple(row[col] for col in location_key_cols)

trees_df["location_id"] = trees_df.apply(get_location_key, axis=1).map(location_map)

#insert into trees table
tree_cols_final = [
    "tree_id", "block_id", "created_at", "tree_dbh", "stump_diam",
    "curb_loc", "sidewalk",
    "status_id", "health_id", "species_id",
    "steward_id", "guard_id", "collector_id",
    "issue_id", "location_id"
]

tree_records = trees_df[tree_cols_final].drop_duplicates().values.tolist()

cur.executemany(
    f"""
    INSERT OR IGNORE INTO trees (
        {','.join(tree_cols_final)}
    ) VALUES ({','.join(['?'] * len(tree_cols_final))})
    """,
    tree_records
)

#insert into tree_problems table (many to many)
problem_pairs = []

for idx, row in df.iterrows():
    if pd.isna(row["problems"]) or row["problems"] == "":
        continue
    prob_list = [p.strip() for p in row["problems"].split(",")]
    for p in prob_list:
        if p in problems_map:
            problem_pairs.append((row["tree_id"], problems_map[p]))

problem_pairs = list(set(problem_pairs))  # remove duplicates

cur.executemany(
    "INSERT OR IGNORE INTO tree_problems (tree_id, problem_id) VALUES (?, ?)",
    problem_pairs
)

print("\n tree data added")

# for each annotator csvs in annotators_statuses dir
annotator_statuses_dir = 'data/annotators_statuses'
for filename in os.listdir(annotator_statuses_dir):
    #insert annotator names from separate csv into annotator table

    # annotator_df = pd.read_csv("./data/curators_status.csv")
    print(f'    \nannotator data with {filename}')

    file_path = os.path.join(annotator_statuses_dir, filename)
    annotator_df = pd.read_csv(file_path)
    print(f'annotator status csv read and shape {annotator_df.shape}')

    annotators = (
        annotator_df[['annotator_name']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    annotators = empty_to_null(annotators)

    #insert annotators into annotator_table
    annotators.to_sql(
        'annotators',
        conn,
        if_exists='append',
        index=False
    )

    print("\n annotator data inserted")

    #insert annotator instances into the annotator_notes table, mapping annotator_id to annotator_name
    annotator_map = build_lookup("annotators", "annotator_name", "annotator_id")

    annotator_df2 = annotator_df.copy()
    annotator_df2["annotator_id"] = annotator_df2["annotator_name"].map(annotator_map)

    notes_records = annotator_df2[['annotator_id', 'tree_id', 'annotator_status']].dropna()

    notes_records.to_sql(
        'annotator_notes',
        conn, 
        if_exists='append',
        index=False
    )

conn.commit()
conn.close()

