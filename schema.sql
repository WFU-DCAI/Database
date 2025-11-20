PRAGMA foreign_keys = ON;

CREATE TABLE species (
    species_id INTEGER PRIMARY KEY AUTOINCREMENT,
    spc_latin TEXT UNIQUE,
    spc_common TEXT
);

CREATE TABLE problems (
    problem_id INTEGER PRIMARY KEY AUTOINCREMENT,
    problems TEXT UNIQUE
);

CREATE TABLE steward_levels (
    steward_id INTEGER PRIMARY KEY AUTOINCREMENT,
    steward TEXT UNIQUE
);

CREATE TABLE guard_types (
    guard_id INTEGER PRIMARY KEY AUTOINCREMENT,
    guards TEXT UNIQUE
);

CREATE TABLE collector_types (
    collector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_type TEXT UNIQUE
);

CREATE TABLE statuses (
    status_id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT UNIQUE
);

CREATE TABLE health_levels (
    health_id INTEGER PRIMARY KEY AUTOINCREMENT,
    health TEXT UNIQUE
);

CREATE TABLE location (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,

    address TEXT,
    zipcode INTEGER,
    zip_city TEXT,

    latitude REAL,
    longitude REAL,
    x_sp REAL,
    y_sp REAL,

    cb_num INTEGER,
    borocode INTEGER,
    boroname TEXT,

    cncldist INTEGER,
    st_assem INTEGER,
    st_senate INTEGER,
    census_tract REAL,

    nta TEXT,
    nta_name TEXT,
    boro_ct INTEGER,

    state TEXT DEFAULT 'New York',

    bin REAL,
    bbl REAL
);

--originaly "Yes/No" columns converted to integer 1/0 for easier analysis
--each tree has one issues record
CREATE TABLE tree_issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,

    root_stone INTEGER, 
    root_grate INTEGER,
    root_other INTEGER,

    trunk_wire INTEGER,
    trnk_light INTEGER,
    trnk_other INTEGER,

    brch_light INTEGER,
    brch_shoe INTEGER,
    brch_other INTEGER
);

CREATE TABLE trees (
    tree_id INTEGER PRIMARY KEY,
    block_id INTEGER, 
    created_at TEXT ,

    tree_dbh INTEGER,
    stump_diam INTEGER,

    curb_loc TEXT,
    sidewalk TEXT,

    status_id INTEGER REFERENCES statuses(status_id),
    health_id INTEGER REFERENCES health_levels(health_id),

    species_id INTEGER REFERENCES species(species_id),
    steward_id INTEGER REFERENCES steward_levels(steward_id),
    guard_id INTEGER REFERENCES guard_types(guard_id),
    collector_id INTEGER REFERENCES collector_types(collector_id),

    issue_id INTEGER REFERENCES tree_issues(issue_id),
    location_id INTEGER REFERENCES location(location_id)
);

CREATE TABLE tree_problems (
    tree_id INTEGER REFERENCES trees(tree_id),
    problem_id INTEGER REFERENCES problems(problem_id),
    PRIMARY KEY (tree_id, problem_id)
);





