-- spectra_db canonical schema (DuckDB)

CREATE TABLE IF NOT EXISTS meta_info (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refs (
  ref_id TEXT PRIMARY KEY,
  citation TEXT,
  doi TEXT,
  url TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS species (
  species_id TEXT PRIMARY KEY,
  formula TEXT NOT NULL,
  name TEXT,
  charge INTEGER DEFAULT 0,
  multiplicity INTEGER,
  inchi_key TEXT,
  tags TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS isotopologues (
  iso_id TEXT PRIMARY KEY,
  species_id TEXT NOT NULL REFERENCES species(species_id),
  label TEXT,
  composition_json TEXT,
  nuclear_spins_json TEXT,
  mass_amu DOUBLE,
  abundance DOUBLE,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS states (
  state_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL REFERENCES isotopologues(iso_id),
  state_type TEXT NOT NULL, -- "atomic" or "molecular"

  -- human-readable label
  electronic_label TEXT,

  -- future-proof structured storage
  vibrational_json TEXT,
  rotational_json TEXT,
  parity TEXT,

  -- physics-ready fields (atomic now; later can be used for molecular state labeling too)
  configuration TEXT,
  term TEXT,
  j_value DOUBLE,
  f_value DOUBLE,
  g_value DOUBLE,

  energy_value DOUBLE,
  energy_unit TEXT,
  energy_uncertainty DOUBLE,

  ref_id TEXT REFERENCES refs(ref_id),
  notes TEXT
);

CREATE TABLE IF NOT EXISTS transitions (
  transition_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL REFERENCES isotopologues(iso_id),
  upper_state_id TEXT REFERENCES states(state_id),
  lower_state_id TEXT REFERENCES states(state_id),

  quantity_value DOUBLE NOT NULL,
  quantity_unit TEXT NOT NULL,
  quantity_uncertainty DOUBLE,

  intensity_json TEXT, -- packed physics payload (Aki, Ei/Ek, gi/gk, etc.)
  selection_rules TEXT,

  ref_id TEXT REFERENCES refs(ref_id),
  source TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS spectroscopic_parameters (
  param_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL REFERENCES isotopologues(iso_id),
  model TEXT,
  name TEXT NOT NULL,
  value DOUBLE NOT NULL,
  unit TEXT NOT NULL,
  uncertainty DOUBLE,
  context_json TEXT,
  convention TEXT,
  ref_id TEXT REFERENCES refs(ref_id),
  source TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS source_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  retrieved_utc TEXT NOT NULL,
  url TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  local_path TEXT,
  notes TEXT
);
