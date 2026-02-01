-- src/spectra_db/db/schema_molecular.sql

CREATE TABLE IF NOT EXISTS meta_info (
  key TEXT PRIMARY KEY,
  value TEXT
);

-- Species (molecular)
CREATE TABLE IF NOT EXISTS species (
  species_id TEXT PRIMARY KEY,
  formula TEXT NOT NULL,
  name TEXT,
  charge INTEGER NOT NULL,
  multiplicity INTEGER,
  inchi_key TEXT,
  tags TEXT,
  notes TEXT,
  extra_json TEXT
);

CREATE TABLE IF NOT EXISTS isotopologues (
  iso_id TEXT PRIMARY KEY,
  species_id TEXT NOT NULL,
  label TEXT,
  composition_json TEXT,
  nuclear_spins_json TEXT,
  mass_amu DOUBLE,
  abundance DOUBLE,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS refs (
  ref_id TEXT PRIMARY KEY,
  ref_type TEXT NOT NULL DEFAULT 'unknown',
  citation TEXT,
  doi TEXT,
  url TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS states (
  state_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL,
  state_type TEXT NOT NULL,
  electronic_label TEXT,
  vibrational_json TEXT,
  rotational_json TEXT,
  parity TEXT,
  configuration TEXT,
  term TEXT,
  j_value DOUBLE,
  f_value DOUBLE,
  g_value DOUBLE,
  lande_g DOUBLE,
  leading_percentages TEXT,
  extra_json TEXT,
  energy_value DOUBLE,
  energy_unit TEXT,
  energy_uncertainty DOUBLE,
  ref_id TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS transitions (
  transition_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL,
  upper_state_id TEXT,
  lower_state_id TEXT,
  quantity_value DOUBLE,
  quantity_unit TEXT,
  quantity_uncertainty DOUBLE,
  intensity_json TEXT,
  extra_json TEXT,
  selection_rules TEXT,
  ref_id TEXT,
  source TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS spectroscopic_parameters (
  param_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL,
  model TEXT NOT NULL,
  name TEXT NOT NULL,
  value DOUBLE,
  unit TEXT,
  uncertainty DOUBLE,
  text_value TEXT,
  value_suffix TEXT,
  markers_json TEXT,
  ref_ids_json TEXT,
  context_json TEXT,
  raw_text TEXT,
  convention TEXT,
  ref_id TEXT,
  source TEXT,
  notes TEXT
);
