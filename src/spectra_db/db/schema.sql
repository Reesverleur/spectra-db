-- spectra_db canonical schema (DuckDB)
-- Keep this schema stable; evolve via additive changes where possible.

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
  tags TEXT, -- freeform (e.g. "atomic", "molecular")
  notes TEXT
);

CREATE TABLE IF NOT EXISTS isotopologues (
  iso_id TEXT PRIMARY KEY,
  species_id TEXT NOT NULL REFERENCES species(species_id),
  label TEXT, -- e.g. "12C16O", "H2O", "14N2"
  composition_json TEXT, -- JSON string describing isotopic composition
  nuclear_spins_json TEXT, -- JSON string mapping nucleus->I
  mass_amu DOUBLE,
  abundance DOUBLE,
  notes TEXT
);

-- State table intentionally flexible: store structured tokens in JSON strings.
CREATE TABLE IF NOT EXISTS states (
  state_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL REFERENCES isotopologues(iso_id),
  state_type TEXT NOT NULL, -- "atomic" or "molecular"
  electronic_label TEXT, -- e.g. "X1Sigma+", "2P3/2"
  vibrational_json TEXT, -- JSON string (e.g. {"v":0} or {"v1":0,"v2":1})
  rotational_json TEXT, -- JSON string (e.g. {"J":1,"N":1,"Ka":0,"Kc":1,"F":2})
  parity TEXT, -- "+", "-", "e", "f", etc.
  energy_value DOUBLE,
  energy_unit TEXT, -- "cm-1", "eV", "Hz"
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
  quantity_unit TEXT NOT NULL, -- "Hz", "cm-1", "nm", etc.
  quantity_uncertainty DOUBLE,
  intensity_json TEXT, -- JSON string (Aki, f, S, log(gf), etc.)
  selection_rules TEXT,
  ref_id TEXT REFERENCES refs(ref_id),
  source TEXT,
  notes TEXT
);

-- Parameters: rovib coupling, anharmonicity, hyperfine, etc.
CREATE TABLE IF NOT EXISTS spectroscopic_parameters (
  param_id TEXT PRIMARY KEY,
  iso_id TEXT NOT NULL REFERENCES isotopologues(iso_id),
  model TEXT, -- "Dunham", "Herzberg", "Watson-A", "Watson-S", "Effective-HFS", ...
  name TEXT NOT NULL, -- e.g. "Be", "alpha_e", "omega_e", "omega_exe", "Y_10", "eQq", ...
  value DOUBLE NOT NULL,
  unit TEXT NOT NULL, -- "cm-1", "MHz", "kHz", dimensionless, etc.
  uncertainty DOUBLE,
  context_json TEXT, -- JSON string specifying state/v-range/etc.
  convention TEXT, -- e.g. "Watson A-reduction Ir"
  ref_id TEXT REFERENCES refs(ref_id),
  source TEXT,
  notes TEXT
);

-- Track raw snapshot provenance (for reproducibility)
CREATE TABLE IF NOT EXISTS source_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  source TEXT NOT NULL, -- "NIST_ASD", "NIST_MOLSPEC_DIATOMIC", ...
  retrieved_utc TEXT NOT NULL, -- ISO timestamp
  url TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  local_path TEXT,
  notes TEXT
);
