-- Which Wirtschaftsatlas layer stands for which Technologiefeld, and which for AI (#148, step 3 of #105).
--
-- The `wirtschaftsatlas` DAG keeps every layer a company appears in (the map's content-tree path,
-- e.g. 'Sectors.DigiTech.Artificial intelligence') in its source record's payload. This table says
-- which of those layers name one of Berlin's 77 fields. It is a signal to be measured
-- (scripts/wirtschaftsatlas_layers_sample.py on the hand-checked sample), not a value: nothing writes
-- it as an observation until its precision is known.
--
-- Only layers that name exactly one field are listed. Left out on purpose, because they span several
-- fields or none: Startups, Venture capital, Accelerators, Healthcare industries / Life Sciences,
-- Financial service providers, Agencies, Digital economy, Information technology, Print media and
-- publishers, Fashion and design industry, Aerospace, Intelligent transport systems, Grids and
-- storage, Industry 4.0, Lightweight construction, Turbomachinery, Bio energy, Consulting.
--
-- The layer is the exact path as stored, trailing space included ('Water management ').
-- Not exposed through Hasura. A change is a new migration; rows are never deleted.

CREATE TABLE public.wirtschaftsatlas_layer_fields (
    layer text PRIMARY KEY,
    technology_field text REFERENCES public.technology_fields(name) ON UPDATE CASCADE,
    is_ai boolean NOT NULL DEFAULT false,
    CHECK (technology_field IS NOT NULL OR is_ai)
);

COMMENT ON TABLE public.wirtschaftsatlas_layer_fields IS 'Wirtschaftsatlas layers that name one Technologiefeld (and the AI layer), #148. A signal to measure, not a value.';

GRANT SELECT ON TABLE public.wirtschaftsatlas_layer_fields TO ${writer_role};

INSERT INTO public.wirtschaftsatlas_layer_fields (layer, technology_field, is_ai) VALUES
    ('Sectors.DigiTech.Artificial intelligence', 'Künstliche Intelligenz', true),
    ('Sectors.DigiTech.Fintech', 'FinTech', false),
    ('Sectors.Photonics.Companies', 'Photonik', false),
    ('Sectors.Transport, Mobility, Logistics.Rail technology.Companies', 'Bahnindustrie', false),
    ('Sectors.Transport, Mobility, Logistics.Automotive.Companies', 'Automobilwirtschaft', false),
    ('Sectors.Transport, Mobility, Logistics.Logistics.Companies', 'Logistik', false),
    ('Sectors.Industrial production.Electric', 'Elektroindustrie', false),
    ('Sectors.Industrial production.Food', 'Ernährungsindustrie', false),
    ('Sectors.Industrial production.Chemicals industry', 'Chemische Industrie', false),
    ('Sectors.Industrial production.Mechanical engineering', 'Maschinen- und Anlagenbau', false),
    ('Sectors.Industrial production.Robotics', 'Robotik', false),
    ('Sectors.Industrial production.3D Printing', 'Additive Fertigung', false),
    ('Sectors.Media, Creative Industries.Music industry', 'Musikwirtschaft', false),
    ('Sectors.Media, Creative Industries.Games industry', 'Games', false),
    ('Sectors.Media, Creative Industries.Film and television industry', 'Filmwirtschaft', false),
    ('Sectors.Energy and environmental technologies.Water management ', 'Wassertechnologien', false),
    ('Sectors.Energy and environmental technologies.Circular economy', 'Kreislaufwirtschaft', false),
    ('Sectors.Energy and environmental technologies.Solar energy.Companies', 'Solarenergie', false),
    ('Sectors.Energy and environmental technologies.Wind energy.Companies', 'Windenergie', false),
    ('Sectors.Healthcare Industry.Hospitals and clinics', 'Gesundheitsversorgung & Pflege', false);
