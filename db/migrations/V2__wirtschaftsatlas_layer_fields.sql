-- Which Wirtschaftsatlas layer stands for which Technologiefeld (#148, step 3 of #105).
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
    technology_field text NOT NULL REFERENCES public.technology_fields(name) ON UPDATE CASCADE
);

COMMENT ON TABLE public.wirtschaftsatlas_layer_fields IS 'Wirtschaftsatlas layers that name one Technologiefeld, #148. A signal to measure, not a value.';

GRANT SELECT ON TABLE public.wirtschaftsatlas_layer_fields TO ${writer_role};

INSERT INTO public.wirtschaftsatlas_layer_fields (layer, technology_field) VALUES
    ('Sectors.DigiTech.Artificial intelligence', 'Künstliche Intelligenz'),
    ('Sectors.DigiTech.Fintech', 'FinTech'),
    ('Sectors.Photonics.Companies', 'Photonik'),
    ('Sectors.Transport, Mobility, Logistics.Rail technology.Companies', 'Bahnindustrie'),
    ('Sectors.Transport, Mobility, Logistics.Automotive.Companies', 'Automobilwirtschaft'),
    ('Sectors.Transport, Mobility, Logistics.Logistics.Companies', 'Logistik'),
    ('Sectors.Industrial production.Electric', 'Elektroindustrie'),
    ('Sectors.Industrial production.Food', 'Ernährungsindustrie'),
    ('Sectors.Industrial production.Chemicals industry', 'Chemische Industrie'),
    ('Sectors.Industrial production.Mechanical engineering', 'Maschinen- und Anlagenbau'),
    ('Sectors.Industrial production.Robotics', 'Robotik'),
    ('Sectors.Industrial production.3D Printing', 'Additive Fertigung'),
    ('Sectors.Media, Creative Industries.Music industry', 'Musikwirtschaft'),
    ('Sectors.Media, Creative Industries.Games industry', 'Games'),
    ('Sectors.Media, Creative Industries.Film and television industry', 'Filmwirtschaft'),
    ('Sectors.Energy and environmental technologies.Water management ', 'Wassertechnologien'),
    ('Sectors.Energy and environmental technologies.Circular economy', 'Kreislaufwirtschaft'),
    ('Sectors.Energy and environmental technologies.Solar energy.Companies', 'Solarenergie'),
    ('Sectors.Energy and environmental technologies.Wind energy.Companies', 'Windenergie'),
    ('Sectors.Healthcare Industry.Hospitals and clinics', 'Gesundheitsversorgung & Pflege');
