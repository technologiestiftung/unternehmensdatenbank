-- The name key treats umlaut and ß spellings alike (#154): "Geräte", "Geraete" and "GERATE" get one key.
--
-- CORDIS writes umlauts as bare vowels ("KNAUER WISSENSCHAFTLICHE GERATE GMBH"), the Förderkatalog
-- partly as "ue" ("Huebner"), the register with umlauts. Measured on play (2026-11-07), against the
-- grants that had no link:
--   - this fold (ä/ae -> a, ö/oe -> o, ü/ue -> u, ß -> ss): 15 more CORDIS names and 11 more
--     Förderkatalog names linked. All 15 CORDIS links hand-checked right.
--   - the transliteration only (ä -> ae, ß -> ss): 3 and 10.
-- The cost: 38 companies now share their key with another company in the same city ("Aerea" and
-- "Area"), so a name match no longer picks either of them alone. That removed 2 Förderkatalog links.
-- A shared key never links the wrong company: it links none, and register_match hands the choice
-- to a person (`match_candidates`).

CREATE OR REPLACE FUNCTION public.normalize_company_name(input_name text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
  WITH step_1 AS (
    SELECT lower(input_name) AS value
  ),
  step_2 AS (
    SELECT regexp_replace(value, '&', ' und ', 'g') AS value
    FROM step_1
  ),
  step_3 AS (
    SELECT regexp_replace(value, '[^[:alnum:]äöüß]+', ' ', 'g') AS value
    FROM step_2
  ),
  -- After the punctuation step, so "ae" is only ever two letters of one word.
  step_fold AS (
    SELECT replace(replace(replace(replace(translate(value, 'äöü', 'aou'), 'ß', 'ss'),
                                   'ae', 'a'), 'oe', 'o'), 'ue', 'u') AS value
    FROM step_3
  ),
  step_4 AS (
    SELECT regexp_replace(
      value,
      '\m(gmbh(\s+co\s+kg)?|ug|haftungsbeschrankt|ag|eg|kg|ohg|gbr|mbh|se|kgaa|ev)\M',
      ' ',
      'g'
    ) AS value
    FROM step_fold
  )
  SELECT NULLIF(trim(regexp_replace(value, '\s+', ' ', 'g')), '')
  FROM step_4;
$$;

COMMENT ON FUNCTION public.normalize_company_name(input_name text) IS 'Normalizes company names for stable matching across legal forms, punctuation and umlaut spellings (ä = ae = a, ß = ss; V8, #154).';

DO $$
BEGIN
  ASSERT public.normalize_company_name('KNAUER WISSENSCHAFTLICHE GERATE GMBH')
       = public.normalize_company_name('KNAUER Wissenschaftliche Geräte GmbH');
  ASSERT public.normalize_company_name('Hübner & Co. Entwicklungs GmbH')
       = public.normalize_company_name('HUEBNER UND CO ENTWICKLUNGS GMBH');
  ASSERT public.normalize_company_name('IOM Innovative Optische Meßtechnik GmbH')
       = public.normalize_company_name('IOM Innovative Optische Messtechnik GmbH');
  ASSERT public.normalize_company_name('Beispiel UG (haftungsbeschränkt)') = 'beispiel';
  ASSERT public.normalize_company_name('Beispiel UG (haftungsbeschraenkt)') = 'beispiel';
  ASSERT public.normalize_company_name('GmbH') IS NULL;
END $$;

-- What the old key computed: the grants' stored key, the index on published names and the links
-- (about 30 s on the local copy of play). The stored profile's grant counts catch up at the next
-- `refresh_company_profile()`, which every coordinator run ends with: in the migration it took 8 of
-- 9 minutes, and the migration runs as the app's initContainer on the hub.
UPDATE public.company_fundings SET recipient_name = recipient_name;
REINDEX INDEX public.company_field_observations_name_key_idx;
SELECT public.refresh_company_funding_links();
