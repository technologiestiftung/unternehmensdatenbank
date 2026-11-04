-- website_discovery's name still said "URL patterns + OpenStreetMap" in databases where the DAG has not
-- run since the OpenStreetMap step was removed (#57). Set it to the name the code registers
-- (`enrichment.observations.SOURCE_NAME`), so the app's sources pages do not name a step that is gone.
UPDATE public.sources
   SET name = 'Website discovery (URL patterns, checked against the Impressum)', updated_at = now()
 WHERE key = 'website_discovery';
