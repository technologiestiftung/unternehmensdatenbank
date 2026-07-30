DROP INDEX IF EXISTS public.companies_lat_lon_idx;
DROP INDEX IF EXISTS public.companies_tags_gin_idx;
DROP INDEX IF EXISTS public.companies_extra_data_gin_idx;

ALTER TABLE public.companies
DROP COLUMN IF EXISTS share_capital,
DROP COLUMN IF EXISTS website_url,
DROP COLUMN IF EXISTS funding,
DROP COLUMN IF EXISTS cluster,
DROP COLUMN IF EXISTS revenue,
DROP COLUMN IF EXISTS revenue_yoy,
DROP COLUMN IF EXISTS revenue_current_year,
DROP COLUMN IF EXISTS employee_count,
DROP COLUMN IF EXISTS tags,
DROP COLUMN IF EXISTS extra_data,
DROP COLUMN IF EXISTS lat,
DROP COLUMN IF EXISTS lon;
