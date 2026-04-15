ALTER TABLE public.companies
ADD COLUMN IF NOT EXISTS postcode TEXT,
ADD COLUMN IF NOT EXISTS city TEXT,
ADD COLUMN IF NOT EXISTS district TEXT,
ADD COLUMN IF NOT EXISTS locality TEXT;

CREATE INDEX IF NOT EXISTS companies_postcode_idx
  ON public.companies (postcode);

CREATE INDEX IF NOT EXISTS companies_normalized_name_postcode_idx
  ON public.companies (normalized_name, postcode);
