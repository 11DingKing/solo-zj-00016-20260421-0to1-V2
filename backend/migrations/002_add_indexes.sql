CREATE INDEX IF NOT EXISTS idx_short_links_created_at ON short_links(created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_short_links_code_unique ON short_links(short_code);
