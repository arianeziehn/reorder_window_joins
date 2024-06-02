SELECT
  title.title,
  kind_type.kind
FROM
  title
JOIN
  kind_type ON title.kind_id = kind_type.id
WHERE
  title.title LIKE 'A%' AND
  title.production_year > 2000;
-- 43963 rows
