SELECT
  title.title,
  name.name
FROM
  title
JOIN
  cast_info ON title.id = cast_info.movie_id
JOIN
  name ON cast_info.person_id = name.id
WHERE
  title.title LIKE 'A%' AND 
  title.production_year > 2000;

-- (1071429 Zeilen)

