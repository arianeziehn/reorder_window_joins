SELECT
  title.title AS Movie_Title,
  name.name AS Actor_Name,
  kind_type.kind AS Movie_Type,
  role_type.role AS Role
FROM
  title
JOIN
  cast_info ON title.id = cast_info.movie_id
JOIN
  name ON cast_info.person_id = name.id
JOIN
  kind_type ON title.kind_id = kind_type.id
JOIN
  role_type ON cast_info.role_id = role_type.id
WHERE
  title.title LIKE 'A%' AND
  title.production_year > 2000;

-- (542277 Zeilen)

