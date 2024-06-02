SELECT
  title.title AS Movie_Title,
  name.name AS Actor_Name,
  kind_type.kind AS Movie_Type,
  role_type.role AS Role,
  movie_info.info AS Movie_Detail,
  info_type.info AS Detail_Type,
  company_name.name AS Production_Company,
  company_type.kind AS Company_Type
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
JOIN
  movie_info ON title.id = movie_info.movie_id
JOIN
  info_type ON movie_info.info_type_id = info_type.id
JOIN
  movie_companies ON title.id = movie_companies.movie_id
JOIN
  company_name ON movie_companies.company_id = company_name.id
JOIN
  company_type ON movie_companies.company_type_id = company_type.id
WHERE
  title.title LIKE 'A%' AND
  title.production_year > 2000;

-- (4489844 Zeilen)

