FILMWORKS = (
    "SELECT id, modified "
    "FROM content.film_work "
    "WHERE modified > %s "
    "ORDER BY modified;"
)

FILMWORKS_ALL = "SELECT id, modified " "FROM content.film_work " "ORDER BY modified;"

GENRES = (
    "SELECT id, modified "
    "FROM content.genre "
    "WHERE modified > %s "
    "ORDER BY modified;"
)

GENRES_ALL = "SELECT id, modified " "FROM content.genre " "ORDER BY modified;"

PERSONS = (
    "SELECT id, modified "
    "FROM content.person "
    "WHERE modified > %s "
    "ORDER BY modified;"
)

PERSONS_ALL = "SELECT id, modified " "FROM content.person " "ORDER BY modified;"

FILMWORKS_BY_P = (
    "SELECT fw.id, fw.modified "
    "FROM content.film_work fw "
    "LEFT JOIN content.person_film_work pfw "
    "ON pfw.film_work_id = fw.id "
    "WHERE pfw.person_id IN %s "
    "ORDER BY fw.modified;"
)

FILMWORKS_BY_G = (
    "SELECT fw.id, fw.modified "
    "FROM content.film_work fw "
    "LEFT JOIN content.genre_film_work gfw "
    "ON gfw.film_work_id = fw.id "
    "WHERE gfw.genre_id IN %s "
    "ORDER BY fw.modified;"
)

MISSING_DATA = (
    "SELECT fw.id as fw_id, "
    "fw.title, "
    "fw.description, "
    "fw.rating, "
    "fw.type, "
    "fw.created, "
    "fw.modified, "
    "pfw.role, "
    "p.id, "
    "p.full_name, "
    "g.name "
    "FROM content.film_work fw "
    "LEFT JOIN content.person_film_work pfw "
    "ON pfw.film_work_id = fw.id "
    "LEFT JOIN content.person p ON p.id = pfw.person_id "
    "LEFT JOIN content.genre_film_work gfw "
    "ON gfw.film_work_id = fw.id "
    "LEFT JOIN content.genre g ON g.id = gfw.genre_id "
    "WHERE fw.id IN %s "
    "ORDER BY fw.modified, g.name, p.id;"
)
