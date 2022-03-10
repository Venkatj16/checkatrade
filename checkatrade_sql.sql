use airflow_db;

select * from 
(Select people.name, count(films.film_name) as TotalFilms from results_people as people 
Inner join results_films as films
On
People.ID_People = films. ID_People
group by people.name
order by TotalFilms desc) a limit 1;