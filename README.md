
docker exec -it ksqldb-cli ksql http://ksqldb:8088

show topics

print users from beginning;

CREATE STREAM users_stream (name VARCHAR, points int, favorite_color VARCHAR, favourite_language VARCHAR) WITH (kafka_topic='users', value_format='AVRO');


show streams;

select * from users_stream emit changes;


SET 'auto.offset.reset' = 'earliest';

create stream great_points as select * from users_stream where POINTS>5 emit changes;


CREATE TABLE users_aggregate_all
AS SELECT S.NAME,SUM(S.POINTS) AS TOTAL 
FROM users_stream S 
GROUP BY S.NAME;


CREATE TABLE users_aggregate 
AS SELECT S.NAME,SUM(S.POINTS) AS TOTAL 
FROM users_stream S WINDOW TUMBLING (SIZE 10 SECOND) 
GROUP BY S.NAME;


