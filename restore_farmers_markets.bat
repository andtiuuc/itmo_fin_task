@echo off

chcp 65001

set /p PGUSER=Введите имя пользователя PostgreSQL: 

set /p FILEPATH=Введите полный путь к SQL файлу (например, D:\farmers_markets_dump.sql): 

createdb -U %PGUSER% -h localhost farmers_markets

psql -U %PGUSER% -h localhost -d farmers_markets -f "%FILEPATH%"

pause