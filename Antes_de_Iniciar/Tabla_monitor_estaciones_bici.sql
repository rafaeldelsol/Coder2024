CREATE TABLE monitor_estaciones_bici (
    station_id INT,
    fecha DATE,
    hora TIME,
    last_reported INT,
    IN_SERVICE BOOLEAN,
    num_bikes_available INT,
    num_bikes_disabled INT,
    num_docks_available INT,
    num_docks_disabled INT )