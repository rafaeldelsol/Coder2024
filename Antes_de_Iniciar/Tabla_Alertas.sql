CREATE TABLE alertas (
	station_id INT,
	name CHAR(60),
	address CHAR(60),
	capacity INT,
	fecha DATE,
    hora TIME,
    num_bikes_disabled INT,
    num_docks_available INT,
    Alerta_Enviada BOOLEAN )