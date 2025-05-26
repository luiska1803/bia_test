
CREATE SCHEMA coord_UK_db;

CREATE TABLE coord_UK_db.postcodes (
    id SERIAL PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    postcode VARCHAR(12),
    admin_district TEXT,
    region TEXT,
    result_lat DOUBLE PRECISION,
    result_lon DOUBLE PRECISION,
    estado_api BIGINT,
    descripcion_estado VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Busqueda por codigo postal
CREATE INDEX indx_postcode ON postcodes(postcode);

-- Consultas por coordenadas rapidas
CREATE INDEX indx_lat_lon ON postcodes(lat, lon);