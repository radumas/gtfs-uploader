--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.3
-- Dumped by pg_dump version 9.5.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = gtfs, pg_catalog;

ALTER TABLE IF EXISTS ONLY gtfs.bus_patterns DROP CONSTRAINT IF EXISTS bus_patterns_stop;
ALTER TABLE IF EXISTS ONLY gtfs.frequencies DROP CONSTRAINT IF EXISTS _trip_id;
ALTER TABLE IF EXISTS ONLY gtfs.stop_times DROP CONSTRAINT IF EXISTS _trip_id;
ALTER TABLE IF EXISTS ONLY gtfs.transfers DROP CONSTRAINT IF EXISTS _to_stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.stops_geog DROP CONSTRAINT IF EXISTS _stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.stop_times DROP CONSTRAINT IF EXISTS _stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.trips DROP CONSTRAINT IF EXISTS _route_id;
ALTER TABLE IF EXISTS ONLY gtfs.transfers DROP CONSTRAINT IF EXISTS _from_stop_id;
DROP INDEX IF EXISTS gtfs.geom_index;
ALTER TABLE IF EXISTS ONLY gtfs.stop_stop_matrix DROP CONSTRAINT IF EXISTS matrix_stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.stops_geog DROP CONSTRAINT IF EXISTS geog_stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.frequencies DROP CONSTRAINT IF EXISTS frequencies_trip_id;
ALTER TABLE IF EXISTS ONLY gtfs.calendar_dates DROP CONSTRAINT IF EXISTS calendar_service_id_key;
ALTER TABLE IF EXISTS ONLY gtfs.bus_patterns DROP CONSTRAINT IF EXISTS bus_patterns_pk;
ALTER TABLE IF EXISTS ONLY gtfs.stop_times DROP CONSTRAINT IF EXISTS _trip_id_sequence;
ALTER TABLE IF EXISTS ONLY gtfs.trips DROP CONSTRAINT IF EXISTS _trip_id;
ALTER TABLE IF EXISTS ONLY gtfs.transfers DROP CONSTRAINT IF EXISTS _transfer_stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.stops DROP CONSTRAINT IF EXISTS _stop_id;
ALTER TABLE IF EXISTS ONLY gtfs.routes DROP CONSTRAINT IF EXISTS _route_id;
DROP TABLE IF EXISTS gtfs.transfers;
DROP TABLE IF EXISTS gtfs.stops_geog;
DROP TABLE IF EXISTS gtfs.stops;
DROP TABLE IF EXISTS gtfs.stop_times;
DROP TABLE IF EXISTS gtfs.stop_stop_matrix;
DROP TABLE IF EXISTS gtfs.shapes_geog;
DROP TABLE IF EXISTS gtfs.shapes;
DROP TABLE IF EXISTS gtfs.routes;
DROP TABLE IF EXISTS gtfs.frequencies;
DROP TABLE IF EXISTS gtfs.calendar_dates;
DROP TABLE IF EXISTS gtfs.calendar;
DROP VIEW IF EXISTS gtfs.bustrippatterns;
DROP TABLE IF EXISTS gtfs.trips;
DROP TABLE IF EXISTS gtfs.bus_patterns;
DROP SCHEMA IF EXISTS gtfs;
--
-- Name: gtfs; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA gtfs;


SET search_path = gtfs, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: bus_patterns; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE bus_patterns (
    shape_id character varying(16) NOT NULL,
    stop_id character varying(32) NOT NULL,
    stop_sequence smallint NOT NULL,
    cumulative_distance integer
);


--
-- Name: trips; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE trips (
    route_id character varying(32) NOT NULL,
    service_id character varying(32),
    trip_id character varying(64) NOT NULL,
    trip_headsign character varying(80),
    trip_short_name character varying(64),
    direction_id smallint,
    block_id character varying(16),
    shape_id character varying(8)
);


--
-- Name: bustrippatterns; Type: VIEW; Schema: gtfs; Owner: -
--

CREATE VIEW bustrippatterns AS
 SELECT trips.route_id,
    trips.direction_id,
    trips.trip_id,
    bp.stop_id,
    bp.stop_sequence,
    bp.cumulative_distance
   FROM (bus_patterns bp
     JOIN trips trips ON (((bp.shape_id)::text = (trips.shape_id)::text)));


--
-- Name: calendar; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE calendar (
    service_id character varying(32) NOT NULL,
    monday boolean NOT NULL,
    tuesday boolean NOT NULL,
    wednesday boolean NOT NULL,
    thursday boolean NOT NULL,
    friday boolean NOT NULL,
    saturday boolean NOT NULL,
    sunday boolean NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL
);


--
-- Name: calendar_dates; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE calendar_dates (
    service_id character varying(32) NOT NULL,
    service_date date NOT NULL,
    exception_type smallint NOT NULL
);


--
-- Name: frequencies; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE frequencies (
    trip_id character varying(64) NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    headway_secs smallint NOT NULL
);


--
-- Name: routes; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE routes (
    route_id character varying(16) NOT NULL,
    route_short_name character varying(28),
    route_long_name character varying(46),
    route_desc character varying(128),
    route_type smallint,
    route_url character varying(2),
    route_color character(6),
    route_text_color character(6),
    agency_id character(1) NOT NULL
);


--
-- Name: shapes; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE shapes (
    shape_id character varying(16) NOT NULL,
    shape_pt_lat character varying(16) NOT NULL,
    shape_pt_lon character varying(16) NOT NULL,
    shape_pt_sequence integer NOT NULL,
    shape_dist_travelled integer
);


--
-- Name: shapes_geog; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE shapes_geog (
    shape_id character varying(16) NOT NULL,
    shape public.geometry(LineString,4326),
    CONSTRAINT enforce_geotype_geom CHECK (((public.geometrytype(shape) = 'LINESTRING'::text) OR (shape IS NULL))),
    CONSTRAINT enforce_srid_geom CHECK ((public.st_srid(shape) = 4326))
);


--
-- Name: stop_stop_matrix; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE stop_stop_matrix (
    from_stop_id character varying(32) NOT NULL,
    to_stop_id character varying(32) NOT NULL,
    dist integer NOT NULL
);


--
-- Name: stop_times; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE stop_times (
    trip_id character varying(64) NOT NULL,
    arrival_time timestamp without time zone NOT NULL,
    departure_time timestamp without time zone NOT NULL,
    stop_id character varying(64) NOT NULL,
    stop_sequence smallint NOT NULL,
    stop_headsign character varying(8),
    pickup_type smallint,
    drop_off_type smallint
);


--
-- Name: stops; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE stops (
    stop_id character varying(32) NOT NULL,
    stop_code character varying(8),
    stop_name character varying(64) NOT NULL,
    stop_desc character varying(128),
    stop_lat character varying(11) NOT NULL,
    stop_lon character varying(12) NOT NULL,
    zone_id character varying(2),
    stop_url character varying(2),
    location_type smallint,
    parent_station character varying(32)
);


--
-- Name: stops_geog; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE stops_geog (
    stop_id character varying(32) NOT NULL,
    parent_station character varying(32),
    geog_latlon public.geography(Point,4326),
    geom public.geometry
);


--
-- Name: transfers; Type: TABLE; Schema: gtfs; Owner: -
--

CREATE TABLE transfers (
    from_stop_id character varying(32) NOT NULL,
    to_stop_id character varying(32) NOT NULL,
    transfer_type smallint NOT NULL,
    min_transfer_time smallint
);


--
-- Name: _route_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY routes
    ADD CONSTRAINT _route_id PRIMARY KEY (route_id);


--
-- Name: _stop_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stops
    ADD CONSTRAINT _stop_id PRIMARY KEY (stop_id);


--
-- Name: _transfer_stop_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY transfers
    ADD CONSTRAINT _transfer_stop_id PRIMARY KEY (from_stop_id, to_stop_id);


--
-- Name: _trip_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY trips
    ADD CONSTRAINT _trip_id PRIMARY KEY (trip_id);


--
-- Name: _trip_id_sequence; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stop_times
    ADD CONSTRAINT _trip_id_sequence PRIMARY KEY (trip_id, stop_sequence);


--
-- Name: bus_patterns_pk; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY bus_patterns
    ADD CONSTRAINT bus_patterns_pk PRIMARY KEY (shape_id, stop_sequence);


--
-- Name: calendar_service_id_key; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY calendar_dates
    ADD CONSTRAINT calendar_service_id_key PRIMARY KEY (service_id, service_date);


--
-- Name: frequencies_trip_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY frequencies
    ADD CONSTRAINT frequencies_trip_id PRIMARY KEY (trip_id);


--
-- Name: geog_stop_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stops_geog
    ADD CONSTRAINT geog_stop_id PRIMARY KEY (stop_id);


--
-- Name: matrix_stop_id; Type: CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stop_stop_matrix
    ADD CONSTRAINT matrix_stop_id PRIMARY KEY (from_stop_id);


--
-- Name: geom_index; Type: INDEX; Schema: gtfs; Owner: -
--

CREATE INDEX geom_index ON stops_geog USING gist (geom);


--
-- Name: _from_stop_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY transfers
    ADD CONSTRAINT _from_stop_id FOREIGN KEY (from_stop_id) REFERENCES stops(stop_id);


--
-- Name: _route_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY trips
    ADD CONSTRAINT _route_id FOREIGN KEY (route_id) REFERENCES routes(route_id);


--
-- Name: _stop_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stop_times
    ADD CONSTRAINT _stop_id FOREIGN KEY (stop_id) REFERENCES stops(stop_id);


--
-- Name: _stop_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stops_geog
    ADD CONSTRAINT _stop_id FOREIGN KEY (stop_id) REFERENCES stops(stop_id) NOT VALID;


--
-- Name: _to_stop_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY transfers
    ADD CONSTRAINT _to_stop_id FOREIGN KEY (to_stop_id) REFERENCES stops(stop_id);


--
-- Name: _trip_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY stop_times
    ADD CONSTRAINT _trip_id FOREIGN KEY (trip_id) REFERENCES trips(trip_id);


--
-- Name: _trip_id; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY frequencies
    ADD CONSTRAINT _trip_id FOREIGN KEY (trip_id) REFERENCES trips(trip_id);


--
-- Name: bus_patterns_stop; Type: FK CONSTRAINT; Schema: gtfs; Owner: -
--

ALTER TABLE ONLY bus_patterns
    ADD CONSTRAINT bus_patterns_stop FOREIGN KEY (stop_id) REFERENCES stops(stop_id);


--
-- Name: gtfs; Type: ACL; Schema: -; Owner: -
--

REVOKE ALL ON SCHEMA gtfs FROM PUBLIC;
REVOKE ALL ON SCHEMA gtfs FROM rad;
GRANT ALL ON SCHEMA gtfs TO rad;
GRANT ALL ON SCHEMA gtfs TO PUBLIC;


--
-- Name: bus_patterns; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE bus_patterns FROM PUBLIC;
REVOKE ALL ON TABLE bus_patterns FROM java;
GRANT ALL ON TABLE bus_patterns TO java;
GRANT ALL ON TABLE bus_patterns TO PUBLIC;


--
-- Name: trips; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE trips FROM PUBLIC;
REVOKE ALL ON TABLE trips FROM java;
GRANT ALL ON TABLE trips TO java;
GRANT ALL ON TABLE trips TO PUBLIC;


--
-- Name: bustrippatterns; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE bustrippatterns FROM PUBLIC;
REVOKE ALL ON TABLE bustrippatterns FROM radumas;
GRANT ALL ON TABLE bustrippatterns TO radumas;
GRANT ALL ON TABLE bustrippatterns TO PUBLIC;


--
-- Name: calendar; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE calendar FROM PUBLIC;
REVOKE ALL ON TABLE calendar FROM java;
GRANT ALL ON TABLE calendar TO java;
GRANT ALL ON TABLE calendar TO PUBLIC;


--
-- Name: calendar_dates; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE calendar_dates FROM PUBLIC;
REVOKE ALL ON TABLE calendar_dates FROM java;
GRANT ALL ON TABLE calendar_dates TO java;
GRANT ALL ON TABLE calendar_dates TO PUBLIC;


--
-- Name: frequencies; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE frequencies FROM PUBLIC;
REVOKE ALL ON TABLE frequencies FROM java;
GRANT ALL ON TABLE frequencies TO java;
GRANT ALL ON TABLE frequencies TO PUBLIC;


--
-- Name: routes; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE routes FROM PUBLIC;
REVOKE ALL ON TABLE routes FROM java;
GRANT ALL ON TABLE routes TO java;
GRANT ALL ON TABLE routes TO PUBLIC;


--
-- Name: shapes; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE shapes FROM PUBLIC;
REVOKE ALL ON TABLE shapes FROM java;
GRANT ALL ON TABLE shapes TO java;
GRANT ALL ON TABLE shapes TO PUBLIC;


--
-- Name: shapes_geog; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE shapes_geog FROM PUBLIC;
REVOKE ALL ON TABLE shapes_geog FROM java;
GRANT ALL ON TABLE shapes_geog TO java;
GRANT ALL ON TABLE shapes_geog TO PUBLIC;


--
-- Name: stop_stop_matrix; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE stop_stop_matrix FROM PUBLIC;
REVOKE ALL ON TABLE stop_stop_matrix FROM java;
GRANT ALL ON TABLE stop_stop_matrix TO java;
GRANT ALL ON TABLE stop_stop_matrix TO PUBLIC;


--
-- Name: stop_times; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE stop_times FROM PUBLIC;
REVOKE ALL ON TABLE stop_times FROM java;
GRANT ALL ON TABLE stop_times TO java;
GRANT ALL ON TABLE stop_times TO PUBLIC;


--
-- Name: stops; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE stops FROM PUBLIC;
REVOKE ALL ON TABLE stops FROM java;
GRANT ALL ON TABLE stops TO java;
GRANT ALL ON TABLE stops TO PUBLIC;


--
-- Name: stops_geog; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE stops_geog FROM PUBLIC;
REVOKE ALL ON TABLE stops_geog FROM java;
GRANT ALL ON TABLE stops_geog TO java;
GRANT ALL ON TABLE stops_geog TO PUBLIC;


--
-- Name: transfers; Type: ACL; Schema: gtfs; Owner: -
--

REVOKE ALL ON TABLE transfers FROM PUBLIC;
REVOKE ALL ON TABLE transfers FROM java;
GRANT ALL ON TABLE transfers TO java;
GRANT ALL ON TABLE transfers TO PUBLIC;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: gtfs; Owner: -
--

ALTER DEFAULT PRIVILEGES FOR ROLE rad IN SCHEMA gtfs REVOKE ALL ON TABLES  FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE rad IN SCHEMA gtfs REVOKE ALL ON TABLES  FROM rad;
ALTER DEFAULT PRIVILEGES FOR ROLE rad IN SCHEMA gtfs GRANT ALL ON TABLES  TO PUBLIC;


--
-- PostgreSQL database dump complete
--

