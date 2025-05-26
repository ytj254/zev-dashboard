--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4 (Ubuntu 17.4-1.pgdg20.04+2)
-- Dumped by pg_dump version 17.4 (Ubuntu 17.4-1.pgdg20.04+2)

-- Started on 2025-05-26 17:53:07 EDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 2 (class 3079 OID 17748)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 4362 (class 0 OID 0)
-- Dependencies: 2
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 223 (class 1259 OID 18836)
-- Name: charger; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.charger (
    id integer NOT NULL,
    fleet_id integer NOT NULL,
    charger character varying(45) DEFAULT NULL::character varying,
    charger_type character varying(45) DEFAULT NULL::character varying,
    connector_type character varying(45) DEFAULT NULL::character varying,
    max_power_output integer,
    dedicated_use boolean
);


ALTER TABLE public.charger OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 18842)
-- Name: charger_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.charger_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.charger_id_seq OWNER TO postgres;

--
-- TOC entry 4363 (class 0 OID 0)
-- Dependencies: 224
-- Name: charger_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.charger_id_seq OWNED BY public.charger.id;


--
-- TOC entry 225 (class 1259 OID 18843)
-- Name: fleet; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fleet (
    id integer NOT NULL,
    fleet_name character varying(45) DEFAULT NULL::character varying,
    fleet_size integer,
    zev_tot integer,
    zev_grant integer,
    charger_grant integer,
    depot_adr text,
    vendor_name text,
    latitude double precision,
    longitude double precision,
    depot_loc public.geography(Point,4326)
);


ALTER TABLE public.fleet OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 18849)
-- Name: fleet_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fleet_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fleet_id_seq OWNER TO postgres;

--
-- TOC entry 4364 (class 0 OID 0)
-- Dependencies: 226
-- Name: fleet_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.fleet_id_seq OWNED BY public.fleet.id;


--
-- TOC entry 227 (class 1259 OID 18850)
-- Name: maintenance; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.maintenance (
    id integer NOT NULL,
    date date,
    maint_ob integer,
    maint_categ character varying(20) DEFAULT NULL::character varying,
    problem text,
    work_perf text,
    maint_type integer,
    enter_shop timestamp without time zone,
    exit_shop timestamp without time zone,
    enter_odo integer,
    exit_odo integer,
    parts_cost numeric(10,0) DEFAULT NULL::numeric,
    labor_cost numeric(10,0) DEFAULT NULL::numeric,
    add_cost numeric(10,0) DEFAULT NULL::numeric,
    warranty boolean,
    fleet_id integer NOT NULL,
    charger_id integer,
    vehicle_id integer
);


ALTER TABLE public.maintenance OWNER TO postgres;

--
-- TOC entry 228 (class 1259 OID 18859)
-- Name: maintenance_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.maintenance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.maintenance_id_seq OWNER TO postgres;

--
-- TOC entry 4365 (class 0 OID 0)
-- Dependencies: 228
-- Name: maintenance_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.maintenance_id_seq OWNED BY public.maintenance.id;


--
-- TOC entry 229 (class 1259 OID 18860)
-- Name: refuel_inf; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.refuel_inf (
    id integer NOT NULL,
    charger_id integer NOT NULL,
    veh_id integer,
    charger_type integer,
    connect_time timestamp without time zone,
    disconnect_time timestamp without time zone,
    refuel_start timestamp without time zone,
    refuel_end timestamp without time zone,
    avg_power integer,
    max_power integer,
    tot_energy integer,
    start_soc numeric(5,2) DEFAULT NULL::numeric,
    end_soc numeric(5,2) DEFAULT NULL::numeric
);


ALTER TABLE public.refuel_inf OWNER TO postgres;

--
-- TOC entry 230 (class 1259 OID 18865)
-- Name: refuel_inf_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.refuel_inf_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.refuel_inf_id_seq OWNER TO postgres;

--
-- TOC entry 4366 (class 0 OID 0)
-- Dependencies: 230
-- Name: refuel_inf_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.refuel_inf_id_seq OWNED BY public.refuel_inf.id;


--
-- TOC entry 231 (class 1259 OID 18866)
-- Name: veh_daily; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.veh_daily (
    id integer NOT NULL,
    veh_id integer NOT NULL,
    date date,
    trip_num integer,
    init_odo integer,
    final_odo integer,
    tot_dist integer,
    tot_dura integer,
    idle_time integer,
    init_soc numeric(5,2) DEFAULT NULL::numeric,
    final_soc numeric(5,2) DEFAULT NULL::numeric,
    tot_soc_used numeric(5,2) DEFAULT NULL::numeric,
    tot_energy integer,
    peak_payload integer
);


ALTER TABLE public.veh_daily OWNER TO postgres;

--
-- TOC entry 232 (class 1259 OID 18872)
-- Name: veh_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.veh_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.veh_daily_id_seq OWNER TO postgres;

--
-- TOC entry 4367 (class 0 OID 0)
-- Dependencies: 232
-- Name: veh_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.veh_daily_id_seq OWNED BY public.veh_daily.id;


--
-- TOC entry 233 (class 1259 OID 18873)
-- Name: veh_tel; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.veh_tel (
    id integer NOT NULL,
    veh_id integer NOT NULL,
    "timestamp" timestamp with time zone,
    elevation numeric(8,3) DEFAULT NULL::numeric,
    speed integer,
    mileage integer,
    soc numeric(5,2) DEFAULT NULL::numeric,
    key_on_time numeric(5,2) DEFAULT NULL::numeric,
    latitude double precision,
    longitude double precision,
    location public.geography(Point,4326)
);


ALTER TABLE public.veh_tel OWNER TO postgres;

--
-- TOC entry 234 (class 1259 OID 18881)
-- Name: veh_tel_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.veh_tel_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.veh_tel_id_seq OWNER TO postgres;

--
-- TOC entry 4368 (class 0 OID 0)
-- Dependencies: 234
-- Name: veh_tel_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.veh_tel_id_seq OWNED BY public.veh_tel.id;


--
-- TOC entry 235 (class 1259 OID 18882)
-- Name: vehicle; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.vehicle (
    id integer NOT NULL,
    fleet_id integer NOT NULL,
    fleet_vehicle_id character varying(45) DEFAULT NULL::character varying,
    make character varying(45) DEFAULT NULL::character varying,
    model character varying(45) DEFAULT NULL::character varying,
    year integer,
    class integer,
    curb_wt integer,
    gross_wt integer,
    rated_cap integer,
    nominal_range integer,
    nominal_eff numeric(5,2) DEFAULT NULL::numeric,
    battery_chem integer,
    peak_power integer,
    peak_torque integer,
    towing_cap integer,
    vocation character varying(45) DEFAULT NULL::character varying
);


ALTER TABLE public.vehicle OWNER TO postgres;

--
-- TOC entry 236 (class 1259 OID 18890)
-- Name: vehicle_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.vehicle_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.vehicle_id_seq OWNER TO postgres;

--
-- TOC entry 4369 (class 0 OID 0)
-- Dependencies: 236
-- Name: vehicle_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.vehicle_id_seq OWNED BY public.vehicle.id;


--
-- TOC entry 4133 (class 2604 OID 18891)
-- Name: charger id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.charger ALTER COLUMN id SET DEFAULT nextval('public.charger_id_seq'::regclass);


--
-- TOC entry 4137 (class 2604 OID 18892)
-- Name: fleet id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fleet ALTER COLUMN id SET DEFAULT nextval('public.fleet_id_seq'::regclass);


--
-- TOC entry 4139 (class 2604 OID 18893)
-- Name: maintenance id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maintenance ALTER COLUMN id SET DEFAULT nextval('public.maintenance_id_seq'::regclass);


--
-- TOC entry 4144 (class 2604 OID 18894)
-- Name: refuel_inf id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refuel_inf ALTER COLUMN id SET DEFAULT nextval('public.refuel_inf_id_seq'::regclass);


--
-- TOC entry 4147 (class 2604 OID 18895)
-- Name: veh_daily id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_daily ALTER COLUMN id SET DEFAULT nextval('public.veh_daily_id_seq'::regclass);


--
-- TOC entry 4151 (class 2604 OID 18896)
-- Name: veh_tel id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_tel ALTER COLUMN id SET DEFAULT nextval('public.veh_tel_id_seq'::regclass);


--
-- TOC entry 4155 (class 2604 OID 18897)
-- Name: vehicle id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vehicle ALTER COLUMN id SET DEFAULT nextval('public.vehicle_id_seq'::regclass);


--
-- TOC entry 4343 (class 0 OID 18836)
-- Dependencies: 223
-- Data for Name: charger; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.charger (id, fleet_id, charger, charger_type, connector_type, max_power_output, dedicated_use) FROM stdin;
1	1	charger 5	3	2	480	t
2	1	charger 6	3	2	480	t
3	2	T184-US2-0624-021	3	2, 3	180	t
4	2	T184-US2-0724-000	3	2, 3	180	t
5	3	Heliox 60 kW	3	6	60	t
6	4	C03P1	3	2	600	t
7	4	C03P2	3	2	600	t
8	4	C03P3	3	2	600	t
9	4	C03P4	3	2	600	t
10	4	C03P5	3	2	600	t
11	4	C03P6	3	2	600	t
12	4	C03P7	3	2	600	t
13	4	C03P8	3	2	600	t
\.


--
-- TOC entry 4345 (class 0 OID 18843)
-- Dependencies: 225
-- Data for Name: fleet; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.fleet (id, fleet_name, fleet_size, zev_tot, zev_grant, charger_grant, depot_adr, vendor_name, latitude, longitude, depot_loc) FROM stdin;
1	SQ Trucking	26	4	4	2	FedEx Station - 300 Tomlinson Dr.  Zelienople, PA 16063	XoS Trucks - XoSphere Platform	40.759472349497834	-80.10481645215879	\N
2	Watsontown Trucking	462	15	5	2	85 Belford Blvd., Milton, PA  17847	Zonar	40.97562934595829	-76.85064869404343	\N
3	Wilsbach Distributors	1	1	1	1	1977 Oberlin Road, Harrisburg, PA 17111	Omnitracs	40.237637035443406	-76.78240832269996	\N
4	Freight Equipment Leasing	500	0	6	8	5641 Grayson Road, Harrisburg, PA 17111	Maven, Geotab, Everse	40.25864993142325	-76.78863089506851	\N
5	Pro Disposal	72	4	12	\N	243 Rubisch Road - Ebensburg PA 15931\n	Verizon Connect \n	40.47363518791422	-78.70294344654337	\N
\.


--
-- TOC entry 4347 (class 0 OID 18850)
-- Dependencies: 227
-- Data for Name: maintenance; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.maintenance (id, date, maint_ob, maint_categ, problem, work_perf, maint_type, enter_shop, exit_shop, enter_odo, exit_odo, parts_cost, labor_cost, add_cost, warranty, fleet_id, charger_id, vehicle_id) FROM stdin;
\.


--
-- TOC entry 4349 (class 0 OID 18860)
-- Dependencies: 229
-- Data for Name: refuel_inf; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.refuel_inf (id, charger_id, veh_id, charger_type, connect_time, disconnect_time, refuel_start, refuel_end, avg_power, max_power, tot_energy, start_soc, end_soc) FROM stdin;
\.


--
-- TOC entry 4132 (class 0 OID 18070)
-- Dependencies: 219
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text) FROM stdin;
\.


--
-- TOC entry 4351 (class 0 OID 18866)
-- Dependencies: 231
-- Data for Name: veh_daily; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.veh_daily (id, veh_id, date, trip_num, init_odo, final_odo, tot_dist, tot_dura, idle_time, init_soc, final_soc, tot_soc_used, tot_energy, peak_payload) FROM stdin;
\.


--
-- TOC entry 4353 (class 0 OID 18873)
-- Dependencies: 233
-- Data for Name: veh_tel; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.veh_tel (id, veh_id, "timestamp", elevation, speed, mileage, soc, key_on_time, latitude, longitude, location) FROM stdin;
\.


--
-- TOC entry 4355 (class 0 OID 18882)
-- Dependencies: 235
-- Data for Name: vehicle; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.vehicle (id, fleet_id, fleet_vehicle_id, make, model, year, class, curb_wt, gross_wt, rated_cap, nominal_range, nominal_eff, battery_chem, peak_power, peak_torque, towing_cap, vocation) FROM stdin;
5	2	4V4WB9KN4PN624693	Volvo	VNRE62T30	2023	8	19287	82000	565	220	\N	1	340	5492	82000	Pickup/Delivery/Short Haul
6	2	4V4WB9KN6PN624694	Volvo	VNRE62T30	2023	8	19287	82000	565	220	\N	1	340	5492	82000	Pickup/Delivery/Short Haul
7	2	4V4WB9KNXPN624696	Volvo	VNRE62T30	2023	8	19287	82000	565	220	\N	1	340	5492	82000	Pickup/Delivery/Short Haul
8	2	5V4H4LN23RH248484	Autocar	E-ACTT (Terminal Tractor)	2024	8	35000	81000	210	\N	\N	1	150	4500	81000	Drayage/Yard Jockey
9	2	5V4H4LN25RH248485	Autocar	E-ACTT (Terminal Tractor)	2024	8	35000	81000	210	\N	\N	1	150	4500	81000	Drayage/Yard Jockey
10	3	V62-2023-01	Volvo	VNRE62T300	2023	8	21912	82000	565	190	2.93	2	340	4051	46200	Beverage Distribution
11	4	DSE175	Freightliner	Cascadia DC 4x2	2025	8	19112	65000	438	230	1.90	1	438	31184	32071	delivery/freight hauling
12	4	DSE176	Freightliner	Cascadia DC 4x2	2025	8	19112	65000	438	230	1.90	1	438	31184	32071	delivery/freight hauling
13	4	DSE177	Freightliner	Cascadia DC 4x2	2025	8	19112	65000	438	230	1.90	1	438	31184	32071	delivery/freight hauling
14	4	SSE26116	Mack	MDe 7	2025	7	17475	33000	240	230	1.04	1	195	2508	15925	delivery/freight hauling
15	4	SE28500	Mack	MDe 7	2025	7	17475	33000	240	230	1.04	1	195	2508	15925	delivery/freight hauling
16	4	SE28501	Mack	MDe 7	2025	7	17475	33000	240	230	1.04	1	195	2508	15925	delivery/freight hauling
17	5	BEV1	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
18	5	BEV2	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
19	5	BEV3	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
20	5	BEV4	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
21	5	BEV5	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
22	5	BEV6	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
23	5	BEV7	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
24	5	BEV8	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
25	5	BEV9	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
26	5	BEV10	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
27	5	BEV11	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
28	5	BEV12	Battle Motors	LNT2	204	8	44000	72380	396	225	160.00	1	570	1991	\N	Refuse/Recycling
1	1	530043	XoS	SV05	2024	6	16000	2300	140	120	1.17	1	347	1737	\N	Courier Services
2	1	530160	XoS	SV05	2024	6	16000	2300	140	120	1.17	1	347	1737	\N	Courier Services
3	1	532213	XoS	SV05	2024	6	16000	2300	140	120	1.17	1	347	1737	\N	Courier Services
4	1	532234	XoS	SV05	2024	6	16000	2300	140	120	1.17	1	347	1737	\N	Courier Services
\.


--
-- TOC entry 4370 (class 0 OID 0)
-- Dependencies: 224
-- Name: charger_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.charger_id_seq', 13, true);


--
-- TOC entry 4371 (class 0 OID 0)
-- Dependencies: 226
-- Name: fleet_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.fleet_id_seq', 1, true);


--
-- TOC entry 4372 (class 0 OID 0)
-- Dependencies: 228
-- Name: maintenance_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.maintenance_id_seq', 1, false);


--
-- TOC entry 4373 (class 0 OID 0)
-- Dependencies: 230
-- Name: refuel_inf_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.refuel_inf_id_seq', 1, false);


--
-- TOC entry 4374 (class 0 OID 0)
-- Dependencies: 232
-- Name: veh_daily_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.veh_daily_id_seq', 1, false);


--
-- TOC entry 4375 (class 0 OID 0)
-- Dependencies: 234
-- Name: veh_tel_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.veh_tel_id_seq', 1, false);


--
-- TOC entry 4376 (class 0 OID 0)
-- Dependencies: 236
-- Name: vehicle_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.vehicle_id_seq', 28, true);


--
-- TOC entry 4165 (class 2606 OID 18899)
-- Name: charger charger_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.charger
    ADD CONSTRAINT charger_pkey PRIMARY KEY (id);


--
-- TOC entry 4167 (class 2606 OID 18901)
-- Name: fleet fleet_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fleet
    ADD CONSTRAINT fleet_pkey PRIMARY KEY (id);


--
-- TOC entry 4171 (class 2606 OID 18903)
-- Name: maintenance maintenance_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maintenance
    ADD CONSTRAINT maintenance_pkey PRIMARY KEY (id);


--
-- TOC entry 4175 (class 2606 OID 18905)
-- Name: refuel_inf refuel_inf_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refuel_inf
    ADD CONSTRAINT refuel_inf_pkey PRIMARY KEY (id);


--
-- TOC entry 4177 (class 2606 OID 18907)
-- Name: veh_daily veh_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_daily
    ADD CONSTRAINT veh_daily_pkey PRIMARY KEY (id);


--
-- TOC entry 4180 (class 2606 OID 18909)
-- Name: veh_tel veh_tel_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_tel
    ADD CONSTRAINT veh_tel_pkey PRIMARY KEY (id);


--
-- TOC entry 4183 (class 2606 OID 18911)
-- Name: vehicle vehicle_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vehicle
    ADD CONSTRAINT vehicle_pkey PRIMARY KEY (id);


--
-- TOC entry 4168 (class 1259 OID 18912)
-- Name: idx_maintenance_charger_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_maintenance_charger_id ON public.maintenance USING btree (charger_id);


--
-- TOC entry 4169 (class 1259 OID 18913)
-- Name: idx_maintenance_vehicle_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_maintenance_vehicle_id ON public.maintenance USING btree (vehicle_id);


--
-- TOC entry 4172 (class 1259 OID 18914)
-- Name: idx_refuel_inf_charger_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_refuel_inf_charger_id ON public.refuel_inf USING btree (charger_id);


--
-- TOC entry 4173 (class 1259 OID 18915)
-- Name: idx_refuel_inf_veh_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_refuel_inf_veh_id ON public.refuel_inf USING btree (veh_id);


--
-- TOC entry 4178 (class 1259 OID 18916)
-- Name: idx_veh_tel_veh_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_veh_tel_veh_id ON public.veh_tel USING btree (veh_id);


--
-- TOC entry 4181 (class 1259 OID 18917)
-- Name: idx_vehicle_fleet_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_vehicle_fleet_id ON public.vehicle USING btree (fleet_id);


--
-- TOC entry 4184 (class 2606 OID 18918)
-- Name: charger fk_charger_fleet1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.charger
    ADD CONSTRAINT fk_charger_fleet1 FOREIGN KEY (fleet_id) REFERENCES public.fleet(id);


--
-- TOC entry 4190 (class 2606 OID 18923)
-- Name: veh_daily fk_daily_veh_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_daily
    ADD CONSTRAINT fk_daily_veh_id FOREIGN KEY (veh_id) REFERENCES public.vehicle(id);


--
-- TOC entry 4185 (class 2606 OID 18928)
-- Name: maintenance fk_maintenance_charger1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maintenance
    ADD CONSTRAINT fk_maintenance_charger1 FOREIGN KEY (charger_id) REFERENCES public.charger(id);


--
-- TOC entry 4186 (class 2606 OID 18933)
-- Name: maintenance fk_maintenance_fleet1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maintenance
    ADD CONSTRAINT fk_maintenance_fleet1 FOREIGN KEY (fleet_id) REFERENCES public.fleet(id);


--
-- TOC entry 4187 (class 2606 OID 18938)
-- Name: maintenance fk_maintenance_vehicle1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.maintenance
    ADD CONSTRAINT fk_maintenance_vehicle1 FOREIGN KEY (vehicle_id) REFERENCES public.vehicle(id);


--
-- TOC entry 4188 (class 2606 OID 18943)
-- Name: refuel_inf fk_refuel_charger_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refuel_inf
    ADD CONSTRAINT fk_refuel_charger_id FOREIGN KEY (charger_id) REFERENCES public.charger(id);


--
-- TOC entry 4189 (class 2606 OID 18948)
-- Name: refuel_inf fk_refuel_veh_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.refuel_inf
    ADD CONSTRAINT fk_refuel_veh_id FOREIGN KEY (veh_id) REFERENCES public.vehicle(id);


--
-- TOC entry 4191 (class 2606 OID 18953)
-- Name: veh_tel fk_telematic_veh_id; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.veh_tel
    ADD CONSTRAINT fk_telematic_veh_id FOREIGN KEY (veh_id) REFERENCES public.vehicle(id) ON UPDATE RESTRICT ON DELETE RESTRICT;


--
-- TOC entry 4192 (class 2606 OID 18958)
-- Name: vehicle fk_vehicle_fleet1; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vehicle
    ADD CONSTRAINT fk_vehicle_fleet1 FOREIGN KEY (fleet_id) REFERENCES public.fleet(id);


-- Completed on 2025-05-26 17:53:07 EDT

--
-- PostgreSQL database dump complete
--

