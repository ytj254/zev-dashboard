# Database Cheat Sheet — `zevperf`
_Generated: 2025-08-08 16:30:03_  
_Engine: PostgreSQL 17.5_

---

## Tables (public)

### `charger`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('charger_id_seq'::regclass)
- `fleet_id` integer NOT NULL
- `charger` character varying DEFAULT NULL::character varying
- `charger_type` character varying DEFAULT NULL::character varying
- `connector_type` character varying DEFAULT NULL::character varying
- `max_power_output` integer
- `dedicated_use` boolean

**Uniques**:
- UNIQUE(charger)

**Foreign keys**:
- FK (fleet_id) → fleet(id)

### `fleet`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('fleet_id_seq'::regclass)
- `fleet_name` character varying DEFAULT NULL::character varying
- `fleet_size` integer
- `zev_tot` integer
- `zev_grant` integer
- `charger_grant` integer
- `depot_adr` text
- `vendor_name` text
- `latitude` double precision
- `longitude` double precision
- `depot_loc` USER-DEFINED (PostGIS)

### `maintenance`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('maintenance_id_seq'::regclass)
- `date` date
- `maint_ob` integer
- `maint_categ` character varying DEFAULT NULL::character varying
- `problem` text
- `work_perf` text
- `maint_type` integer
- `enter_shop` timestamp
- `exit_shop` timestamp
- `enter_odo` integer
- `exit_odo` integer
- `parts_cost` numeric DEFAULT NULL::numeric
- `labor_cost` numeric DEFAULT NULL::numeric
- `add_cost` numeric DEFAULT NULL::numeric
- `warranty` boolean
- `fleet_id` integer NOT NULL
- `charger_id` integer
- `vehicle_id` integer

**Foreign keys**:
- FK (charger_id) → charger(id)
- FK (fleet_id) → fleet(id)
- FK (vehicle_id) → vehicle(id)

### `refuel_inf`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('refuel_inf_id_seq'::regclass)
- `charger_id` character varying NOT NULL
- `veh_id` character varying
- `connect_time` timestamp
- `disconnect_time` timestamp
- `refuel_start` timestamp
- `refuel_end` timestamp
- `avg_power` numeric
- `max_power` numeric
- `tot_energy` numeric
- `start_soc` numeric DEFAULT NULL::numeric
- `end_soc` numeric DEFAULT NULL::numeric

**Foreign keys**:
- FK (charger_id) → charger(charger)
- FK (veh_id) → vehicle(fleet_vehicle_id)

### `spatial_ref_sys`
**Primary key**: srid

**Columns**:
- `srid` integer NOT NULL
- `auth_name` character varying
- `auth_srid` integer
- `srtext` character varying
- `proj4text` character varying

### `veh_daily`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('veh_daily_id_seq'::regclass)
- `veh_id` character varying NOT NULL
- `date` date
- `trip_num` integer
- `init_odo` integer
- `final_odo` integer
- `tot_dist` numeric
- `tot_dura` numeric
- `idle_time` integer
- `init_soc` numeric DEFAULT NULL::numeric
- `final_soc` numeric DEFAULT NULL::numeric
- `tot_soc_used` numeric DEFAULT NULL::numeric
- `tot_energy` integer
- `peak_payload` integer

**Foreign keys**:
- FK (veh_id) → vehicle(fleet_vehicle_id)

### `veh_tel`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('veh_tel_id_seq'::regclass)
- `veh_id` integer NOT NULL
- `timestamp` timestamp with time zone
- `elevation` numeric DEFAULT NULL::numeric
- `speed` integer
- `mileage` integer
- `soc` numeric DEFAULT NULL::numeric
- `key_on_time` numeric DEFAULT NULL::numeric
- `latitude` double precision
- `longitude` double precision
- `location` USER-DEFINED (PostGIS)

**Foreign keys**:
- FK (veh_id) → vehicle(id)

### `vehicle`
**Primary key**: id

**Columns**:
- `id` integer NOT NULL DEFAULT nextval('vehicle_id_seq'::regclass)
- `fleet_id` integer NOT NULL
- `fleet_vehicle_id` character varying DEFAULT NULL::character varying
- `make` character varying DEFAULT NULL::character varying
- `model` character varying DEFAULT NULL::character varying
- `year` integer
- `class` integer
- `curb_wt` integer
- `gross_wt` integer
- `rated_cap` integer
- `nominal_range` integer
- `nominal_eff` numeric DEFAULT NULL::numeric
- `battery_chem` integer
- `peak_power` integer
- `peak_torque` integer
- `towing_cap` integer
- `vocation` character varying DEFAULT NULL::character varying

**Uniques**:
- UNIQUE(fleet_vehicle_id)

**Foreign keys**:
- FK (fleet_id) → fleet(id)

---
## Quick Join Hints

- `charger`.fleet_id → `fleet`.id
- `maintenance`.charger_id → `charger`.id
- `maintenance`.fleet_id → `fleet`.id
- `maintenance`.vehicle_id → `vehicle`.id
- `refuel_inf`.charger_id → `charger`.charger
- `refuel_inf`.veh_id → `vehicle`.fleet_vehicle_id
- `veh_daily`.veh_id → `vehicle`.fleet_vehicle_id
- `veh_tel`.veh_id → `vehicle`.id
- `vehicle`.fleet_id → `fleet`.id

---
## Handy Snippets

```sql
-- Recent telematics
SELECT t."timestamp", t.speed, v.fleet_vehicle_id, f.fleet_name
FROM veh_tel t
JOIN vehicle v ON t.veh_id = v.id
JOIN fleet   f ON v.fleet_id = f.id
ORDER BY t."timestamp" DESC
LIMIT 100;

-- Daily usage with vehicle
SELECT d.date, d.tot_dist, v.make, v.model
FROM veh_daily d
JOIN vehicle v ON d.veh_id = v.fleet_vehicle_id;

-- Charging with charger & fleet
SELECT r.refuel_start, r.tot_energy, c.charger, f.fleet_name
FROM refuel_inf r
JOIN charger c ON r.charger_id = c.charger
JOIN fleet   f ON c.fleet_id = f.id;
