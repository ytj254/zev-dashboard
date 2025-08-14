/* ========================================
   SAFETY CHECKS (Type-Aware)
   ======================================== */

-- veh_daily
DO $$
DECLARE missing_count int;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='veh_daily'
      AND column_name='veh_id' AND data_type LIKE 'character%'
  ) THEN
    SELECT COUNT(*) INTO missing_count
    FROM veh_daily d
    LEFT JOIN vehicle v
      ON v.fleet_vehicle_id = d.veh_id
    WHERE v.id IS NULL;
  ELSE
    SELECT COUNT(*) INTO missing_count
    FROM veh_daily d
    LEFT JOIN vehicle v
      ON v.id = d.veh_id
    WHERE v.id IS NULL;
  END IF;

  IF missing_count > 0 THEN
    RAISE EXCEPTION 'veh_daily has % unmapped vehicle IDs. Fix before migration.', missing_count;
  END IF;
END$$;


-- refuel_inf (veh_id)
DO $$
DECLARE missing_count int;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='refuel_inf'
      AND column_name='veh_id' AND data_type LIKE 'character%'
  ) THEN
    SELECT COUNT(*) INTO missing_count
    FROM refuel_inf r
    LEFT JOIN vehicle v
      ON v.fleet_vehicle_id = r.veh_id
    WHERE v.id IS NULL;
  ELSE
    SELECT COUNT(*) INTO missing_count
    FROM refuel_inf r
    LEFT JOIN vehicle v
      ON v.id = r.veh_id
    WHERE v.id IS NULL;
  END IF;

  IF missing_count > 0 THEN
    RAISE EXCEPTION 'refuel_inf has % unmapped vehicle IDs. Fix before migration.', missing_count;
  END IF;
END$$;


-- refuel_inf (charger_id)
DO $$
DECLARE missing_count int;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='refuel_inf'
      AND column_name='charger_id' AND data_type LIKE 'character%'
  ) THEN
    SELECT COUNT(*) INTO missing_count
    FROM refuel_inf r
    LEFT JOIN charger c
      ON c.charger = r.charger_id
    WHERE c.id IS NULL;
  ELSE
    SELECT COUNT(*) INTO missing_count
    FROM refuel_inf r
    LEFT JOIN charger c
      ON c.id = r.charger_id
    WHERE c.id IS NULL;
  END IF;

  IF missing_count > 0 THEN
    RAISE EXCEPTION 'refuel_inf has % unmapped charger IDs. Fix before migration.', missing_count;
  END IF;
END$$;


/* ========================================
   ENSURE PER-FLEET UNIQUENESS
   ======================================== */
CREATE UNIQUE INDEX IF NOT EXISTS ux_vehicle_fleet_code
  ON public.vehicle(fleet_id, fleet_vehicle_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_charger_fleet_name
  ON public.charger(fleet_id, charger);


/* ========================================
   MIGRATIONS (only if still varchar)
   ======================================== */

-- veh_daily
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='veh_daily'
      AND column_name='veh_id' AND data_type LIKE 'character%'
  ) THEN
    RAISE NOTICE 'Migrating veh_daily.veh_id from varchar to int...';
    ALTER TABLE public.veh_daily ADD COLUMN veh_id_int integer;
    UPDATE public.veh_daily d
    SET    veh_id_int = v.id
    FROM   public.vehicle v
    WHERE  v.fleet_vehicle_id = d.veh_id;
    ALTER TABLE public.veh_daily ALTER COLUMN veh_id_int SET NOT NULL;
    ALTER TABLE public.veh_daily
      ADD CONSTRAINT fk_veh_daily_veh_id_int
      FOREIGN KEY (veh_id_int) REFERENCES public.vehicle(id);
    CREATE UNIQUE INDEX IF NOT EXISTS uq_veh_daily_vehid_date
      ON public.veh_daily (veh_id_int, date);
    ALTER TABLE public.veh_daily DROP CONSTRAINT IF EXISTS fk_daily_veh_id;
    ALTER TABLE public.veh_daily DROP COLUMN veh_id;
    ALTER TABLE public.veh_daily RENAME COLUMN veh_id_int TO veh_id;
  ELSE
    RAISE NOTICE 'veh_daily already uses int veh_id. Skipping.';
  END IF;
END$$;


-- refuel_inf
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='refuel_inf'
      AND column_name='veh_id' AND data_type LIKE 'character%'
  ) THEN
    RAISE NOTICE 'Migrating refuel_inf IDs from varchar to int...';
    ALTER TABLE public.refuel_inf
      ADD COLUMN veh_id_int integer,
      ADD COLUMN charger_id_int integer;
    UPDATE public.refuel_inf r
    SET    veh_id_int = v.id
    FROM   public.vehicle v
    WHERE  v.fleet_vehicle_id = r.veh_id;
    UPDATE public.refuel_inf r
    SET    charger_id_int = c.id
    FROM   public.charger c
    WHERE  c.charger = r.charger_id;
    ALTER TABLE public.refuel_inf
      ALTER COLUMN veh_id_int SET NOT NULL,
      ALTER COLUMN charger_id_int SET NOT NULL;
    ALTER TABLE public.refuel_inf
      ADD CONSTRAINT fk_refuel_veh_id_int     FOREIGN KEY (veh_id_int)     REFERENCES public.vehicle(id),
      ADD CONSTRAINT fk_refuel_charger_id_int FOREIGN KEY (charger_id_int) REFERENCES public.charger(id);
    CREATE INDEX IF NOT EXISTS idx_refuel_inf_veh_id_int     ON public.refuel_inf(veh_id_int);
    CREATE INDEX IF NOT EXISTS idx_refuel_inf_charger_id_int ON public.refuel_inf(charger_id_int);
    ALTER TABLE public.refuel_inf DROP CONSTRAINT IF EXISTS fk_refuel_veh_id;
    ALTER TABLE public.refuel_inf DROP CONSTRAINT IF EXISTS fk_refuel_charger_id;
    ALTER TABLE public.refuel_inf DROP COLUMN veh_id;
    ALTER TABLE public.refuel_inf DROP COLUMN charger_id;
    ALTER TABLE public.refuel_inf RENAME COLUMN veh_id_int     TO veh_id;
    ALTER TABLE public.refuel_inf RENAME COLUMN charger_id_int TO charger_id;
  ELSE
    RAISE NOTICE 'refuel_inf already uses int veh_id and charger_id. Skipping.';
  END IF;
END$$;


-- veh_tel
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='veh_tel'
      AND column_name='veh_id' AND data_type LIKE 'character%'
  ) THEN
    RAISE NOTICE 'Migrating veh_tel.veh_id from varchar to int...';
    ALTER TABLE public.veh_tel ADD COLUMN veh_id_int integer;
    UPDATE public.veh_tel t
    SET    veh_id_int = v.id
    FROM   public.vehicle v
    WHERE  v.fleet_vehicle_id = t.veh_id;
    ALTER TABLE public.veh_tel ALTER COLUMN veh_id_int SET NOT NULL;
    ALTER TABLE public.veh_tel
      ADD CONSTRAINT fk_telematic_veh_id_int
      FOREIGN KEY (veh_id_int) REFERENCES public.vehicle(id);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_veh_tel_vehid_ts
      ON public.veh_tel (veh_id_int, "timestamp");
    ALTER TABLE public.veh_tel DROP CONSTRAINT IF EXISTS fk_telematic_veh_id;
    ALTER TABLE public.veh_tel DROP COLUMN veh_id;
    ALTER TABLE public.veh_tel RENAME COLUMN veh_id_int TO veh_id;
  ELSE
    RAISE NOTICE 'veh_tel already uses int veh_id. Skipping.';
  END IF;
END$$;
