CREATE OR REPLACE FUNCTION
	--create info table and check if table is temporal
	t__createInfoTableAndCheckTemporal(IN _table_name TEXT) RETURNS void AS $$
DECLARE
	_o RECORD;
BEGIN
	--table with all informations about temporal tables
	CREATE TABLE IF NOT EXISTS t__infotable (table_name TEXT, valid_time BOOL, transaction_time BOOL);
	FOR _o IN
		EXECUTE format('SELECT table_name FROM t__infotable WHERE table_name = ''%I'';', _table_name)
	LOOP
		RAISE EXCEPTION 'Table % is already temporal.', _table_name;
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--drop temporal table with all dependencies
	dropTemporalTable(IN _table_name TEXT) RETURNS bool AS $$ 
DECLARE
	_o RECORD;
	_b BOOLEAN;
	_r RECORD;
	_t VARCHAR(1); --reference table (from table) type {t - transaction time, v - valid time, o - other}
BEGIN
	_t := t__getTableType(_table_name);
	IF _t = 'o' THEN
		RAISE EXCEPTION 'Table is not temporal';
	END IF;
	--delete type
	EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN) CASCADE;', _table_name);
	EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT) CASCADE;', _table_name);
	EXECUTE format('DROP FUNCTION IF EXISTS snapshot%I(TEXT) CASCADE;', _table_name);
	EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN, TIMESTAMP) CASCADE;', _table_name);
	EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT, TIMESTAMP) CASCADE;', _table_name);
	EXECUTE format('DROP TYPE IF EXISTS t__temporal_%I CASCADE', _table_name);
	EXECUTE format('DROP FUNCTION IF EXISTS restructualize_%I();', _table_name);
	--table has to be reference
	/*IF (SELECT count(*) FROM t__reference_integrity WHERE reference_table_name = _table_name) THEN
		RAISE EXCEPTION 'Reference table can not be droped. Drop referencing table first.';
	END IF;*/
	FOR _r IN
		EXECUTE format('SELECT table_name FROM t__join WHERE table_name @> array[''%s'']', _table_name)
	LOOP
		SELECT dropTemporalJoin(_r.table_name) INTO _b;
	END LOOP;
	FOR _o IN
		SELECT reference_table_name FROM t__reference_integrity WHERE referencing_table_name = _table_name
	LOOP
		EXECUTE format('DROP TABLE IF EXISTS t__%I_%I__ct CASCADE', _o.reference_table_name, _table_name);
	END LOOP;
	EXECUTE format('DROP TABLE IF EXISTS t__%I__temp_data CASCADE', _table_name);
	DELETE FROM t__reference_integrity WHERE referencing_table_name = _table_name;
	EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', _table_name);
	DELETE FROM t__infotable WHERE table_name = _table_name;
	
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--return table type b - bitemporal, v - valid time table, t - transaction time table, o - other
	t__getTableType(IN _table_name TEXT) RETURNS VARCHAR(1) AS $$
DECLARE
	_b BOOLEAN;
	_t VARCHAR(1);
BEGIN
	--{t - transaction time, v - valid time, o - other, b - bitemporal}
	_b := false;
	_t := 'o';
	FOR _b IN
		EXECUTE format('SELECT true FROM t__infotable WHERE (table_name = ''%I'' AND transaction_time = true)', _table_name)
	LOOP
		_t = 't';
	END LOOP;
	FOR _b IN
		EXECUTE format('SELECT true FROM t__infotable WHERE (table_name = ''%I'' AND valid_time = true)', _table_name)
	LOOP
		_t = 'v';
	END LOOP;
	FOR _b IN
		EXECUTE format('SELECT true FROM t__infotable WHERE (table_name = ''%I'' AND valid_time = true AND transaction_time = true)', _table_name)
	LOOP
		_t = 'b';
	END LOOP;

	RETURN _t;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--check transaction time table to transaction time table reference integrity
	t__checkTTRI(IN _table_name NAME, IN _oid OID, IN _new_oid OID) RETURNS bool AS $$ 
DECLARE
	_o RECORD;
	_r RECORD;
BEGIN
	--check referencing table to update
	--for all referencing tables
	FOR _o IN
		EXECUTE format('SELECT referencing_table_name FROM t__reference_integrity WHERE reference_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			--deleted record cant be paired
			EXECUTE format('SELECT referencing_table_row FROM t__%I_%I__ct WHERE 
				reference_table_row = %L AND 
				referencing_table_row IN (SELECT row_oid FROM t__%I__temp_data WHERE now() < next(transaction_time))', 
					_table_name, _o.referencing_table_name, _oid, _o.referencing_table_name)
		LOOP
			EXECUTE format('INSERT INTO t__%I_%I__ct (reference_table_row, referencing_table_row) VALUES (%L, %L)', 
					_table_name, _o.referencing_table_name, _new_oid, _r.referencing_table_row);
		END LOOP;
	END LOOP;
	--check reference table to update
	FOR _o IN
		EXECUTE format('SELECT reference_table_name FROM t__reference_integrity WHERE referencing_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			--deleted record cant be paired
			EXECUTE format('SELECT reference_table_row FROM t__%I_%I__ct WHERE 
				referencing_table_row = %L AND 
				reference_table_row IN (SELECT row_oid FROM t__%I__temp_data WHERE now() < next(transaction_time))', 
					_o.reference_table_name, _table_name, _oid, _o.reference_table_name)
		LOOP
			EXECUTE format('INSERT INTO t__%I_%I__ct (referencing_table_row, reference_table_row) VALUES (%L, %L)', 
					_o.reference_table_name, _table_name, _new_oid, _r.reference_table_row);
		END LOOP;
	END LOOP;
	
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--check bitemporal table to bitemporal time table reference integrity
	t__checkBTRI(IN _table_name NAME, IN _oid OID, IN _new_oid OID) RETURNS bool AS $$ 
DECLARE
	_o RECORD;
	_r RECORD;
BEGIN
	--check referencing table to update
	--for all referencing tables
	FOR _o IN
		EXECUTE format('SELECT referencing_table_name FROM t__reference_integrity WHERE reference_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			EXECUTE format('SELECT referencing_table_row FROM t__%I_%I__ct WHERE 
				reference_table_row = %L AND 
				referencing_table_row IN (SELECT row_oid FROM t__%I__temp_data WHERE now() < next(transaction_time))', 
					_table_name, _o.referencing_table_name, _oid, _o.referencing_table_name)
		LOOP
			EXECUTE format('INSERT INTO t__%I_%I__ct (reference_table_row, referencing_table_row) VALUES (%L, %L)', 
					_table_name, _o.referencing_table_name, _new_oid, _r.referencing_table_row);
		END LOOP;
	END LOOP;
	--check reference table to update
	FOR _o IN
		EXECUTE format('SELECT reference_table_name FROM t__reference_integrity WHERE referencing_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			EXECUTE format('SELECT reference_table_row FROM t__%I_%I__ct WHERE 
				referencing_table_row = %L AND 
				reference_table_row IN (SELECT row_oid FROM t__%I__temp_data WHERE now() < next(transaction_time))', 
					_o.reference_table_name, _table_name, _oid, _o.reference_table_name)
		LOOP
			EXECUTE format('INSERT INTO t__%I_%I__ct (referencing_table_row, reference_table_row) VALUES (%L, %L)', 
					_o.reference_table_name, _table_name, _new_oid, _r.reference_table_row);
		END LOOP;
	END LOOP;
	
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--check bitemporal table to bitemporal time table reference integrity
	t__checkBTOverlaps(IN _table_name NAME, IN _valid_time PERIOD, _old_oid OID) RETURNS bool AS $$ 
DECLARE
	_o RECORD;
	_r RECORD;
	_rec RECORD;
	_p PERIOD;
	_vt PERIOD[];
BEGIN
	--check referencing table to update
	--for all referencing tables
	FOR _o IN
		EXECUTE format('SELECT referencing_table_name FROM t__reference_integrity WHERE reference_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			EXECUTE format('SELECT referencing_table_row FROM t__%I_%I__ct WHERE reference_table_row = %L', 
					_table_name, _o.referencing_table_name, _old_oid)
		LOOP
			_vt := array[]::period[];
			FOR _rec IN
				EXECUTE format('SELECT valid_time FROM t__%I__temp_data WHERE now() < last(transaction_time)', _o.referencing_table_name)
			LOOP
				_vt := array_append(_vt, _rec.valid_time);
			END LOOP;
			IF _vt = array[]::period[] THEN
				RETURN true;
			END IF;
			_vt := t__restructualize(_vt);
			--check if overlaps
			FOREACH _p IN ARRAY _vt LOOP
				IF overlaps(_p, _valid_time) = false THEN
					RAISE EXCEPTION 'Forbiden operation. Inserted references are not overlaps.';
				END IF;
			END LOOP;
			RETURN true;
		END LOOP;
	END LOOP;
	--check reference table to update
	FOR _o IN
		EXECUTE format('SELECT reference_table_name FROM t__reference_integrity WHERE referencing_table_name = ''%I''', _table_name)
	LOOP
		--for all couples duplicate with new reference oid
		FOR _r IN
			EXECUTE format('SELECT reference_table_row FROM t__%I_%I__ct WHERE referencing_table_row = %L', 
					_o.reference_table_name, _table_name, _old_oid)
		LOOP
			_vt := array[]::period[];
			FOR _rec IN
				EXECUTE format('SELECT valid_time FROM t__%I__temp_data WHERE now() < last(transaction_time)', _o.reference_table_name)
			LOOP
				_vt := array_append(_vt, _rec.valid_time);
			END LOOP;
			IF _vt = array[]::period[] THEN
				RETURN true;
			END IF;
			_vt := t__restructualize(_vt);
			--check if overlaps
			FOREACH _p IN ARRAY _vt LOOP
				IF overlaps(_p, _valid_time) = false THEN
					RAISE EXCEPTION 'Forbiden operation. Inserted references are not overlaps.';
				END IF;
			END LOOP;
			RETURN true;
		END LOOP;
	END LOOP;
	
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION
	--restructualize array of periods and returns array of restructualized periods
	t__restructualize(IN _times period[]) RETURNS period[] AS $$
DECLARE
	_t1 period;
	_t2 period;
	_new_time period;
	_new_times period[];
	_change boolean;
BEGIN
	_new_times := array[]::period[];
	loop
		_change := false;
		foreach _t1 in array _times loop
			_new_time := _t1;
			--return -inf, inf
			if equals(_new_time, period('-infinity'::timestamp, 'infinity'::timestamp)) then
				return array[_new_time];
			end if;
			foreach _t2 in array _times loop
				if nequals(_t1, _t2) then
					--right timestamp of period touches or is contained by second period
					if (contains(_t2, last(_new_time)) or last(_new_time) = first(_t2)) and last(_t2) > last(_new_time) then
						_new_time := period(first(_new_time), next(_t2));
						_change := true;
					end if;
					--left timestamp of period touches or is contained by second period
					if (contains(_t2, first(_new_time)) or first(_new_time) = last(_t2)) and last(_new_time) > last(_t2) then
						_new_time := period(first(_t2), next(_new_time));
						_change := true;
					end if;
					--special for infinity
					if first(_t1) = '-infinity'::timestamp and first(_t2) = '-infinity'::timestamp then
						if next(_t1) > next(_t2) then
							_new_time := period('-infinity'::timestamp, next(_t1));
						else
							_new_time := period('-infinity'::timestamp, next(_t2));
						end if;
						_change := true;
					end if;
					if last(_t1) = 'infinity'::timestamp and last(_t2) = 'infinity'::timestamp then
						if first(_t1) < first(_t2) then
							_new_time := period(first(_t1), 'infinity'::timestamp);
						else
							_new_time := period(first(_t2), 'infinity'::timestamp);
						end if;
						_change := true;
					end if;
				end if;
			end loop;
			--test on duplicates and add edited period
			if not(_new_times @> array[_new_time]) then
				_new_times := array_append(_new_times, _new_time);
			end if;
		end loop;
		--no more changes in output array
		if _change = false then
			exit;
		end if;
		--actualize period array
		_times := _new_times;
		_new_times := array[]::period[];
	end loop;

	return _times;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--create type and restructualize function for table_name table
	t__create_restructualize(IN _table_name TEXT) RETURNS boolean AS $$
DECLARE
	_b BOOLEAN;
	_t TEXT;
	_i INTEGER;
	_r RECORD;
	_ft VARCHAR(1);
BEGIN
	_ft := t__getTableType(_table_name);
	IF _ft = 'v' THEN
		_t := format('CREATE TYPE t__temporal_%I AS (', _table_name);
		_b := TRUE;
		--all columns are contained in datatype
		FOR _r IN
			EXECUTE format('SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %L', _table_name)
		LOOP
			IF _b = FALSE THEN
				_t := _t || ', ';
			END IF;
			IF _b = TRUE THEN
				_b := FALSE;
			END IF;
			_t := _t || _r.column_name || ' ' || _r.data_type;
		END LOOP;
		_t := _t || ', valid_time period);';
		_i := 0;
		EXECUTE format('SELECT count(*) FROM pg_type WHERE typname = ''t__temporal_%I''', _table_name) INTO _i;
		--drop existing type
		IF _i = 1 THEN
			EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS snapshot%I(TEXT) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN, TIMESTAMP) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT, TIMESTAMP) CASCADE;', _table_name);
			EXECUTE format('DROP TYPE t__temporal_%I', _table_name);
		END IF;
		--actualized type
		EXECUTE _t;
		--sequenced select
		EXECUTE format('
			CREATE OR REPLACE FUNCTION
				sequenced%I(IN _restructualize BOOLEAN DEFAULT false) RETURNS setof t__temporal_%I AS $t__temporal_%I$
			DECLARE
				_r %I%%ROWTYPE;
				_rec t__temporal_%I%%ROWTYPE;
				_t PERIOD;
				_times PERIOD[];
				_oid OID;
			BEGIN
				IF _restructualize = false THEN
					FOR _rec IN
						SELECT %s.*, t__%s__temp_data.valid_time FROM %s JOIN t__%s__temp_data ON %s.oid = t__%s__temp_data.row_oid
					LOOP
						RETURN NEXT _rec;
					END LOOP;
				ELSE
					FOR _oid IN
						--get all oid to unique row; min function get one oid of oids with equal rows
						SELECT min(oid) AS oid FROM %I GROUP BY ROW(%I.*)
					LOOP
						_times := array[]::period[];
						FOR _t IN
							--select valid_time in duplicit table rows
							EXECUTE format(''SELECT valid_time 
									FROM t__%I__temp_data 
									WHERE row_oid in 
										(SELECT oid 
										FROM %I 
										WHERE ROW(%I.*) in 
											(SELECT * 
											FROM %I 
											WHERE oid = %%L))'', _oid)
						LOOP
							_times := array_append(_times, _t);
						END LOOP;
						_times := t__restructualize(_times);
						
						
						SELECT * FROM %I WHERE oid = _oid INTO _r;
						FOREACH _t IN array _times LOOP
							_rec := ROW(_r.*, _t);
							RETURN next _rec;
						END LOOP;
					END LOOP;
				END IF;
			END;
			$t__temporal_%I$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
		--nonsequenced select
		EXECUTE format('
			CREATE OR REPLACE FUNCTION
				nonsequenced%I(_where TEXT DEFAULT '''') RETURNS setof %I AS $%I$
			DECLARE
				_q TEXT;
				_c TEXT;
			BEGIN
				_q := ''SELECT '';
				FOR _c IN
					SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %L
				LOOP
					_q := _q || _c || '', '';
				END LOOP;
				_q := left(_q, length(_q)-2);
				_q := _q || '' FROM sequenced%I()'';
				IF _where <> '''' THEN
					_q := _q || '' WHERE '' || _where;
				END IF;
				RETURN QUERY EXECUTE _q;
			END;
			$%I$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
	END IF;
	IF _ft = 't' THEN	
		--drop existing type
		EXECUTE format('DROP FUNCTION IF EXISTS snapshot%I(TIMESTAMP);', _table_name);
		--snapshot select
		EXECUTE format('
			CREATE OR REPLACE FUNCTION
				snapshot%I(IN _time TIMESTAMP DEFAULT now()::timestamp) RETURNS setof %I AS $%I$
			DECLARE
				_r %I%%ROWTYPE;
			BEGIN
				FOR _r IN
					EXECUTE format(''SELECT * FROM %I WHERE oid IN (SELECT row_oid FROM t__%I__temp_data WHERE contains(transaction_time, ''''%%s''''::timestamp))'', _time)
				LOOP
					RETURN NEXT _r;
				END LOOP;
			END;
			$%I$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
	END IF;
	IF _ft = 'b' THEN
		_t := format('CREATE TYPE t__temporal_%I AS (', _table_name);
		_b := TRUE;
		--all columns are contained in datatype
		FOR _r IN
			EXECUTE format('SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %L', _table_name)
		LOOP
			IF _b = FALSE THEN
				_t := _t || ', ';
			END IF;
			IF _b = TRUE THEN
				_b := FALSE;
			END IF;
			_t := _t || _r.column_name || ' ' || _r.data_type;
		END LOOP;
		_t := _t || ', valid_time period);';
		_i := 0;
		EXECUTE format('SELECT count(*) FROM pg_type WHERE typname = ''t__temporal_%I''', _table_name) INTO _i;
		--drop existing type
		IF _i = 1 THEN
			EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN, TIMESTAMP) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT, TIMESTAMP) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I(BOOLEAN) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I(TEXT) CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS sequenced%I() CASCADE;', _table_name);
			EXECUTE format('DROP FUNCTION IF EXISTS nonsequenced%I() CASCADE;', _table_name);
			EXECUTE format('DROP TYPE t__temporal_%I CASCADE', _table_name);
		END IF;
		--actualized type
		EXECUTE _t;
		--sequenced select
		EXECUTE format('
			CREATE OR REPLACE FUNCTION
				sequenced%I(IN _restructualize BOOLEAN DEFAULT false, _transaction_time TIMESTAMP DEFAULT now()::timestamp) RETURNS setof t__temporal_%I AS $t__temporal_%I$
			DECLARE
				_r %I%%ROWTYPE;
				_rec t__temporal_%I%%ROWTYPE;
				_t PERIOD;
				_times PERIOD[];
				_oid OID;
			BEGIN
				IF _restructualize = false THEN
					FOR _rec IN
						SELECT %s.*, t__%s__temp_data.valid_time FROM %s JOIN t__%s__temp_data ON %s.oid = t__%s__temp_data.row_oid WHERE contains(transaction_time, _transaction_time)
					LOOP
						RETURN NEXT _rec;
					END LOOP;
				ELSE
					FOR _oid IN
						--get all oid to unique row; min function get one oid of oids with equal rows
						SELECT min(oid) AS oid FROM %I GROUP BY ROW(%I.*)
					LOOP
						_times := array[]::period[];
						FOR _t IN
							--select valid_time in duplicit table rows
							EXECUTE format(''SELECT valid_time 
									FROM t__%I__temp_data 
									WHERE row_oid in 
										(SELECT oid 
										FROM %I 
										WHERE ROW(%I.*) in 
											(SELECT * 
											FROM %I 
											WHERE oid = %%L))
									AND contains(transaction_time, ''''%%s''''::timestamp)'', _oid, _transaction_time)
						LOOP
							_times := array_append(_times, _t);
						END LOOP;
						_times := t__restructualize(_times);
						
						SELECT * FROM %I WHERE oid = _oid INTO _r;
						FOREACH _t IN array _times LOOP
							_rec := ROW(_r.*, _t);
							RETURN next _rec;
						END LOOP;
					END LOOP;
				END IF;
			END;
			$t__temporal_%I$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
		--nonsequenced select
		EXECUTE format('
			CREATE OR REPLACE FUNCTION
				nonsequenced%I(_where TEXT DEFAULT '''', _transaction_time TIMESTAMP DEFAULT now()::timestamp) RETURNS setof %I AS $%I$
			DECLARE
				_q TEXT;
				_c TEXT;
			BEGIN
				_q := ''SELECT '';
				FOR _c IN
					SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %L
				LOOP
					_q := _q || _c || '', '';
				END LOOP;
				_q := left(_q, length(_q)-2);
				_q := _q || '' FROM sequenced%I(false, '''''' || _transaction_time::text || '''''')'';
				IF _where <> '''' THEN
					_q := _q || '' WHERE '' || _where;
				END IF;
				RETURN QUERY EXECUTE _q;
			END;
			$%I$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
	END IF;

	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--return text form of from clause
	t__getFromPart(IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_r RECORD;
	_ret TEXT;
	_t TEXT;
BEGIN
	_ret := '';
	--tables and temporal data tables
	FOREACH _t IN ARRAY _tables LOOP
		IF t__getTableType(_t) = 'o' THEN
			_ret := _ret || _t || ', ';
		ELSE
			_ret := _ret || _t || ', t__' || _t || '__temp_data, ';
		END IF;
	END LOOP; 
	--coupled tables
	FOR _r IN
		EXECUTE format('SELECT reference_table_name, referencing_table_name FROM t__reference_integrity 
			WHERE reference_table_name IN (''%s'') AND referencing_table_name IN (''%s'')', array_to_string(_tables, ''', '''), array_to_string(_tables, ''', '''))
	LOOP
		_ret := _ret || format('t__%s_%s__ct, ', _r.reference_table_name, _r.referencing_table_name);
	END LOOP;

	--remove last ', '
	RETURN left(_ret, length(_ret)-length(', '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--returns text form of period intersect of all _tables valid time
	t__getPeriodIntersect(IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_i INTEGER;
	_len INTEGER;
	_ret TEXT;
	_t VARCHAR(1);
BEGIN
	_ret := '';
	_len = array_length(_tables, 1);
	FOR _i IN 1..(_len-1) LOOP
		IF t__getTableType(_tables[_i]) = 'o' THEN
			_ret := _ret || 'period_intersect(period(''-infinity''::timestamp, ''infinity''::timestamp), ';
		ELSE
			_ret := _ret || 'period_intersect(t__' || _tables[_i] || '__temp_data.valid_time, ';
		END IF;
	END LOOP;
	IF t__getTableType(_tables[_len]) = 'o' THEN
		_ret := _ret || 'period(''-infinity''::timestamp, ''infinity''::timestamp)';
	ELSE
		_ret := _ret || 't__' || _tables[_len] || '__temp_data.valid_time';
	END IF;
	FOR _i IN 1..(_len-1) LOOP
		_ret := _ret || ')'; 
	END LOOP;

	RETURN _ret;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--returns text form where transaction time contains timestamp
	t__getTransactionTimeWhere(IN _tables TEXT[], IN _time TIMESTAMP) RETURNS text AS $$
DECLARE
	_i INTEGER;
	_len INTEGER;
	_ret TEXT;
	_t TEXT;
BEGIN
	_ret := '';
	FOREACH _t IN array _tables LOOP
		_ret := _ret || 'contains(t__' || _t || format('__temp_data.transaction_time, ''%s''::timestamp) AND ', _time); 
	END LOOP;
	
	RETURN left(_ret, length(_ret)-length(' AND '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--returns select clause
	t__getSelectPart(IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_ret TEXT;
	_t TEXT;
	_r RECORD;
BEGIN
	_ret := '';
	FOREACH _t IN ARRAY _tables LOOP
		FOR _r IN
			EXECUTE format('SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = %L', _t)
		LOOP
			_ret := _ret || _t || '.' || _r.column_name || ', ';
		END LOOP;
	END LOOP;

	RETURN _ret || t__getPeriodIntersect(_tables);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--returns comparison string
	t__getComparePart(IN _name1 TEXT, IN _name2 TEXT, IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_ret TEXT;
	_t TEXT;
	_r RECORD;
BEGIN
	_ret := '';
	FOREACH _t IN ARRAY _tables LOOP
		FOR _r IN
			EXECUTE format('SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = %L', _t)
		LOOP
			_ret := _ret || _name1 || '.' || _t || '_' || _r.column_name ||
				' = '|| _name2 || '.' || _t || '_' || _r.column_name || ' AND ';
		END LOOP;
	END LOOP;

	RETURN left(_ret, length(_ret)-length(' AND '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--returns select clause
	t__getSelectPartTT(IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_ret TEXT;
	_t TEXT;
	_r RECORD;
BEGIN
	_ret := '';
	FOREACH _t IN ARRAY _tables LOOP
		FOR _r IN
			EXECUTE format('SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = %L', _t)
		LOOP
			_ret := _ret || _t || '.' || _r.column_name || ', ';
		END LOOP;
	END LOOP;

	RETURN left(_ret, length(_ret)-length(', '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--return where part for temporal join from tables
	t__getWherePart(IN _tables TEXT[]) RETURNS text AS $$
DECLARE
	_t TEXT;
	_r RECORD;
	_ret TEXT;
BEGIN
	_ret := '';
	--table with temporal data
	FOREACH _t IN array _tables LOOP
		IF t__getTableType(_t) <> 'o' THEN
			_ret := _ret || _t || '.oid = t__' || _t || '__temp_data.row_oid AND '; 
		END IF;
	END LOOP;
	--tables with couple tables
	FOREACH _t IN array _tables LOOP
		FOR _r IN
			EXECUTE format('SELECT reference_table_name, referencing_table_name FROM t__reference_integrity 
				WHERE reference_table_name IN (''%s'') AND referencing_table_name = ''%s''', array_to_string(_tables, ''', '''), _t)
		LOOP
			_ret := _ret || 't__' || _r.reference_table_name || '_' || _r.referencing_table_name || '__ct.reference_table_row = ' || _r.reference_table_name || '.oid AND ';
			_ret := _ret || 't__' || _r.reference_table_name || '_' || _r.referencing_table_name || '__ct.referencing_table_row = ' || _r.referencing_table_name || '.oid AND ';
		END LOOP;
	END LOOP;

	RETURN left(_ret, length(_ret)-length(' AND '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--only for bitemporal tables, returns where string where each table transaction_time contains _time
	t__getWhereBitemporalPart(IN _tables TEXT[], _time TIMESTAMP) RETURNS text AS $$
DECLARE
	_t TEXT;
	_r RECORD;
	_ret TEXT;
BEGIN
	_ret := '';
	--transaction time contains _time
	FOREACH _t IN array _tables LOOP
		_ret := _ret || 'contains(t__' || _t || '__temp_data.transaction_time, ''' || _time::text || '''::timestamp) AND ';
	END LOOP;

	RETURN left(_ret, length(_ret)-length(' AND '));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--add temporal join function sequenced and nonsequenced or snapshot
	addTemporalJoin(IN _tables TEXT[]) RETURNS boolean AS $$
DECLARE
	_t TEXT;
	_fce TEXT;
	_type TEXT;
	_type_nonseq TEXT;
	_type_snapshot TEXT;
	_type_name TEXT;
	_type_name_nonseq TEXT;
	_r RECORD;
	_i INTEGER;
	_ri_tables TEXT[];
	_tables_type VARCHAR(1); --v - only valid time tables, t - only transaction time tables, s - valid time table combine with state table, b - bitemporal, o - other
	_ft VARCHAR(1); --reference table (from table) type
BEGIN
	--table with join function names
	CREATE TABLE IF NOT EXISTS t__join (function_name text, table_name text[], CONSTRAINT function_name_pk PRIMARY KEY (function_name));
	--check if has reference integrity
	EXECUTE format('SELECT array_agg(reference_table_name) || array_agg(referencing_table_name)
			FROM t__reference_integrity 
			WHERE reference_table_name IN (''%s'') AND referencing_table_name IN (''%s'')', array_to_string(_tables, ''', '''), array_to_string(_tables, ''', ''')) INTO _ri_tables;
	IF _ri_tables IS NULL THEN
		RAISE EXCEPTION 'No reference found.';
	END IF;
	_tables_type = '';
	FOREACH _t IN array _tables LOOP
		_ft := t__getTableType(_t);
		IF _ft = 'v' AND (_tables_type = '' OR _tables_type = 'v') THEN
			_tables_type = 'v';
		ELSIF _ft = 'o' AND (_tables_type = '' OR _tables_type = 's' OR _tables_type = 'v') THEN
			_tables_type = 's';
		ELSIF _ft = 't' AND (_tables_type = '' OR _tables_type = 't') THEN
			_tables_type = 't';
		ELSIF _ft = 'b' AND (_tables_type = '' OR _tables_type = 'b') THEN
			_tables_type = 'b';
		ELSE 
			_tables_type = 'o';
		END IF;
 	END LOOP;
 	IF _tables_type = 'o' THEN
		RAISE EXCEPTION 'Unexpected error.';
	END IF;
	IF _tables_type = 'v' THEN
		--create type name
		_type_name := 't__temporal_sequenced_';
		_type_name_nonseq := 't__temporal_nonsequenced_';
		--create function name
		_fce := '';
		FOREACH _t IN array _tables LOOP
			_type_name := _type_name || _t || '_';
			_type_name_nonseq := _type_name_nonseq || _t || '_';
			_fce := _fce || upper(left(_t, 1)) || lower(right(_t, length(_t)-1));
		END LOOP;
		_type_name := left(_type_name, length(_type_name)-1);
		_type_name_nonseq := left(_type_name_nonseq, length(_type_name_nonseq)-1);
		--is already exists
		EXECUTE format('SELECT count(*) FROM t__join WHERE function_name IN (''sequencedJoin%s'', ''nonsequencedJoin%s'');', _fce, _fce) INTO _i;
		IF _i > 0 THEN
			RAISE EXCEPTION 'Temporal join function already exists with names ''sequencedJoin%'' and ''nonsequencedJoin%''.', _fce, _fce; 
		END IF;
		EXECUTE format('DROP FUNCTION IF EXISTS sequencedJoin%s(boolean);', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS nonsequencedJoin%s(text);', _fce);
		--drop existing type
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', _type_name);
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', _type_name_nonseq);
		EXECUTE format('INSERT INTO t__join (function_name, table_name) VALUES (''sequencedJoin%s'', array[''%s'']);', _fce, array_to_string(_tables, ''', '''));
		EXECUTE format('INSERT INTO t__join (function_name, table_name) VALUES (''nonsequencedJoin%s'', array[''%s'']);', _fce, array_to_string(_tables, ''', '''));
		--create new type
		_type := 'CREATE TYPE ' || _type_name || ' AS (';
		_type_nonseq := 'CREATE TYPE ' || _type_name_nonseq || ' AS (';
		FOREACH _t IN ARRAY _tables LOOP
			FOR _r IN
				EXECUTE format('SELECT column_name, data_type
					FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = %L', _t)
			LOOP
				_type := _type || _t || '_' || _r.column_name || ' ' || _r.data_type || ', ';
				_type_nonseq := _type_nonseq || _t || '_' || _r.column_name || ' ' || _r.data_type || ', ';
			END LOOP;
		END LOOP;
		EXECUTE _type || 'valid_time period);';
		EXECUTE left(_type_nonseq, length(_type_nonseq)-2) || ');';
		--create compare function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				t__compare_%s(
					IN _nonseq %s, 
					IN _seq %s) RETURNS BOOLEAN AS $t__compare_%s$
			BEGIN
				IF %s THEN
					RETURN true;
				END IF;
				RETURN false;
			END;
			$t__compare_%s$ LANGUAGE plpgsql;', _fce, _type_name_nonseq, _type_name, _fce, t__getComparePart('_nonseq', '_seq', _tables), _fce);
		--create sequenced function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				sequencedJoin%s(_restruct BOOLEAN DEFAULT false) RETURNS setof %s AS $sequencedJoin%s$
			DECLARE
				_rec %s%%ROWTYPE;
				_tables TEXT[];
				_nonseq %s%%ROWTYPE;
				_seq %s%%ROWTYPE;
				_nonseqs "%s"[];
				_seqs "%s"[];
				_new %s%%ROWTYPE;
				_times PERIOD[];
				_t PERIOD;
			BEGIN
				IF _restruct = false THEN
					_tables := array[''%s''];
					FOR _rec IN 
						EXECUTE format(''SELECT %%s FROM %%s WHERE %%s AND nequals(%%s, empty_period())'', t__getSelectPart(_tables), t__getFromPart(_tables), t__getWherePart(_tables), t__getPeriodIntersect(_tables))
					LOOP
						RETURN NEXT _rec;
					END LOOP;
				ELSE
					--get all nonsequenced
					FOR _nonseq IN
						SELECT DISTINCT * FROM nonsequencedJoin%s()
					LOOP
						_nonseqs := array_append(_nonseqs, _nonseq);
					END LOOP;
					--get all sequenced
					FOR _seq IN
						SELECT * FROM sequencedJoin%s()
					LOOP
						_seqs := array_append(_seqs, _seq);
					END LOOP;
					--for each unique nonsequenced row restructualize valid_time
					FOREACH _nonseq IN ARRAY _nonseqs LOOP
						_times := array[]::period[];
						FOREACH _seq IN ARRAY _seqs LOOP
							--compare rows
							IF t__compare_%s(_nonseq, _seq) THEN
								--add valid time to array with restructualize valid times
								IF not(_times @> array[_seq.valid_time]) THEN
									_times := array_append(_times, _seq.valid_time);
								END IF;
							END IF;
						END LOOP;
						--restructualize and return
						_times := t__restructualize(_times);
						FOREACH _t IN ARRAY _times LOOP
							_new := ROW(_nonseq.*, _t);
							RETURN next _new;
						END LOOP;
					END LOOP;
				END IF;
			END;
			$sequencedJoin%s$ LANGUAGE plpgsql;', _fce, _type_name, _fce, _type_name, 
			_type_name_nonseq, _type_name, _type_name_nonseq, _type_name, _type_name,
			array_to_string(_tables, ''', '''), _fce, _fce, _fce, _fce);
		--create nonsequenced function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				nonsequencedJoin%s(_where TEXT DEFAULT '''') RETURNS setof %s AS $nonsequencedJoin%s$
			DECLARE
				_tables TEXT[];
				_t TEXT;
				_c TEXT;
				_q TEXT;
			BEGIN
				_tables := (SELECT table_name FROM t__join WHERE function_name = ''nonsequencedJoin%s'');
				_q := ''SELECT '';
				FOREACH _t IN ARRAY _tables LOOP
					FOR _c IN
						SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = _t
					LOOP
						_q := _q || format(''%%s_%%s'', _t, _c) || '', '';
					END LOOP;
				END LOOP;
				_q := left(_q, length(_q)-2);
				_q := _q || '' FROM sequencedJoin%s()'';
				IF _where <> '''' THEN
					_q := _q || '' WHERE '' || _where;
				END IF;

				RETURN QUERY EXECUTE _q;
			END;
			$nonsequencedJoin%s$ LANGUAGE plpgsql;', _fce, _type_name_nonseq, _fce, _fce, _fce, _fce);
	END IF;
	IF _tables_type = 't' THEN
		--create type name
		_type_name := 't__temporal_snapshot_';
		--create function name
		_fce := '';
		FOREACH _t IN array _tables LOOP
			_type_name := _type_name || _t || '_';
			_fce := _fce || upper(left(_t, 1)) || lower(right(_t, length(_t)-1));
		END LOOP;
		_type_name := left(_type_name, length(_type_name)-1);
		--is already exists
		EXECUTE format('SELECT count(*) FROM t__join WHERE function_name = ''snapshotJoin%s'';', _fce) INTO _i;
		IF _i > 0 THEN
			RAISE EXCEPTION 'Temporal join function already exists with name ''snapshotJoin%''.', _fce; 
		END IF;
		EXECUTE format('DROP FUNCTION IF EXISTS snapshotJoin%s(TIMESTAMP);', _fce);
		--drop existing type
		EXECUTE format('DROP TYPE IF EXISTS %s;', _type_name);
		EXECUTE format('INSERT INTO t__join (function_name, table_name) VALUES (''snapshotJoin%s'', array[''%s'']);', _fce, array_to_string(_tables, ''', '''));
		--create new type
		_type := 'CREATE TYPE ' || _type_name || ' AS (';
		FOREACH _t IN ARRAY _tables LOOP
			FOR _r IN
				EXECUTE format('SELECT column_name, data_type
					FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = %L', _t)
			LOOP
				_type := _type || _t || '_' || _r.column_name || ' ' || _r.data_type || ', ';
			END LOOP;
		END LOOP;
		EXECUTE left(_type, length(_type)-2) || ');';
		--create snapshot function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				snapshotJoin%s(_time TIMESTAMP DEFAULT now()) RETURNS setof %s AS $snapshotJoin%s$
			DECLARE
				_rec %s%%ROWTYPE;
				_tables TEXT[];
			BEGIN
				_tables := array[''%s''];
				FOR _rec IN 
					EXECUTE format(''SELECT %%s FROM %%s WHERE %%s AND %%s'', t__getSelectPartTT(_tables), t__getFromPart(_tables), t__getWherePart(_tables), t__getTransactionTimeWhere(_tables, _time))
				LOOP
					RETURN NEXT _rec;
				END LOOP;
			END;
			$snapshotJoin%s$ LANGUAGE plpgsql;', _fce, _type_name, _fce, _type_name, array_to_string(_tables, ''', '''), _fce);
	END IF;
	IF _tables_type = 'b' THEN
		--create type name
		_type_name := 't__temporal_sequenced_';
		_type_name_nonseq := 't__temporal_nonsequenced_';
		--create function name
		_fce := '';
		FOREACH _t IN array _tables LOOP
			_type_name := _type_name || _t || '_';
			_type_name_nonseq := _type_name_nonseq || _t || '_';
			_fce := _fce || upper(left(_t, 1)) || lower(right(_t, length(_t)-1));
		END LOOP;
		_type_name := left(_type_name, length(_type_name)-1);
		_type_name_nonseq := left(_type_name_nonseq, length(_type_name_nonseq)-1);
		--is already exists
		EXECUTE format('SELECT count(*) FROM t__join WHERE function_name IN (''sequencedJoin%s'', ''nonsequencedJoin%s'');', _fce, _fce) INTO _i;
		IF _i > 0 THEN
			RAISE EXCEPTION 'Temporal join function already exists with names ''sequencedJoin%'' and ''nonsequencedJoin%''.', _fce, _fce; 
		END IF;
		EXECUTE format('DROP FUNCTION IF EXISTS sequencedJoin%s(boolean, timestamp);', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS nonsequencedJoin%s(text, timestamp);', _fce);
		--drop existing type
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', _type_name);
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', _type_name_nonseq);
		EXECUTE format('INSERT INTO t__join (function_name, table_name) VALUES (''sequencedJoin%s'', array[''%s'']);', _fce, array_to_string(_tables, ''', '''));
		EXECUTE format('INSERT INTO t__join (function_name, table_name) VALUES (''nonsequencedJoin%s'', array[''%s'']);', _fce, array_to_string(_tables, ''', '''));
		--create new type
		_type := 'CREATE TYPE ' || _type_name || ' AS (';
		_type_nonseq := 'CREATE TYPE ' || _type_name_nonseq || ' AS (';
		FOREACH _t IN ARRAY _tables LOOP
			FOR _r IN
				EXECUTE format('SELECT column_name, data_type
					FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = %L', _t)
			LOOP
				_type := _type || _t || '_' || _r.column_name || ' ' || _r.data_type || ', ';
				_type_nonseq := _type_nonseq || _t || '_' || _r.column_name || ' ' || _r.data_type || ', ';
			END LOOP;
		END LOOP;
		EXECUTE _type || 'valid_time period);';
		EXECUTE left(_type_nonseq, length(_type_nonseq)-2) || ');';
		--create compare function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				t__compare_%s(
					IN _nonseq %s, 
					IN _seq %s) RETURNS BOOLEAN AS $t__compare_%s$
			BEGIN
				IF %s THEN
					RETURN true;
				END IF;
				RETURN false;
			END;
			$t__compare_%s$ LANGUAGE plpgsql;', _fce, _type_name_nonseq, _type_name, _fce, t__getComparePart('_nonseq', '_seq', _tables), _fce);
		--create sequenced function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				sequencedJoin%s(_restruct BOOLEAN DEFAULT false, _transaction_time TIMESTAMP DEFAULT now()::timestamp) RETURNS setof %s AS $sequencedJoin%s$
			DECLARE
				_rec %s%%ROWTYPE;
				_tables TEXT[];
				_nonseq %s%%ROWTYPE;
				_seq %s%%ROWTYPE;
				_nonseqs "%s"[];
				_seqs "%s"[];
				_new %s%%ROWTYPE;
				_times PERIOD[];
				_t PERIOD;
			BEGIN
				IF _restruct = false THEN
					_tables := array[''%s''];
					FOR _rec IN 
						EXECUTE format(''SELECT %%s FROM %%s WHERE %%s AND %%s AND nequals(%%s, empty_period())'', t__getSelectPart(_tables), t__getFromPart(_tables), t__getWhereBitemporalPart(_tables, _transaction_time), t__getWherePart(_tables), t__getPeriodIntersect(_tables))
					LOOP
						RETURN NEXT _rec;
					END LOOP;
				ELSE
					--get all nonsequenced
					FOR _nonseq IN
						SELECT DISTINCT * FROM nonsequencedJoin%s('''', _transaction_time)
					LOOP
						_nonseqs := array_append(_nonseqs, _nonseq);
					END LOOP;
					--get all sequenced
					FOR _seq IN
						SELECT * FROM sequencedJoin%s(false, _transaction_time)
					LOOP
						_seqs := array_append(_seqs, _seq);
					END LOOP;
					--for each unique nonsequenced row restructualize valid_time
					FOREACH _nonseq IN ARRAY _nonseqs LOOP
						_times := array[]::period[];
						FOREACH _seq IN ARRAY _seqs LOOP
							--compare rows
							IF t__compare_%s(_nonseq, _seq) THEN
								--add valid time to array with restructualize valid times
								IF not(_times @> array[_seq.valid_time]) THEN
									_times := array_append(_times, _seq.valid_time);
								END IF;
							END IF;
						END LOOP;
						--restructualize and return
						_times := t__restructualize(_times);
						FOREACH _t IN ARRAY _times LOOP
							_new := ROW(_nonseq.*, _t);
							RETURN next _new;
						END LOOP;
					END LOOP;
				END IF;
			END;
			$sequencedJoin%s$ LANGUAGE plpgsql;', _fce, _type_name, _fce, _type_name, 
			_type_name_nonseq, _type_name, _type_name_nonseq, _type_name, _type_name,
			array_to_string(_tables, ''', '''), _fce, _fce, _fce, _fce);
		--create nonsequenced function
		EXECUTE format('CREATE OR REPLACE FUNCTION
				nonsequencedJoin%s(_where TEXT DEFAULT '''', _transaction_time TIMESTAMP DEFAULT now()::timestamp) RETURNS setof %s AS $nonsequencedJoin%s$
			DECLARE
				_tables TEXT[];
				_t TEXT;
				_c TEXT;
				_q TEXT;
			BEGIN
				_tables := (SELECT table_name FROM t__join WHERE function_name = ''nonsequencedJoin%s'');
				_q := ''SELECT '';
				FOREACH _t IN ARRAY _tables LOOP
					FOR _c IN
						SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = _t
					LOOP
						_q := _q || format(''%%s_%%s'', _t, _c) || '', '';
					END LOOP;
				END LOOP;
				_q := left(_q, length(_q)-2);
				_q := _q || '' FROM sequencedJoin%s(false, '''''' || _transaction_time || '''''')'';
				IF _where <> '''' THEN
					_q := _q || '' WHERE '' || _where;
				END IF;

				RETURN QUERY EXECUTE _q;
			END;
			$nonsequencedJoin%s$ LANGUAGE plpgsql;', _fce, _type_name_nonseq, _fce, _fce, _fce, _fce);
	END IF;
	
	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--return table with all temporal join function names
	getTemporalJoinFunctions() RETURNS setof text AS $$
BEGIN
	RETURN QUERY SELECT function_name FROM t__join;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--delete temporal join
	dropTemporalJoin(IN _tables TEXT[]) RETURNS boolean AS $$
DECLARE
	_fce TEXT;
	_type_name TEXT;
	_t TEXT;
	_i INTEGER;
BEGIN
	--create function name
	_fce := '';
	--create type name
	_type_name := '';
	FOREACH _t IN array _tables LOOP
		_fce := _fce || upper(left(_t, 1)) || lower(right(_t, length(_t)-1));
		_type_name := _type_name || _t || '_';
	END LOOP;
	_type_name := left(_type_name, length(_type_name)-1);
	EXECUTE format('SELECT count(*) FROM t__join WHERE function_name IN (''sequencedJoin%s'', ''nonsequencedJoin%s'', ''snapshotJoin%s'');', _fce, _fce, _fce) INTO _i;
	IF _i > 0 THEN
		EXECUTE format('DROP FUNCTION IF EXISTS sequencedJoin%s(BOOLEAN) CASCADE', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS sequencedJoin%s(BOOLEAN, TIMESTAMP) CASCADE', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS snapshotJoin%s(TIMESTAMP) CASCADE', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS nonsequencedJoin%s(text) CASCADE', _fce);
		EXECUTE format('DROP FUNCTION IF EXISTS nonsequencedJoin%s(text, TIMESTAMP) CASCADE', _fce);
		--EXECUTE format('DROP FUNCTION IF EXISTS t__compare_%s(%s, %s)', _fce, ('t__temporal_nonsequenced_' || _type_name), ('t__temporal_sequenced_' || _type_name)); 
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', ('t__temporal_sequenced_' || _type_name));
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', ('t__temporal_nonsequenced_' || _type_name));
		EXECUTE format('DROP TYPE IF EXISTS %s CASCADE;', ('t__temporal_snapshot_' || _type_name));
		EXECUTE format('DELETE FROM t__join WHERE function_name IN (''sequencedJoin%s'', ''nonsequencedJoin%s'', ''snapshotJoin%s'');', _fce, _fce, _fce);
	END IF;

	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--inserting into couple table
	insertReferenceIntegrity(_reference_table TEXT, _referencing_table TEXT, _oid_reference_table OID[], _oid_referencing_table OID[]) RETURNS bool AS $$
DECLARE
	_oid1 OID;
	_oid2 OID;
	_i INTEGER;
BEGIN
	--check to possible coupling
	EXECUTE format('SELECT count(*) FROM t__reference_integrity WHERE reference_table_name = %L AND
				referencing_table_name = %L', _reference_table, _referencing_table) INTO _i;
	IF _i = 0 THEN
		RAISE EXCEPTION 'Tables has not reference.';
	END IF;
	--coupled all oids from first select with all oids from second select
	FOREACH _oid1 IN ARRAY _oid_reference_table LOOP
		FOREACH _oid2 IN ARRAY _oid_referencing_table LOOP
			--not insert duplicit
			EXECUTE format('SELECT count(*) FROM t__%I_%I__ct WHERE reference_table_row = %s AND
					referencing_table_row = %s', _reference_table, _referencing_table, _oid1::text, _oid2::text) INTO _i;
			IF _i = 0 THEN
				EXECUTE format('INSERT INTO t__%I_%I__ct (reference_table_row, referencing_table_row)
						VALUES (%s, %s)', _reference_table, _referencing_table, _oid1::text, _oid2::text);
			END IF;
		END LOOP;
	END LOOP;
	
	RETURN true;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--split timestamp to 2
	--example: select splitValidTimeRecord('emp', array(select oid from emp), now()::timestamp);
	splitValidTimeRecord(_table TEXT, _oids OID[], _time TIMESTAMP) RETURNS bool AS $$
DECLARE
	_oid OID;
	_p PERIOD;
	_t TEXT;
BEGIN
	--check valid time table
	_t := (SELECT t__getTableType(_table));
	IF _t <> 'v' THEN
		RAISE EXCEPTION 'Table is not Valid Time table.';
	END IF;
	--get period, update old, insert new
	FOREACH _oid IN ARRAY _oids LOOP
		FOR _p IN
			EXECUTE format('SELECT valid_time FROM t__%I__temp_data WHERE row_oid = %L', _table, _oid)
		LOOP
			IF contains(_p, _time) THEN
				EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(first(valid_time), ''%I''::timestamp) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, _time, _oid, first(_p), last(_p));
				EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time) VALUES (%L, period(%L, ''%I''::timestamp))', _table, _oid, _time, next(_p));
			END IF;
		END LOOP;
	END LOOP;

	RETURN true;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--delete period 
	--example: select deleteValidTime('emp', array(select oid from emp), period(now(), '2015-05-01'::timestamp));
	deleteValidTime(_table TEXT, _oids OID[], _period PERIOD) RETURNS bool AS $$
DECLARE
	_oid OID;
	_p PERIOD;
	_t TEXT;
	_r RECORD;
BEGIN
	--check valid time table
	_t := (SELECT t__getTableType(_table));
	IF _t <> 'v' AND _t <> 'b' THEN
		RAISE EXCEPTION 'Table is not Valid Time table.';
	END IF;
	IF _t = 'v' THEN
		--get period, update old, insert new
		FOREACH _oid IN ARRAY _oids LOOP
			FOR _p IN
				EXECUTE format('SELECT valid_time FROM t__%I__temp_data WHERE row_oid = %L', _table, _oid)
			LOOP
				--new period is full contained in old
				IF contains(_p, _period) THEN
					IF is_empty(period(first(_p), first(_period))) THEN
						EXECUTE format('DELETE FROM t__%I__temp_data WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, _oid, first(_p), next(_p));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_p), first(_period), _oid, first(_p), next(_p));
					END IF;
					IF is_empty(period(next(_period), next(_p))) = false THEN
						EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time) VALUES (%L, period(%L, %L))', _table, _oid, next(_period), next(_p));
					END IF;
					CONTINUE;
				END IF;
				--first part new period is contained in old
				IF contains(_p, first(_period)) THEN
					IF is_empty(period(first(_p), first(_period))) THEN
						EXECUTE format('DELETE FROM t__%I__temp_data WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, _oid, first(_p), next(_p));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_p), first(_period), _oid, first(_p), next(_p));
					END IF;
					CONTINUE;
				END IF;
				--last part new period is contained in old
				IF contains(_p, last(_period)) THEN
					IF is_empty(period(next(_period), next(_p))) THEN
						EXECUTE format('DELETE FROM t__%I__temp_data WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, _oid, first(_p), next(_p));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, next(_period), next(_p), _oid, first(_p), next(_p));
					END IF;
					CONTINUE;
				END IF;
			END LOOP;
		END LOOP;
	END IF;
	IF _t = 'b' THEN
		--get period, update old, insert new
		FOREACH _oid IN ARRAY _oids LOOP
			FOR _r IN
				EXECUTE format('SELECT row_oid, valid_time, transaction_time FROM t__%I__temp_data WHERE row_oid = %L', _table, _oid)
			LOOP
				--new period is full contained in old
				IF contains(_r.valid_time, _period) THEN
					IF is_empty(period(first(_r.valid_time), first(_period))) THEN
						EXECUTE format('UPDATE t__%I__temp_data SET transaction_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_r.transaction_time), now()::timestamp, _oid, first(_r.valid_time), next(_r.valid_time));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_r.valid_time), first(_period), _oid, first(_r.valid_time), next(_r.valid_time));
					END IF;
					IF is_empty(period(first(_period), next(_period))) = false THEN
						EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%s, period(%L, %L), period(%L, %L))', _table, _r.row_oid, first(_period), next(_period), first(_r.transaction_time), now()::timestamp);
					END IF;
					IF is_empty(period(next(_period), next(_r.valid_time))) = false THEN
						EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%s, period(%L, %L), period(%L, %L))', _table, _r.row_oid, next(_period), next(_r.valid_time), first(_r.transaction_time), 'infinity'::timestamp);
					END IF;
					CONTINUE;
				END IF;
				--first part new period is contained in old
				IF contains(_r.valid_time, first(_period)) THEN
					IF is_empty(period(first(_r.valid_time), first(_period))) THEN
						EXECUTE format('UPDATE t__%I__temp_data SET transaction_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_r.transaction_time), now()::timestamp, _oid, first(_r.valid_time), next(_r.valid_time));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_r.valid_time), first(_period), _oid, first(_r.valid_time), next(_r.valid_time));
					END IF;
					IF is_empty(period(first(_period), next(_r.valid_time))) = false THEN
						EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%s, period(%L, %L), period(%L, %L))', _table, _r.row_oid, first(_period), next(_r.valid_time), first(_r.transaction_time), now()::timestamp);
					END IF;
					CONTINUE;
				END IF;
				--last part new period is contained in old
				IF contains(_r.valid_time, last(_period)) THEN
					IF is_empty(period(next(_period), next(_r.valid_time))) THEN
						EXECUTE format('UPDATE t__%I__temp_data SET transaction_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, first(_r.transaction_time), now()::timestamp, _oid, first(_r.valid_time), next(_r.valid_time));
					ELSE
						EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)', _table, next(_period), next(_r.valid_time), _oid, first(_r.valid_time), next(_r.valid_time));
					END IF;
					IF is_empty(period(first(_r.valid_time), next(_period))) = false THEN
						EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%s, period(%L, %L), period(%L, %L))', _table, _r.row_oid, first(_r.valid_time), next(_period), first(_r.transaction_time), now()::timestamp);
					END IF;
					CONTINUE;
				END IF;
			END LOOP;
		END LOOP;
	END IF;

	RETURN true;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--set oid in table as primary key
	t__setOidAsPK(IN _table_name TEXT) RETURNS void AS $$
DECLARE
	_r TEXT;
BEGIN
	FOR _r IN
		EXECUTE format('SELECT conname FROM pg_constraint WHERE contype = ''p'' AND conrelid = ''%I''::regclass', _table_name)
	LOOP
		EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I CASCADE;', _table_name, _r);
	END LOOP;
	EXECUTE format(E'ALTER TABLE %I ADD PRIMARY KEY (oid);', _table_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION 
	--update bitemporal valid time help function
	t__updateBitemporalValidTime(_old_row_oid OID, _old_valid_time PERIOD, _old_transaction_time PERIOD,
							_new_valid_time PERIOD, _table TEXT, _where TEXT, _delete_other BOOLEAN) RETURNS bool AS $t__updateBitemporalValidTime$
DECLARE
	_r RECORD;
	_b BOOLEAN;
	_p PERIOD;
	_tt PERIOD;
	_c INTEGER;
BEGIN
	--deleted record can not be updated
	--updated valid time only
	IF last(_old_transaction_time) < now() OR is_empty(_old_valid_time) OR is_empty(_new_valid_time) OR equals(_old_valid_time, _new_valid_time) THEN
		RETURN true;
	END IF;
	FOR _r IN
		EXECUTE format('SELECT valid_time FROM t__%I__temp_data WHERE row_oid = %L AND (%s)', _table, _old_row_oid, _where)
	LOOP
		IF equals(_r.valid_time, _new_valid_time) THEN
			RETURN true;
		END IF;
	END LOOP; 
	--check RI overlaps
	SELECT t__checkBTOverlaps(_table, _new_valid_time, _old_row_oid) INTO _b;
	-- OLD: a [-----------) b
	-- NEW: c    [-----)    d
	IF contains(_old_valid_time, _new_valid_time) AND _delete_other = true THEN
		--[c, d) continues
		EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)',
				_table, first(_new_valid_time), next(_new_valid_time), _old_row_oid, first(_old_valid_time), next(_old_valid_time));
		--[a, c) insert
		IF first(_old_valid_time) <> first(_new_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(%L, now()))', 
				_table, _old_row_oid, first(_old_valid_time), first(_new_valid_time), first(_old_transaction_time));
		END IF;
		--[d, b) insert
		IF next(_new_valid_time) <> next(_old_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(%L, now()))', 
				_table, _old_row_oid, next(_new_valid_time), next(_old_valid_time), first(_old_transaction_time));
		END IF;
		RETURN true;
	END IF;
	-- OLD: a    [-----)    b
	-- NEW: c [-----------) d
	IF contains(_new_valid_time, _old_valid_time) AND _delete_other = true THEN
		--[c, a) insert
		FOR _r IN
			EXECUTE format('SELECT valid_time, transaction_time FROM t__%I__temp_data WHERE row_oid = %L AND (%s)', _table, _old_row_oid, _where)
		LOOP
			IF is_empty(_r.transaction_time) THEN
				CONTINUE WHEN TRUE;
			END IF;
			--[c, a)
			_p := period(first(_new_valid_time), first(_old_valid_time));
			IF _p <> _r.valid_time AND is_empty(_p) = false THEN
				_b := t__updateBitemporalValidTime(_old_row_oid, _r.valid_time, _r.transaction_time, _p, _table, _where, false);
			END IF;
			--[a, b)
			_p := _old_valid_time;
			IF _p <> _r.valid_time AND is_empty(_p) = false THEN
				_b := t__updateBitemporalValidTime(_old_row_oid, _r.valid_time, _r.transaction_time, _p, _table, _where, false);
			END IF;
			--[b, d)
			_p := period(next(_old_valid_time), next(_new_valid_time));
			IF _p <> _r.valid_time AND is_empty(_p) = false THEN
				_b := t__updateBitemporalValidTime(_old_row_oid, _r.valid_time, _r.transaction_time, _p, _table, _where, false);
			END IF;
		END LOOP;
		RETURN true;
	END IF;
	-- OLD: a    [-----)          b
	-- NEW: c       [-----------) d
	IF contains(_old_valid_time, first(_new_valid_time)) AND _delete_other = true THEN
		--[c, b) continues
		EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)',
				_table, first(_new_valid_time), next(_old_valid_time), _old_row_oid, first(_old_valid_time), next(_old_valid_time));
		--[a, c) insert
		IF first(_old_valid_time) <> first(_new_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(%L, now()))', 
				_table, _old_row_oid, first(_old_valid_time), first(_new_valid_time), first(_old_transaction_time));
		END IF;
		--[b, d) insert
		IF next(_old_valid_time) <> next(_new_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(now(), ''infinity''::timestamp))', 
				_table, _old_row_oid, next(_old_valid_time), next(_new_valid_time));
		END IF;
		RETURN true;
	END IF;
	-- OLD: a          [-----) b
	-- NEW: c [-----------)    d
	IF contains(_old_valid_time, last(_new_valid_time)) AND _delete_other = true THEN
		--[a, d) continues
		EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND valid_time = period(%L, %L)',
				_table, first(_old_valid_time), next(_new_valid_time), _old_row_oid, first(_old_valid_time), next(_old_valid_time));
		--[d, b) insert
		IF next(_new_valid_time) <> next(_old_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(%L, now()))', 
				_table, _old_row_oid, next(_new_valid_time), next(_old_valid_time), first(_old_transaction_time));
		END IF;
		--[c, a) insert
		IF first(_new_valid_time) <> first(_old_valid_time) THEN
			EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(now(), ''infinity''::timestamp))', 
				_table, _old_row_oid, first(_new_valid_time), first(_old_valid_time));
		END IF;
		RETURN true;
	END IF;
	-- OLD: a                [-----) b
	-- NEW: c [-----------)          d
	IF _delete_other = true THEN
		--[a, b) end
		EXECUTE format('UPDATE t__%I__temp_data SET transaction_time = period(first(transaction_time), now()) WHERE row_oid = %L AND valid_time = period(%L, %L)',
				_table, _old_row_oid, first(_old_valid_time), next(_old_valid_time));
	END IF;
	EXECUTE format('SELECT count(*) FROM t__%I__temp_data WHERE overlaps(valid_time, period(''%s'')) AND row_oid = %L AND (%s)', _table, period(first(_new_valid_time), next(_new_valid_time)), _old_row_oid, _where) INTO _c;
	--[c, d) insert
	IF first(_new_valid_time) <> next(_new_valid_time) AND _c = 0 THEN
		EXECUTE format('INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) VALUES (%L, period(%L, %L), period(now(), ''infinity''::timestamp))', 
			_table, _old_row_oid, first(_new_valid_time), next(_new_valid_time));
	END IF;
	RETURN true;
END;
$t__updateBitemporalValidTime$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--create reference between two tables
	createReferenceTable(IN _reference_table TEXT, IN _referencing_table TEXT) RETURNS bool AS $$
DECLARE
	_c INTEGER;
	_r RECORD;
	_r2 RECORD;
	_b BOOLEAN;
	_refcol NAME;
	_ft VARCHAR(1); --reference table (from table) type 
	_tt VARCHAR(1); --referencing table (to table) type
BEGIN
	_refcol := '';
	_ft := t__getTableType(_reference_table);
	_tt := t__getTableType(_referencing_table);
	--no referencing TT - transaction time table, VT - valid time table, OT - other tables
	-------------------------
	--|FROM	|	TO	|
	--|-----|---------------|
	--|TT	|	VT	|
	--|TT	|	OT	|
	--|VT	|	TT	|
	--|OT	|	TT	|
	-------------------------
	IF (_ft = 't' AND _tt = 'v') OR (_ft = 't' AND _tt = 'o') OR (_ft = 'v' AND _tt = 't') OR (_ft = 'o' AND _tt = 't') THEN
		RAISE EXCEPTION 'Forbidden operation! Reference and referencing table have unsuported formats.';
	END IF;
	--set with oids for nontemporal tables
	IF _ft = 'o' THEN
		EXECUTE format('ALTER TABLE %I SET WITH OIDS', _reference_table);
	END IF;
	IF _tt = 'o' THEN
		EXECUTE format('ALTER TABLE %I SET WITH OIDS', _referencing_table);
	END IF;
	--create coupling table
	EXECUTE format('DROP TABLE IF EXISTS t__%I_%I__ct CASCADE;', _reference_table, _referencing_table);
	EXECUTE format('CREATE TABLE IF NOT EXISTS t__%I_%I__ct (
			reference_table_row OID NOT NULL,
			referencing_table_row OID NOT NULL
		)', _reference_table, _referencing_table);
	FOR _r IN
		EXECUTE format('SELECT conname, conrelid, conkey FROM pg_constraint WHERE contype = ''f'' 
			AND conrelid IN (SELECT oid FROM pg_class WHERE relname = ''%I'') 
			AND confrelid IN (SELECT oid FROM pg_class WHERE relname = ''%I'');', _reference_table, _referencing_table)
	LOOP
		--drop constraint FK
		FOR _r2 IN
			EXECUTE format('SELECT attname FROM pg_attribute WHERE attrelid = %L AND attnum > 0 AND attnum IN (%L)', _r.conrelid, array_to_string(_r.conkey, ', '))
		LOOP
			--change column with not real FK to oid type
			EXECUTE format('ALTER TABLE %I DROP COLUMN %I', _reference_table, _r2.attname);
		END LOOP;
	END LOOP;

	--remove from couple table when delete from v->o, v->v, o->v
	IF (_ft = 'v' AND (_tt = 'o' OR _tt = 'v')) OR (_ft = 'o' AND _tt = 'v') THEN
		IF _ft = 'v' THEN
			--reference table temp data trigger function
			EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftriad() RETURNS trigger AS $t__%I__temp_data__triad$
				BEGIN
					DELETE FROM t__%I_%I__ct WHERE reference_table_row = OLD.row_oid;

					RETURN NULL;
				END;
				$t__%I__temp_data__triad$ LANGUAGE plpgsql;', _reference_table, _reference_table, _reference_table, _referencing_table, _reference_table);
			--reference triger
			EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__triad ON t__%I__temp_data', _reference_table, _reference_table);
			EXECUTE format('CREATE TRIGGER t__%I__temp_data__triad AFTER DELETE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftriad();', _reference_table, _reference_table, _reference_table);
		ELSE
			--reference table temp data trigger function
			EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftriad() RETURNS trigger AS $t__%I__temp_data__triad$
				BEGIN
					DELETE FROM t__%I_%I__ct WHERE reference_table_row = OLD.oid;

					RETURN NULL;
				END;
				$t__%I__temp_data__triad$ LANGUAGE plpgsql;', _reference_table, _reference_table, _reference_table, _referencing_table, _reference_table);
			--reference triger
			EXECUTE format('DROP TRIGGER IF EXISTS %I__triad ON %I', _reference_table, _reference_table);
			EXECUTE format('CREATE TRIGGER %I__triad AFTER DELETE ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftriad();', _reference_table, _reference_table, _reference_table);
		END IF;
		IF _tt = 'v' THEN
			--referencing table temp data trigger function
			EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftriad() RETURNS trigger AS $t__%I__temp_data__triad$
				BEGIN
					DELETE FROM t__%I_%I__ct WHERE referencing_table_row = OLD.row_oid;
					
					RETURN NULL;
				END;
				$t__%I__temp_data__triad$ LANGUAGE plpgsql;', _referencing_table, _referencing_table, _reference_table, _referencing_table, _referencing_table);
			--referencing trigger
			EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__triad ON t__%I__temp_data', _referencing_table, _referencing_table);
			EXECUTE format('CREATE TRIGGER t__%I__temp_data__triad AFTER DELETE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftriad();', _referencing_table, _referencing_table, _referencing_table);
		ELSE
			--referencing table temp data trigger function
			EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftriad() RETURNS trigger AS $t__%I__temp_data__triad$
				BEGIN
					DELETE FROM t__%I_%I__ct WHERE referencing_table_row = OLD.oid;
					
					RETURN NULL;
				END;
				$t__%I__temp_data__triad$ LANGUAGE plpgsql;', _referencing_table, _referencing_table, _reference_table, _referencing_table, _referencing_table);
			--referencing trigger
			EXECUTE format('DROP TRIGGER IF EXISTS %I__triad ON %I', _referencing_table, _referencing_table);
			EXECUTE format('CREATE TRIGGER %I__triad AFTER DELETE ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftriad();', _referencing_table, _referencing_table, _referencing_table);
		END IF;
	END IF;
	IF (_ft = 'v' AND _tt = 'v') THEN
		--coupled table trigger function before insert
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I_%I__ct__ftribi() RETURNS trigger AS $t__%I_%I__ct__tribi$
			DECLARE
				_p1 PERIOD;
				_p1s PERIOD[];
				_p2 PERIOD;
				_p2s PERIOD[];
				_r RECORD;
				_overlaps BOOLEAN;
			BEGIN
				_p1s := array[]::period[];
				_p2s := array[]::period[];
				FOR _r IN
					SELECT valid_time FROM t__%I__temp_data WHERE row_oid = NEW.reference_table_row
				LOOP
					_p1s := array_append(_p1s, _r.valid_time);
				END LOOP;
				FOR _r IN
					SELECT valid_time FROM t__%I__temp_data WHERE row_oid = NEW.referencing_table_row
				LOOP
					_p2s := array_append(_p2s, _r.valid_time);
				END LOOP;
				IF _p1s <> array[]::period[] THEN
					_p1s := t__restructualize(_p1s);
				END IF;
				IF _p2s <> array[]::period[] THEN
					_p2s := t__restructualize(_p2s);
				END IF;
				--check if overlaps
				FOREACH _p1 IN ARRAY _p1s LOOP
					FOREACH _p2 IN ARRAY _p2s LOOP
						IF overlaps(_p1, _p2) = false THEN
							RAISE EXCEPTION ''Forbiden operation. Inserted references are not overlaps.'';
						END IF;
					END LOOP;
				END LOOP;

				RETURN NEW;
			END;
			$t__%I_%I__ct__tribi$ LANGUAGE plpgsql;', _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table);
		--reference triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I_%I__ct__tribi ON t__%I_%I__ct', _reference_table, _referencing_table, _reference_table, _referencing_table);
		EXECUTE format('CREATE TRIGGER t__%I_%I__ct__tribi BEFORE INSERT ON t__%I_%I__ct FOR EACH ROW EXECUTE PROCEDURE t__%I_%I__ct__ftribi();', 
			_reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table);
		--reference table trigger function before update
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftribu() RETURNS trigger AS $t__%I__temp_data__tribu$
			DECLARE
				_r RECORD;
				_overlaps BOOLEAN;
			BEGIN
				--overlap check
				FOR _r IN
					SELECT valid_time 
						FROM t__%I__temp_data
						WHERE row_oid IN (SELECT referencing_table_row FROM t__%I_%I__ct WHERE reference_table_row = OLD.row_oid)
				LOOP
					_overlaps := (SELECT overlaps(NEW.valid_time, _r.valid_time));
					IF _overlaps = FALSE THEN
						RAISE EXCEPTION ''Forbiden operation. Updated references are not overlaps.'';
					END IF;
				END LOOP;

				RETURN NEW;
			END;
			$t__%I__temp_data__tribu$ LANGUAGE plpgsql;', _reference_table, _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table);
		--reference triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__tribu ON t__%I__temp_data', _reference_table, _reference_table);
		EXECUTE format('CREATE TRIGGER t__%I__temp_data__tribu BEFORE UPDATE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftribu();', 
			_reference_table, _reference_table, _reference_table);
		--referencing table trigger function before update
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftribu() RETURNS trigger AS $t__%I__temp_data__tribu$
			DECLARE
				_r RECORD;
				_overlaps BOOLEAN;
			BEGIN
				--overlap check
				FOR _r IN
					SELECT valid_time 
						FROM t__%I__temp_data
						WHERE row_oid IN (SELECT reference_table_row FROM t__%I_%I__ct WHERE referencing_table_row = OLD.row_oid)
				LOOP
					_overlaps := (SELECT overlaps(NEW.valid_time, _r.valid_time));
					IF _overlaps = FALSE THEN
						RAISE EXCEPTION ''Forbiden operation. Updated references are not overlaps.'';
					END IF;
				END LOOP;

				RETURN NEW;
			END;
			$t__%I__temp_data__tribu$ LANGUAGE plpgsql;', _referencing_table, _referencing_table, _reference_table, _reference_table, _referencing_table, _referencing_table);
		--reference triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__tribu ON t__%I__temp_data', _referencing_table, _referencing_table);
		EXECUTE format('CREATE TRIGGER t__%I__temp_data__tribu BEFORE UPDATE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftribu();', 
			_referencing_table, _referencing_table, _referencing_table);
	END IF;
	IF (_ft = 't' AND _tt = 't') THEN
		--secured by function t__checkTTRI in rule instead of update
	END IF;
	IF (_ft = 'b' AND _tt = 'b') THEN
		--coupled table trigger function before insert
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I_%I__ct__ftribi() RETURNS trigger AS $t__%I_%I__ct__tribi$
			DECLARE
				_p1 PERIOD;
				_p2 PERIOD;
				_overlaps BOOLEAN;
			BEGIN
				--overlap check
				_p1 := (SELECT valid_time FROM t__%I__temp_data WHERE row_oid = NEW.reference_table_row);
				_p2 := (SELECT valid_time FROM t__%I__temp_data WHERE row_oid = NEW.referencing_table_row);
				_overlaps := (SELECT overlaps(_p1, _p2));
				IF _overlaps = FALSE THEN
					RAISE EXCEPTION ''Forbiden operation. Inserted references are not overlaps.'';
				END IF;

				RETURN NEW;
			END;
			$t__%I_%I__ct__tribi$ LANGUAGE plpgsql;', _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table);
		--reference triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I_%I__ct__tribi ON t__%I_%I__ct', _reference_table, _referencing_table, _reference_table, _referencing_table);
		EXECUTE format('CREATE TRIGGER t__%I_%I__ct__tribi BEFORE INSERT ON t__%I_%I__ct FOR EACH ROW EXECUTE PROCEDURE t__%I_%I__ct__ftribi();', 
			_reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table, _referencing_table);
		--reference table trigger function before update
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftribu() RETURNS trigger AS $t__%I__temp_data__tribu$
			DECLARE
				_r RECORD;
				_overlaps BOOLEAN;
			BEGIN
				--overlap check
				FOR _r IN
					SELECT valid_time 
						FROM t__%I__temp_data
						WHERE row_oid IN (SELECT referencing_table_row FROM t__%I_%I__ct WHERE reference_table_row = OLD.row_oid)
				LOOP
					_overlaps := (SELECT overlaps(NEW.valid_time, _r.valid_time));
					IF _overlaps = FALSE THEN
						RAISE EXCEPTION ''Forbiden operation. Updated references are not overlaps.'';
					END IF;
				END LOOP;

				RETURN NEW;
			END;
			$t__%I__temp_data__tribu$ LANGUAGE plpgsql;', _reference_table, _reference_table, _referencing_table, _reference_table, _referencing_table, _reference_table);
		--reference triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__tribu ON t__%I__temp_data', _reference_table, _reference_table);
		EXECUTE format('CREATE TRIGGER t__%I__temp_data__tribu BEFORE UPDATE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftribu();', 
			_reference_table, _reference_table, _reference_table);
		--referencing table trigger function before update
		EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftribu() RETURNS trigger AS $t__%I__temp_data__tribu$
			DECLARE
				_r RECORD;
				_overlaps BOOLEAN;
			BEGIN
				--overlap check
				FOR _r IN
					SELECT valid_time 
						FROM t__%I__temp_data
						WHERE row_oid IN (SELECT reference_table_row FROM t__%I_%I__ct WHERE referencing_table_row = OLD.row_oid)
				LOOP
					_overlaps := (SELECT overlaps(NEW.valid_time, _r.valid_time));
					IF _overlaps = FALSE THEN
						RAISE EXCEPTION ''Forbiden operation. Updated references are not overlaps.'';
					END IF;
				END LOOP;

				RETURN NEW;
			END;
			$t__%I__temp_data__tribu$ LANGUAGE plpgsql;', _referencing_table, _referencing_table, _reference_table, _reference_table, _referencing_table, _referencing_table);
		--referencing triger
		EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__tribu ON t__%I__temp_data', _referencing_table, _referencing_table);
		EXECUTE format('CREATE TRIGGER t__%I__temp_data__tribu BEFORE UPDATE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftribu();', 
			_referencing_table, _referencing_table, _referencing_table);
	END IF;
	IF (_ft = 'b' AND _tt = 't') OR (_ft = 't' AND _tt = 'b') OR (_ft = 'b' AND _tt = 'b') THEN

	END IF;

	CREATE TABLE IF NOT EXISTS t__reference_integrity
	(
	  id SERIAL PRIMARY KEY,
	  reference_table_name TEXT,
	  referencing_table_name TEXT
	);
	EXECUTE format('INSERT INTO t__reference_integrity (reference_table_name, referencing_table_name) VALUES (''%I'', ''%I'')', _reference_table, _referencing_table);
	--create temporal join
	SELECT addTemporalJoin(array[_reference_table, _referencing_table]) INTO _b;

	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION 
	--update valid time in valid time table or bitemporal table
	updateValidTime(_table_name TEXT, _oids OID[], _where TEXT, _valid_time PERIOD) RETURNS bool AS $t__updateValidTime$
DECLARE
	_r RECORD;
	_t TEXT;
	_b BOOLEAN;
	_oid OID;
BEGIN
	_t := t__getTableType(_table_name);
	IF _t <> 'v' AND _t <> 'b' THEN
		RETURN true;
	END IF;
	IF _where = '' THEN
		_where := ' 1 = 1 ';
	END IF;
	--update bitemporal table
	IF _t = 'b' THEN
		FOREACH _oid IN ARRAY _oids LOOP
			FOR _r IN
				EXECUTE format('SELECT valid_time, transaction_time FROM t__%I__temp_data WHERE row_oid = %L AND (%s)', _table_name, _oid, _where)
			LOOP
				_b := t__updateBitemporalValidTime(_oid, _r.valid_time, _r.transaction_time, _valid_time, _table_name, _where, true);
			END LOOP;
		END LOOP;
		RETURN true;
	END IF;
	--update valid time table
	IF _t = 'v' THEN
		FOREACH _oid IN ARRAY _oids LOOP
			EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(%L, %L) WHERE row_oid = %L AND (%s)', _table_name, first(_valid_time), next(_valid_time), _oid, _where); 
		END LOOP; 
		RETURN true;
	END IF;
END;
$t__updateValidTime$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--insert valid time into bitemporal table or valid time table
	insertValidTime(IN _insert TEXT, IN _valid_time PERIOD) RETURNS text AS $$
DECLARE
	_o RECORD;
	_n NAME;
	_table_type TEXT;
	_t TEXT;
BEGIN
	--WARNING!!! in the insert text can not be RETURNING clause
	FOR _t IN 
		EXECUTE format('EXPLAIN (VERBOSE, FORMAT YAML) %s', _insert)
	LOOP
		--insert containing RETURNING clause contains 2 "Output:" string in verbose explain yaml format
		IF _t LIKE '%Output:%Output:%' THEN
			RAISE EXCEPTION 'Insert may contains RETURNING clause, please remove this clause.';
		END IF;
	END LOOP;
	FOR _o IN
		EXECUTE format('%s RETURNING oid, tableoid', _insert)
	LOOP
		EXECUTE format('SELECT relname FROM pg_class WHERE oid = %s', _o.tableoid) INTO _n;
		_table_type := t__getTableType(_n);
		IF _table_type = 'v' OR _table_type = 'b' THEN
			EXECUTE format('UPDATE t__%I__temp_data SET valid_time = period(''%s'') WHERE row_oid = %s;', _n, _valid_time, _o.oid);
			
			RETURN TRUE;
		END IF;
	END LOOP;

	return null;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--create valid time table from _table_name
	createValidTimeTable(IN _table_name TEXT) RETURNS bool AS $$
DECLARE
	_r RECORD;
	_b BOOLEAN;
BEGIN
	--create info table and check if is table already temporal
	EXECUTE format(E'SELECT t__createInfoTableAndCheckTemporal(\'%I\')', _table_name);
	--insert info to infotable
	EXECUTE format('INSERT INTO t__infotable (table_name, valid_time, transaction_time) VALUES (''%I'', TRUE, FALSE);', _table_name);
	--create identifier
	EXECUTE format('ALTER TABLE %I SET WITH OIDS;', _table_name);
	--new PK
	EXECUTE format('SELECT t__setOidAsPK(''%I'')', _table_name);
	--create temporal help temporal data table
	EXECUTE format('CREATE TABLE IF NOT EXISTS t__%I__temp_data (row_oid OID REFERENCES %I(oid) ON DELETE CASCADE, valid_time PERIOD) WITH OIDS;', _table_name, _table_name);
	EXECUTE format('TRUNCATE TABLE t__%I__temp_data;', _table_name);
	--function for trigger after insert (ftai)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftai() RETURNS trigger AS $t__%I__tai$
		BEGIN
			INSERT INTO t__%I__temp_data (row_oid, valid_time) 
				VALUES (NEW.oid, period(''-infinity''::timestamp, ''infinity''::timestamp));
			RETURN NEW;
		END;
		$t__%I__tai$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after insert (tai)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tai ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tai AFTER INSERT ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftai();', _table_name, _table_name, _table_name);
	--function for trigger after delete (ftad)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__temp_data__ftad() RETURNS trigger AS $t__%I__temp_data__tad$
		BEGIN
			DELETE FROM %I WHERE oid = OLD.row_oid;

			RETURN NULL;
		END;
		$t__%I__temp_data__tad$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after delete (tad)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON t__%I__temp_data', _table_name, _table_name);
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__temp_data__tad ON t__%I__temp_data', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__temp_data__tad AFTER DELETE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__temp_data__ftad();', _table_name, _table_name, _table_name);
	--function for trigger after delete
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftad() RETURNS trigger AS $t__%I__tad$
		BEGIN
			DELETE FROM t__%I__temp_data WHERE row_oid = OLD.oid;

			RETURN NULL;
		END;
		$t__%I__tad$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--delete data from temp data table
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tad AFTER DELETE ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftad();', _table_name, _table_name, _table_name);

	--create record in table with temporal data for all records from _table_name table
	FOR _r IN
		EXECUTE format('SELECT oid FROM %I;', _table_name)
	LOOP
		--set each valid time to from -inf to inf
		EXECUTE format('INSERT INTO t__%I__temp_data VALUES (%s, period(''-infinity''::timestamp, ''infinity''::timestamp));', _table_name, _r);
	END LOOP;

	--restructualize section
	SELECT t__create_restructualize(_table_name) INTO _b;

	RETURN _b;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--create transaction time table from _table_name
	createTransactionTimeTable(IN _table_name TEXT) RETURNS bool AS $$
DECLARE
	_o RECORD;
	_b BOOLEAN;
BEGIN
	--create info table and check if is table already temporal
	EXECUTE format(E'SELECT t__createInfoTableAndCheckTemporal(\'%I\')', _table_name);
	--insert info to infotable
	EXECUTE format('INSERT INTO t__infotable (table_name, valid_time, transaction_time) VALUES (''%I'', FALSE, TRUE);', _table_name);
	--create identifier
	EXECUTE format('ALTER TABLE %I SET WITH OIDS;', _table_name);
	--new PK
	EXECUTE format('SELECT t__setOidAsPK(''%I'')', _table_name);
	--create temporal help temporal data table
	EXECUTE format('CREATE TABLE IF NOT EXISTS t__%I__temp_data (row_oid OID REFERENCES %I(oid) ON DELETE CASCADE, transaction_time PERIOD);', _table_name, _table_name);
	EXECUTE format('TRUNCATE TABLE t__%I__temp_data;', _table_name);
	--function for trigger after insert (ftai)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftai() RETURNS trigger AS $t__%I__tai$
		BEGIN
			INSERT INTO t__%I__temp_data (row_oid, transaction_time) 
				VALUES (NEW.oid, period(now(), ''infinity''::timestamp));
			RETURN NEW;
		END;
		$t__%I__tai$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after insert (tai)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tai ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tai AFTER INSERT ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftai();', _table_name, _table_name, _table_name);

	--function for trigger after delete (ftad)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftad() RETURNS trigger AS $t__%I__tad$
		BEGIN
			DELETE FROM %I WHERE oid = OLD.row_oid;
			RETURN NULL;
		END;
		$t__%I__tad$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after delete (tad)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON %I', _table_name, _table_name);
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON t__%I__temp_data', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tad AFTER DELETE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__ftad();', _table_name, _table_name, _table_name);
	--function for rule instead of delete (friod)
	--for all affected rows set end transaction time
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__friod(_oids OID[]) RETURNS void AS $t__%I__friod$
		DECLARE
			_r RECORD;
			_i OID;
			_p PERIOD;
		BEGIN
			FOREACH _i IN ARRAY _oids LOOP
				FOR _p IN
					SELECT transaction_time FROM t__%I__temp_data WHERE row_oid = _i AND now() < next(transaction_time)
				LOOP
					UPDATE t__%I__temp_data SET transaction_time = period(first(_p), now()) WHERE row_oid = _i AND now() < next(transaction_time);
				END LOOP;
			END LOOP;
		END;
		$t__%I__friod$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name);
	--rule instead of delete (riod)
	EXECUTE format('CREATE OR REPLACE RULE "t__%I__riod" AS ON DELETE TO %I DO INSTEAD
		SELECT t__%I__friod(array(SELECT OLD.oid));', _table_name, _table_name, _table_name);
	--rule instead of update (riou)
	/*EXECUTE format('CREATE OR REPLACE RULE "t__%I__riou" AS ON UPDATE TO %I DO INSTEAD (
		DELETE FROM %I WHERE oid IN ((SELECT OLD.oid));
		--INSERT INTO %I VALUES (NEW.*);
		--SELECT t__checkTTRI(''%I''::name, OLD.oid, (SELECT max(oid) FROM %I));--(INSERT INTO %I VALUES (NEW.*) RETURNING oid));
		);', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);*/
	--for all affected rows insert new row
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftbu() RETURNS trigger AS $t__%I__ftbu$
		DECLARE
			_c INTEGER;
			_b BOOLEAN;
		BEGIN
			--deleted cant update
			SELECT count(row_oid) FROM t__%I__temp_data WHERE row_oid = OLD.oid AND now() < next(transaction_time) INTO _c;
			IF _c > 0 THEN
				INSERT INTO %I VALUES (NEW.*);
				SELECT t__checkTTRI(''%I''::name, OLD.oid, (SELECT max(oid) FROM %I)) INTO _b;
			END IF;
			DELETE FROM %I WHERE oid IN ((SELECT OLD.oid));

			RETURN NULL;
		END;
		$t__%I__ftbu$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
	--delete data from temp data table
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tbu ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tbu BEFORE UPDATE ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftbu();', _table_name, _table_name, _table_name);
	--create record in table with temporal data for all records from _table_name table
	FOR _o IN
		EXECUTE format('SELECT oid FROM %I;', _table_name)
	LOOP
		--set each valid time to from -inf to inf
		EXECUTE format('INSERT INTO t__%I__temp_data VALUES (%s, period(''-infinity''::timestamp, ''infinity''::timestamp));', _table_name, _o);
	END LOOP;

	--restructualize section
	SELECT t__create_restructualize(_table_name) INTO _b;

	RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
	--create bitemporal table from _table_name
	createBitemporalTable(IN _table_name TEXT) RETURNS bool AS $$
DECLARE
	_r RECORD;
	_b BOOLEAN;
BEGIN
	--create info table and check if is table already temporal
	EXECUTE format('SELECT t__createInfoTableAndCheckTemporal(''%I'')', _table_name);
	--insert info to infotable
	EXECUTE format('INSERT INTO t__infotable (table_name, valid_time, transaction_time) VALUES (''%I'', TRUE, TRUE);', _table_name);
	--create identifier
	EXECUTE format('ALTER TABLE %I SET WITH OIDS;', _table_name);
	--new PK
	EXECUTE format('SELECT t__setOidAsPK(''%I'')', _table_name);
	--create temporal help temporal data table
	EXECUTE format('CREATE TABLE IF NOT EXISTS t__%I__temp_data (row_oid OID REFERENCES %I(oid) ON DELETE CASCADE, valid_time PERIOD, transaction_time PERIOD) WITH OIDS;', _table_name, _table_name);
	EXECUTE format('TRUNCATE TABLE t__%I__temp_data;', _table_name);
	--function for trigger after insert (ftai)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftai() RETURNS trigger AS $t__%I__tai$
		BEGIN
			INSERT INTO t__%I__temp_data (row_oid, valid_time, transaction_time) 
				VALUES (NEW.oid, period(''-infinity''::timestamp, ''infinity''::timestamp), period(now(), ''infinity''::timestamp));
			RETURN NEW;
		END;
		$t__%I__tai$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after insert (tai)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tai ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tai AFTER INSERT ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftai();', _table_name, _table_name, _table_name);

	--function for trigger after delete (ftad)
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftad() RETURNS trigger AS $t__%I__tad$
		BEGIN
			DELETE FROM %I WHERE oid = OLD.row_oid;

			RETURN NULL;
		END;
		$t__%I__tad$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name);
	--create trigger after delete (tad)
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON %I', _table_name, _table_name);
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tad ON t__%I__temp_data', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tad AFTER DELETE ON t__%I__temp_data FOR EACH ROW EXECUTE PROCEDURE t__%I__ftad();', _table_name, _table_name, _table_name);
	--function for rule instead of delete (friod)
	--for all affected rows set end transaction time
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__friod(_oids OID[]) RETURNS void AS $t__%I__friod$
		DECLARE
			_r RECORD;
			_i OID;
			_p PERIOD;
		BEGIN
			FOREACH _i IN ARRAY _oids LOOP
				FOR _p IN
					SELECT transaction_time FROM t__%I__temp_data WHERE row_oid = _i
				LOOP
					UPDATE t__%I__temp_data SET transaction_time = period(first(_p), now()) WHERE row_oid = _i AND last(transaction_time) > now();
				END LOOP;
			END LOOP;
		END;
		$t__%I__friod$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name);
	--rule instead of delete (riod)
	EXECUTE format('CREATE OR REPLACE RULE "t__%I__riod" AS ON DELETE TO %I DO INSTEAD
		SELECT t__%I__friod((SELECT array_agg(OLD.oid)));', _table_name, _table_name, _table_name);
	
	--create record in table with temporal data for all records from _table_name table
	--for all affected rows insert new row
	EXECUTE format('CREATE OR REPLACE FUNCTION t__%I__ftbu() RETURNS trigger AS $t__%I__ftbu$
		DECLARE
			_c INTEGER;
			_b BOOLEAN;
			_r RECORD;
		BEGIN
			--deleted cant update
			SELECT count(row_oid) FROM t__%I__temp_data WHERE row_oid = OLD.oid AND now() < next(transaction_time) INTO _c;
			IF _c > 0 THEN
				INSERT INTO %I VALUES (NEW.*);
				--update valid_time
				_b := true;
				FOR _r IN
					SELECT valid_time FROM t__%I__temp_data WHERE row_oid = OLD.oid
				LOOP
					IF _b = true THEN
						UPDATE t__%I__temp_data SET valid_time = _r.valid_time WHERE row_oid = (SELECT max(oid) FROM %I);
						_b := false;
					ELSE
						INSERT INTO t__%I__temp_data VALUES ((SELECT max(oid) FROM %I), _r.valid_time, period(now(), ''infinity''::timestamp));
					END IF;
				END LOOP;
				SELECT t__checkBTRI(''%I''::name, OLD.oid, (SELECT max(oid) FROM %I)) INTO _b;
			END IF;
			DELETE FROM %I WHERE oid IN ((SELECT OLD.oid));

			RETURN NULL;
		END;
		$t__%I__ftbu$ LANGUAGE plpgsql;', _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name, _table_name);
	--delete data from temp data table
	EXECUTE format('DROP TRIGGER IF EXISTS t__%I__tbu ON %I', _table_name, _table_name);
	EXECUTE format('CREATE TRIGGER t__%I__tbu BEFORE UPDATE ON %I FOR EACH ROW EXECUTE PROCEDURE t__%I__ftbu();', _table_name, _table_name, _table_name);
	FOR _r IN
		EXECUTE format('SELECT oid FROM %I;', _table_name)
	LOOP
		--set each valid time to from -inf to inf
		EXECUTE format('INSERT INTO t__%I__temp_data VALUES (%s, period(''-infinity''::timestamp, ''infinity''::timestamp), period(now(), ''infinity''::timestamp));', _table_name, _r);
	END LOOP;

	--restructualize section
	SELECT t__create_restructualize(_table_name) INTO _b;
	RETURN true;
END;
$$ LANGUAGE plpgsql;
