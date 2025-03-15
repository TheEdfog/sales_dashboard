-- DROP FUNCTION std8_69.f_load_plant_mart(date, date, text);

CREATE OR REPLACE FUNCTION std8_69.f_load_plant_mart(p_start date, p_end date, p_to_table text DEFAULT NULL::text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	

-- p_start and p_end both must be inclusive

DECLARE
	v_table_name TEXT;
	v_sql TEXT;
	v_result INT;
BEGIN

	IF p_to_table IS NULL THEN 
		v_table_name := 'std8_69.mart_plants_' || to_char(p_start, 'YYYYMMDD') || '_' || to_char(p_end, 'YYYYMMDD'); 
	ELSE 
		v_table_name := p_to_table; 
	END IF;

	PERFORM std8_69.f_load_write_log(p_log_type := 'INFO',
							p_log_message := 'Start f_load_plant_mart('||p_start||', '|| p_end || ', ' || COALESCE(p_to_table, 'NULL') || ')',
							p_location := 'f_load_plant_mart');
	
	PERFORM std8_69.f_load_write_log(p_log_type := 'INFO',
							p_log_message := 'Start DROP TABLE IF EXISTS '||v_table_name,
							p_location := 'f_load_plant_mart');

	EXECUTE 'DROP TABLE IF EXISTS '|| v_table_name;

	RAISE NOTICE 'DROP TABLE IF EXISTS %', v_table_name;
	
	v_sql = 'CREATE TABLE ' || v_table_name || ' 
	WITH 
	(
		appendonly = true,
		orientation = row,
		compresstype = zstd,
		compresslevel = 1
	)
	AS 
    WITH 
    -- gross sales:
    -- total amount of money with no withdraw of discount yet
    -- total amount of products sold
    -- count of distinct bills
    ct_gs AS
    (
        SELECT
            bh.plant, 
            SUM(bi.rpa_sat) AS revenue, 
            SUM(bi.qty) AS quantity, 
            COUNT(DISTINCT bh.billnum) AS bills_count
        FROM
            std8_69.bills_head bh
        JOIN
            std8_69.bills_item bi ON bi.billnum = bh.billnum
        WHERE bh.calday BETWEEN ''' || p_start || '''::date AND ''' || p_end || '''::date
        GROUP BY bh.plant
    ),
    -- promo totals
    -- total amount of discount to be held from gross sales
    ct_pt AS 
    (
        SELECT 
        f_p.plant AS plant, 
        SUM(
            CASE 
                WHEN f_p.type_id = ''001'' THEN f_p.discount
                -- only 1 product can be used to apply coupon
                -- so, we divide by their number to calculate the discount
                WHEN f_p.type_id = ''002'' THEN f_p.rpa_sat::numeric / f_p.qty * (f_p.discount / 100.0)
                ELSE 0::NUMERIC 
            END
        ) AS discount
        FROM 
        (
            -- DISTINCT ON gets the 1st row partitioned by coupon_id
            -- which is the same as giving discount only for the 1st product in the bill
            SELECT DISTINCT ON(c.coupon_id)
                c.plant, p.type_id, p.discount, bi.rpa_sat, bi.qty
            FROM
                std8_69.coupons c
            JOIN std8_69.bills_item bi 
                ON bi.billnum = c.billnum AND bi.material = c.material
            JOIN
                std8_69.bills_head bh ON bh.plant = c.plant AND bh.billnum = bi.billnum
            JOIN 
                std8_69.promos p ON p.promo_id = c.promo_id AND p.material = c.material 
            WHERE bh.calday BETWEEN ''' || p_start || '''::date AND ''' || p_end || '''::date
            -- get the first row with the lowest billnum 
            -- according to the description of coupons table
            ORDER BY c.coupon_id, bi.billnum
        ) AS f_p
        GROUP BY f_p.plant
        ORDER BY f_p.plant
    ),
    -- traffic
    -- volume of traffic 
    ct_t AS 
    (
        SELECT t.plant, SUM(t.quantity) AS traffic
        FROM std8_69.traffic t 
        WHERE t."date" BETWEEN ''' || p_start || '''::date AND ''' || p_end || '''::date
        GROUP BY t.plant
    ),
    -- products with coupons
    -- number of products with coupons
    ct_c AS
    (
        SELECT c.plant, COUNT(1) AS quantity
        FROM std8_69.coupons c
        WHERE c."date" BETWEEN ''' || p_start || '''::date AND ''' || p_end || '''::date
        GROUP BY c.plant
    )
    -- wrap it all up
    SELECT 
        -- "Завод (Текст)
        s.plant, 
        -- "Завод" (Текст) 
        s.txt,
        -- Оборот
        ct_gs.revenue,
        -- Скидки по купонам
        ct_pt.discount,
        -- Оборот с учетом скидки
        (ct_gs.revenue - ct_pt.discount) AS profit,
        -- кол-во проданных товаров
        ct_gs.quantity,
        -- Количество чеков
        ct_gs.bills_count,
        -- Трафик
        ct_t.traffic,
        -- Кол-во товаров по акции
        ct_c.quantity AS promo_sold,
        -- Доля товаров со скидкой
	    ROUND(CASE WHEN ct_gs.quantity = 0 THEN 0 ELSE ct_c.quantity / ct_gs.quantity::numeric * 100.0 END, 1) AS promo_rate,
	    -- Среднее количество товаров в чеке 
	    ROUND(CASE WHEN ct_gs.bills_count = 0 THEN 0 ELSE ct_gs.quantity / ct_gs.bills_count::numeric END, 2) AS avg_qty_per_bill,
	    -- Коэффициент конверсии магазина, % 
	    ROUND(CASE WHEN ct_t.traffic = 0 THEN 0 ELSE ct_gs.bills_count / ct_t.traffic::numeric * 100.0 END, 2) AS btt,
	    -- Средний чек, руб 
	    ROUND(CASE WHEN ct_gs.bills_count = 0 THEN 0 ELSE ct_gs.revenue / ct_gs.bills_count::numeric END, 1) AS avg_bill_rub,
	    -- Средняя выручка на одного посетителя, руб 
	    ROUND(CASE WHEN ct_t.traffic = 0 THEN 0 ELSE ct_gs.revenue / ct_t.traffic::numeric END, 1) AS avg_customer_rub
    FROM std8_69.stores s 
    JOIN ct_gs ON ct_gs.plant = s.plant
    JOIN ct_pt ON ct_pt.plant = s.plant
    JOIN ct_t ON ct_t.plant = s.plant
    JOIN ct_c ON ct_c.plant = s.plant
    ORDER BY s.plant
    ';

	PERFORM std8_69.f_load_write_log(p_log_type := 'INFO',
							p_log_message := 'Start '|| v_sql,
							p_location := 'f_load_plant_mart');

	RAISE NOTICE 'Start %', v_sql;

    EXECUTE v_sql;

	PERFORM std8_69.f_load_write_log(p_log_type := 'INFO',
							p_log_message := 'Mart is '|| v_table_name,
							p_location := 'f_load_plant_mart');

	RAISE NOTICE 'Mart is %', v_table_name;

	-- record an amount of rows inserted into the main table
	execute 'select count(1) from '||v_table_name into v_result;

	return v_result;
	
END;

$$
EXECUTE ON ANY;
