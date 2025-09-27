-- Food Log Statistics Template
-- This file contains the INSERT statement for the foodlogstats MV template

INSERT INTO _wh.mv_templates (
    template_name,
    description,
    query_template,
    column_definitions,
    indexes
) VALUES (
    'foodlogstats',
    'Daily food log statistics with waste tracking, location, and cost data',
    $template$
SELECT
    pf.id,
    pl.name AS store,
    s.station,
    pf.validation_status,
    pr.name AS region,
    pf.action_taken_id,
    pa.name AS action_taken,
    pi.name,
    pf.quantity,
    pp.average_weight,
    (pf.quantity - pp.average_weight) AS weight,
    pfo.name AS food_category,
    pi.cost_per_lb,
    pi.target_weight,
    pi.price_per_lb,
    par.name AS action_reason,
    pi.client_id,
    pf.logged_time,
    pf.flags
FROM phood_foodlogsum pf
LEFT JOIN phood_actionreason par ON (pf.action_reason_id = par.id)
LEFT JOIN phood_location pl ON (pf.location_id = pl.id)
LEFT JOIN phood_region pr ON (pl.region_id = pr.id)
LEFT JOIN phood_actiontaken pa ON pf.action_taken_id = pa.id
LEFT JOIN phood_inventoryitem pi ON pf.inventory_item_id = pi.id
LEFT JOIN phood_pan pp ON pp.id = pf.pan_id
LEFT JOIN phood_foodcategory pfo ON pfo.id = pi.food_category_id
LEFT JOIN (
    SELECT DISTINCT ps.name AS station
    , pm.inventory_item_id
    , pm.location_id
    , pm.station_id
    FROM phood_menuitem pm
    LEFT JOIN phood_station ps ON ps.id = pm.station_id
) s ON (pf.inventory_item_id = s.inventory_item_id AND s.location_id = pf.location_id)
WHERE pf.logged_time::DATE = '{TARGET_DATE}'::DATE
$template$,
    $columns$
id INTEGER,
store TEXT,
station TEXT,
validation_status TEXT,
region TEXT,
action_taken_id INTEGER,
action_taken TEXT,
name TEXT,
quantity NUMERIC,
average_weight NUMERIC,
weight NUMERIC,
food_category TEXT,
cost_per_lb NUMERIC,
target_weight NUMERIC,
price_per_lb NUMERIC,
action_reason TEXT,
client_id TEXT,
logged_time TIMESTAMPTZ,
flags TEXT
$columns$,
'CREATE UNIQUE INDEX idx_{SCHEMA}_{VIEW_NAME}_id ON {SCHEMA}.{VIEW_NAME} (id);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_logged_time ON {SCHEMA}.{VIEW_NAME} (logged_time);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_store ON {SCHEMA}.{VIEW_NAME} (store);
CREATE INDEX idx_{SCHEMA}_{VIEW_NAME}_action_taken_id ON {SCHEMA}.{VIEW_NAME} (action_taken_id);'
);