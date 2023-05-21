CREATE OR REPLACE FUNCTION dz_topo.array_compress(
   arr1  ANYARRAY
) RETURNS ANYARRAY
IMMUTABLE
AS $BODY$
   SELECT ARRAY_AGG(
      DISTINCT elem ORDER BY elem
   )
   FROM (
      SELECT 
      UNNEST(arr1) AS elem 
   ) s;

$BODY$
LANGUAGE sql;

GRANT EXECUTE ON FUNCTION dz_topo.array_compress(
    ANYARRAY
) TO PUBLIC;
