CREATE OR REPLACE FUNCTION dz_topo.array_merge(
    arr1  ANYARRAY
   ,arr2  ANYARRAY
) RETURNS ANYARRAY
IMMUTABLE
AS $BODY$
   SELECT ARRAY_AGG(
      DISTINCT elem ORDER BY elem
   )
   FROM (
      SELECT 
      UNNEST(arr1) AS elem 
      UNION
      SELECT 
      UNNEST(arr2)
   ) s;

$BODY$
LANGUAGE sql;

GRANT EXECUTE ON FUNCTION dz_topo.array_merge(
    ANYARRAY
   ,ANYARRAY
) TO PUBLIC;
