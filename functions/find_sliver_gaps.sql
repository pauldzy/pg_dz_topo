CREATE OR REPLACE FUNCTION dz_topo.find_sliver_gaps(
    IN  p_layer_schema          VARCHAR
   ,IN  p_layer_table           VARCHAR
   ,IN  p_layer_column          VARCHAR
   ,IN  p_areasqkm_threshold    NUMERIC DEFAULT 0.001
   ,IN  p_minradiuskm_threshold NUMERIC DEFAULT 0.001
   ,IN  p_limit_value           INTEGER DEFAULT NULL
) RETURNS TABLE (
    face_id     INTEGER
   ,areasqkm    NUMERIC
   ,minradiuskm NUMERIC
   ,shape       GEOMETRY
)
STABLE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   rec_topo          RECORD;
   rec_layer         RECORD;
   int_count         INTEGER;
   str_sql           VARCHAR;
   
BEGIN

   ----------------------------------------------------------------------------
   -- Step 10
   -- Get and validate topology and source layer ids
   ----------------------------------------------------------------------------
   rec_topo := FindTopology(
       p_layer_schema
      ,p_layer_table
      ,p_layer_column
   );
   IF rec_topo.id IS NULL
   THEN
      RAISE EXCEPTION 'topology not found';
      
   END IF;
   
   rec_layer := FindLayer(
       schema_name    := p_layer_schema
      ,table_name     := p_layer_table
      ,feature_column := p_layer_column
   );
   IF rec_layer.layer_id IS NULL
   THEN
      RAISE EXCEPTION 'topology layer not found';
      
   END IF;
   
   IF rec_layer.feature_type != 3
   THEN
      RAISE EXCEPTION 'topology layer is not polygon feature type';
   
   END IF;
   
   SELECT 
   COUNT(*)
   INTO int_count
   FROM
   topology.layer a
   WHERE
   topology_id = rec_topo.id;
   
   IF int_count != 1
   THEN
      RAISE EXCEPTION 'topology has multiple layers which this process is not designed to work with';
   
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Identify the sliver faces
   ----------------------------------------------------------------------------
   str_sql := '
      SELECT
       a.face_id
      ,a.areasqkm
      ,a.minradiuskm
      ,a.shape
      FROM (
         SELECT 
          aa.face_id
         ,(ST_AREA(ST_TRANSFORM(aa.shape,4326)::GEOGRAPHY) / 1000000)::NUMERIC AS areasqkm
         ,((SELECT radius FROM ST_MINIMUMBOUNDINGRADIUS(ST_TRANSFORM(aa.shape,4326))) / 1000)::NUMERIC AS minradiuskm 
         ,aa.shape   
         FROM (
            SELECT
             aaa.face_id
            ,ST_GetFaceGeometry(''' || quote_ident(rec_topo.name) || ''',aaa.face_id) AS shape
            FROM (
               SELECT
               aaaa.face_id
               FROM 
               ' || quote_ident(rec_topo.name) || '.face aaaa   
               LEFT JOIN
               ' || quote_ident(rec_topo.name) || '.relation bbbb
               ON
               aaaa.face_id = bbbb.element_id
               WHERE
                   aaaa.face_id > 0
               AND bbbb.element_id IS NULL
            ) aaa
         ) aa
      ) a
      WHERE
          a.areasqkm    <= $1
      AND a.minradiuskm <= $2';
      
   IF p_limit_value IS NOT NULL
   THEN
      str_sql := str_sql || ' LIMIT ' || p_limit_value::VARCHAR;
      
   END IF;
   
   RETURN QUERY EXECUTE str_sql
   USING
    p_areasqkm_threshold
   ,p_minradiuskm_threshold;
   
END;
$BODY$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION dz_topo.find_sliver_gaps(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,NUMERIC
   ,NUMERIC
   ,INTEGER
) TO PUBLIC;
