CREATE OR REPLACE FUNCTION dz_topo.remove_sliver_gaps(
    IN  p_layer_schema     VARCHAR
   ,IN  p_layer_table      VARCHAR
   ,IN  p_layer_column     VARCHAR
   ,IN  p_slivertbl_owner  VARCHAR
   ,IN  p_slivertbl_name   VARCHAR
   ,IN  p_slivertbl_where  VARCHAR DEFAULT NULL
   ,IN  p_generate_stats   BOOLEAN DEFAULT TRUE
) RETURNS TABLE(
    incoming_face_id         INTEGER
   ,incoming_face_areasqkm   NUMERIC
   ,incoming_face_minradius  NUMERIC
   ,target_topoid            INTEGER
   ,common_length            NUMERIC
   ,orig_target_areasqkm     NUMERIC
   ,new_target_areasqkm      NUMERIC
   ,orig_target_mbr_areasqkm NUMERIC
   ,new_target_mbr_areasqkm  NUMERIC
   ,orig_target_minradiuskm  NUMERIC
   ,new_target_minradiuskm   NUMERIC
   ,return_code              INTEGER
   ,status_message           VARCHAR
)
VOLATILE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   rec_topo          RECORD;
   rec_layer         RECORD;
   str_sql           VARCHAR;
   int_count         INTEGER;
   int_target        INTEGER;
   num_target_length NUMERIC;
   orig_target       GEOMETRY;
   
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
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create temporary edge table
   ----------------------------------------------------------------------------
   IF dz_topo.temp_table_exists('tmp_dz_edge_neighbors')
   THEN
      TRUNCATE TABLE tmp_dz_edge_neighbors;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_edge_neighbors(
          edge_id        INTEGER 
         ,other_face_id  INTEGER
         ,edge_length    NUMERIC
         ,topogeo_id    INTEGER
      );

      CREATE UNIQUE INDEX tmp_dz_edge_neighbors_pk
      ON tmp_dz_edge_neighbors(edge_id);
      
      CREATE INDEX tmp_dz_edge_neighbors_01i
      ON tmp_dz_edge_neighbors(topogeo_id);

   END IF;

   ----------------------------------------------------------------------------
   -- Step 30
   -- Create the loop SQL statement
   ----------------------------------------------------------------------------
   str_sql := '
      SELECT
       a.face_id
      ,a.areasqkm
      ,a.minradiuskm
      ,a.shape
      FROM
      ' || quote_ident(p_slivertbl_owner) || '.' || quote_ident(p_slivertbl_name) || ' a
   ';
   
   IF p_slivertbl_where IS NOT NULL
   THEN
      str_sql := str_sql || 'WHERE ' || REPLACE(
          REPLACE(p_slivertbl_where,'--','')
         ,';'
         ,''
      );
   
   END IF;
    
   ----------------------------------------------------------------------------
   -- Step 40
   -- Loop through the face_ids to process
   ----------------------------------------------------------------------------
   FOR rec IN EXECUTE str_sql
   LOOP
   
      TRUNCATE TABLE tmp_dz_edge_neighbors;
      
      EXECUTE '
      INSERT INTO tmp_dz_edge_neighbors(
         edge_id
        ,other_face_id
        ,edge_length
        ,topogeo_id
      )
      SELECT
       a.edge_id
      ,a.other_face_id
      ,a.edge_length
      ,b.topogeo_id
      FROM (
         SELECT
          aa.edge_id
         ,CASE 
          WHEN aa.left_face = $1
          THEN
            aa.right_face
          ELSE
            aa.left_face
          END AS other_face_id
         ,ST_LENGTH(aa.geom) AS edge_length
         FROM
         ' || quote_ident(rec_topo.name) || '.edge aa
         WHERE
            aa.right_face = $2
         OR aa.left_face  = $3
      ) a
      JOIN
      ' || quote_ident(rec_topo.name) || '.relation b 
      ON
      b.element_id = a.other_face_id'
      USING      
       rec.face_id
      ,rec.face_id
      ,rec.face_id;
      
      GET DIAGNOSTICS int_count = ROW_COUNT;
      
      SELECT
       a.topogeo_id
      ,a.sum_edge_length
      INTO 
       int_target
      ,num_target_length
      FROM (
         SELECT
          aa.topogeo_id
         ,SUM(aa.edge_length) AS sum_edge_length
         FROM
         tmp_dz_edge_neighbors aa
         GROUP BY
         aa.topogeo_id
      ) a
      ORDER BY
      a.sum_edge_length DESC
      LIMIT 1;
      
      IF p_generate_stats
      THEN
         EXECUTE '
         SELECT
         ST_TRANSFORM(a.' || quote_ident(p_layer_column) || '::GEOMETRY,4326)
         FROM
         ' || quote_ident(p_layer_schema) || '.' || quote_ident(p_layer_table) || ' a
         WHERE
         (a.' || quote_ident(p_layer_column) || ').id = $1 '
         INTO
         orig_target
         USING
         rec.face_id;
         
         orig_target_areasqkm     := ST_AREA(orig_target::GEOGRAPHY)::NUMERIC / 1000000;
         orig_target_minradiuskm  := ((SELECT radius FROM ST_MINIMUMBOUNDINGRADIUS(orig_target)) / 1000)::NUMERIC;
         orig_target_mbr_areasqkm := ST_AREA(ST_ENVELOPE(orig_target)::GEOGRAPHY)::NUMERIC / 1000000;
         
      END IF;

      EXECUTE '
      UPDATE 
      ' || quote_ident(p_layer_schema) || '.' || quote_ident(p_layer_table) || ' a
      SET ' || quote_ident(p_layer_column) || ' = TopoGeom_addElement(' || quote_ident(p_layer_column) || ',$1)
      WHERE
      (a.' || quote_ident(p_layer_column) || ').id = $2 '
      USING ARRAY[rec.face_id,3]::topology.topoelement,int_target;

      IF p_generate_stats
      THEN
         EXECUTE '
         SELECT
         ST_TRANSFORM(a.' || quote_ident(p_layer_column) || '::GEOMETRY,4326)
         FROM
         ' || quote_ident(p_layer_schema) || '.' || quote_ident(p_layer_table) || ' a
         WHERE
         (a.' || quote_ident(p_layer_column) || ').id = $1 '
         INTO
         orig_target
         USING
         rec.face_id;
         
         new_target_areasqkm     := ST_AREA(orig_target::GEOGRAPHY)::NUMERIC / 1000000;
         new_target_minradiuskm  := ((SELECT radius FROM ST_MINIMUMBOUNDINGRADIUS(orig_target)) / 1000)::NUMERIC;
         new_target_mbr_areasqkm := ST_AREA(ST_ENVELOPE(orig_target)::GEOGRAPHY)::NUMERIC / 1000000;
         
      END IF;
      
      incoming_face_id         := rec.face_id;
      incoming_face_areasqkm   := rec.areasqkm;
      incoming_face_minradius  := rec.minradiuskm;
      target_topoid            := int_target;
      common_length            := num_target_length;
      return_code              := 0;
      status_message           := NULL;
      RETURN NEXT;
   
   END LOOP;
   
   RETURN ;
   
END;
$BODY$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION dz_topo.remove_sliver_gaps(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,BOOLEAN
) TO PUBLIC;
