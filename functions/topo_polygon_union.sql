CREATE OR REPLACE FUNCTION dz_topo.topo_polygon_union(
    IN  p_layer_schema     VARCHAR
   ,IN  p_layer_table      VARCHAR
   ,IN  p_layer_column     VARCHAR
   ,IN  p_topogeom_ids     INTEGER[]
   ,IN  p_remove_holes     BOOLEAN DEFAULT FALSE
   ,OUT out_areasqkm       NUMERIC
   ,OUT out_geometry       GEOMETRY
   ,OUT out_return_code    NUMERIC
   ,OUT out_status_message VARCHAR
)
VOLATILE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   rec_topo          RECORD;
   rec_layer         RECORD;
   ary_edges         INTEGER[];
   ary_rings         INTEGER[];
   int_edge_count    INTEGER;
   sdo_ring          GEOMETRY;
   int_sanity        INTEGER;
   ary_polygons      GEOMETRY[];
   ary_holes         GEOMETRY[];
   boo_running       BOOLEAN;
   
BEGIN

   out_return_code := 0;

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
   -- Step 10
   -- Create temporary edge table
   ----------------------------------------------------------------------------
   IF dz_topo.temp_table_exists('tmp_dz_edges')
   THEN
      TRUNCATE TABLE tmp_dz_edges;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_edges(
          edge_id        INTEGER 
         ,interior_side  VARCHAR(1)
         ,start_node_id  INTEGER
         ,end_node_id    INTEGER
         ,shape          GEOMETRY 
         ,touch_count    INTEGER
      );

      CREATE UNIQUE INDEX tmp_dz_edges_pk
      ON tmp_dz_edges(edge_id);
      
      CREATE INDEX tmp_dz_edges_01i
      ON tmp_dz_edges(start_node_id);
            
      CREATE INDEX tmp_dz_edges_02i
      ON tmp_dz_edges(end_node_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 20
   -- Create temporary ring table
   ----------------------------------------------------------------------------
   IF dz_topo.temp_table_exists('tmp_dz_rings')
   THEN
      TRUNCATE TABLE tmp_dz_rings;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_rings(
          ring_id        INTEGER
         ,ring_type      VARCHAR(1)
         ,shape          GEOMETRY
      );

      CREATE UNIQUE INDEX tmp_dz_rings_pk
      ON tmp_dz_rings(ring_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Create temporary face table
   ----------------------------------------------------------------------------
   IF dz_topo.temp_table_exists('tmp_dz_faces')
   THEN
      TRUNCATE TABLE tmp_dz_faces;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_faces(
          face_id        INTEGER
      );

      CREATE UNIQUE INDEX tmp_dz_faces_pk
      ON tmp_dz_faces(face_id);

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Load the polygon faces into temporary table
   ----------------------------------------------------------------------------   
   EXECUTE '
   INSERT INTO tmp_dz_faces(
      face_id
   )
   SELECT 
   a.element_id 
   FROM 
   ' || quote_ident(rec_topo.name) || '.relation a
   WHERE
       a.layer_id     = ' || rec_layer.layer_id::VARCHAR || '
   AND a.topogeo_id   = ANY($1::INTEGER[])
   AND a.element_type = 3 
   ON CONFLICT DO NOTHING'
   USING p_topogeom_ids;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Load the edges into temporary table where face is only on a single side
   ----------------------------------------------------------------------------
   EXECUTE '
   INSERT INTO tmp_dz_edges(
       edge_id
      ,interior_side
      ,start_node_id
      ,end_node_id
      ,shape
      ,touch_count
   )
   SELECT
    a1.edge_id
   ,''L''
   ,a1.start_node
   ,a1.end_node
   ,a1.geom
   ,0
   FROM
   ' || quote_ident(rec_topo.name) || '.edge_data a1 
   WHERE 
       EXISTS (SELECT 1 FROM tmp_dz_faces f1 where f1.face_id = a1.left_face)  
   AND NOT EXISTS (SELECT 1 FROM tmp_dz_faces f2 WHERE f2.face_id = a1.right_face) 
   UNION ALL SELECT 
    a2.edge_id
   ,''R''
   ,a2.start_node
   ,a2.end_node
   ,a2.geom
   ,0
   FROM 
   ' || quote_ident(rec_topo.name) || '.edge_data a2 
   WHERE 
       NOT EXISTS (SELECT 1 FROM tmp_dz_faces f3 where f3.face_id = a2.left_face) 
   AND EXISTS (SELECT 1 FROM tmp_dz_faces f4 WHERE f4.face_id = a2.right_face)';
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Pull out any single edge rings
   ----------------------------------------------------------------------------
   WITH insertedges AS (
      INSERT INTO tmp_dz_rings(
          ring_id
         ,ring_type
         ,shape
      )
      SELECT
       a.edge_id
      ,CASE
       WHEN   (ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'L')
       OR (NOT ST_IsPolygonCCW(ST_MakePolygon(a.shape)) AND a.interior_side = 'R')
       THEN
         'E'
       ELSE
         'I'
       END AS ring_type
      ,a.shape
      FROM
      tmp_dz_edges a
      WHERE
      a.start_node_id = a.end_node_id
      RETURNING ring_id
   )
   SELECT
   array_agg(ring_id)
   INTO ary_edges
   FROM
   insertedges;
   
   DELETE FROM tmp_dz_edges
   WHERE edge_id = ANY(ary_edges);
   
   ----------------------------------------------------------------------------
   -- Step 70
   -- Recursively pull out rings
   ----------------------------------------------------------------------------
   SELECT 
   COUNT(*)
   INTO int_edge_count
   FROM
   tmp_dz_edges a; 
   
   int_sanity := 1;
   boo_running := TRUE;
   WHILE boo_running
   LOOP
      boo_running := dz_topo.edges2rings();
      
      int_sanity := int_sanity + 1;
      
      IF int_sanity > 10000
      THEN
         RAISE EXCEPTION 'sanity check';
         
      END IF;
      
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 80
   -- Organize the polygons outer and inner rings
   ----------------------------------------------------------------------------
   FOR rec IN 
      SELECT
       a.ring_id 
      ,a.shape
      FROM
      tmp_dz_rings a
      WHERE
      a.ring_type = 'E'
      ORDER BY
      ST_Area(ST_MakePolygon(a.shape)) ASC
   LOOP
      ary_holes := NULL;
      
      SELECT
       array_agg(b.ring_id)
      ,array_agg(b.shape)
      INTO 
       ary_rings
      ,ary_holes
      FROM
      tmp_dz_rings b
      WHERE
          b.ring_type = 'I'
      AND ST_Intersects(
          ST_MakePolygon(rec.shape)
         ,b.shape
      );
   
      IF ary_rings IS NULL
      OR array_length(ary_rings,1) = 0
      THEN
         sdo_ring := ST_MakePolygon(rec.shape);
         
      ELSE
         IF p_remove_holes
         THEN
            sdo_ring := ST_MakePolygon(rec.shape);
         
         ELSE
            sdo_ring := ST_MakePolygon(rec.shape,ary_holes);
            
         END IF;
         
         DELETE FROM tmp_dz_rings
         WHERE ring_id = ANY(ary_rings);
         
      END IF;
   
      ary_polygons := array_append(ary_polygons,sdo_ring);
   
   END LOOP;
      
   ----------------------------------------------------------------------------
   -- Step 90
   -- Generate the final geometries
   ----------------------------------------------------------------------------
   IF array_length(ary_polygons,1) = 1
   THEN
      out_geometry := ST_ForcePolygonCCW(ary_polygons[1]);
      
   ELSE
      out_geometry := ST_ForcePolygonCCW(ST_Collect(ary_polygons));
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 100
   -- Calculate the area
   ----------------------------------------------------------------------------
   out_areasqkm := ST_Area(ST_Transform(out_geometry,4326)::GEOGRAPHY) * 0.0000010;
   
   RETURN;
   
END;
$BODY$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION dz_topo.topo_polygon_union(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,INTEGER[]
   ,BOOLEAN
) TO PUBLIC;
