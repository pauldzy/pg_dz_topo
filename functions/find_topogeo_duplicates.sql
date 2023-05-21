CREATE OR REPLACE FUNCTION dz_topo.find_topogeo_duplicates(
    IN  p_layer_schema          VARCHAR
   ,IN  p_layer_table           VARCHAR
   ,IN  p_layer_column          VARCHAR
   ,IN  p_envelope              GEOMETRY DEFAULT NULL
   ,IN  p_limit_value           INTEGER  DEFAULT NULL
) RETURNS TABLE (
    objectid     INTEGER
   ,topogeo_dups INTEGER[]
   ,areasqkm     NUMERIC
   ,shape        GEOMETRY
)
VOLATILE
AS
$BODY$ 
DECLARE
   rec               RECORD;
   rec_topo          RECORD;
   rec_layer         RECORD;
   int_count         INTEGER;
   str_sql           VARCHAR;
   ary_topogeo_ids   INTEGER[];
   ary_topogeo_chk   INTEGER[];
   
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
   -- Create temporary tables
   ----------------------------------------------------------------------------
   IF dz_topo.temp_table_exists('tmp_dz_overlapped_faces')
   THEN
      TRUNCATE TABLE tmp_dz_overlapped_faces;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_overlapped_faces(
          face_id        INTEGER 
         ,topogeo_count  INTEGER
      );

      CREATE UNIQUE INDEX tmp_dz_overlapped_faces_pk
      ON tmp_dz_overlapped_faces(face_id);

   END IF;
   
   IF dz_topo.temp_table_exists('tmp_dz_topogeo_ids')
   THEN
      TRUNCATE TABLE tmp_dz_topogeo_ids;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_topogeo_ids(
          topogeo_id     INTEGER 
         ,face_ids       INTEGER[]
         ,face_count     INTEGER
      );

      CREATE UNIQUE INDEX tmp_dz_topogeo_ids_pk
      ON tmp_dz_topogeo_ids(topogeo_id);
      
      CREATE INDEX tmp_dz_topogeo_ids_gin
      ON tmp_dz_topogeo_ids USING GIN(face_ids);

   END IF;
   
   IF dz_topo.temp_table_exists('tmp_dz_topogeo_rez')
   THEN
      TRUNCATE TABLE tmp_dz_topogeo_rez;
      
   ELSE
      CREATE TEMPORARY TABLE tmp_dz_topogeo_rez(
          topogeo_dups INTEGER[]
         ,areasqkm     NUMERIC
         ,shape        GEOMETRY
      );

   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Get the list of overlapped faces to narrow things down
   ----------------------------------------------------------------------------
   str_sql := '
      INSERT INTO tmp_dz_overlapped_faces(
          face_id
         ,topogeo_count
      )
      SELECT
       b.element_id AS face_id
      ,COUNT(*)
      FROM
      ' || quote_ident(rec_topo.name) || '.relation b
      WHERE
      b.element_type = 3';
            
   IF p_envelope IS NOT NULL
   THEN
      str_sql := str_sql || 'AND b.topogeo_id IN (
         SELECT
         bb.topogeo_id
         FROM
         ' || quote_ident(rec_topo.name) || '.face aa
         JOIN
         ' || quote_ident(rec_topo.name) || '.relation bb
         ON
         aa.face_id = bb.element_id
         WHERE
         ST_INTERSECTS(aa.mbr,$1)
      ) ';
   
   END IF;  

   str_sql := str_sql || ' 
      GROUP BY
      b.element_id
      HAVING COUNT(*) > 1';
      
   IF p_envelope IS NOT NULL
   THEN
      EXECUTE str_sql USING p_envelope;
      
   ELSE
      EXECUTE str_sql;
   
   END IF;
   
   GET DIAGNOSTICS int_count = ROW_COUNT; 
   
   IF int_count > 50000
   THEN
      ANALYZE tmp_dz_overlapped_faces;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 30
   -- Get the list of topogeo ids and component face ids for comparison
   ----------------------------------------------------------------------------
   str_sql := '
      INSERT INTO tmp_dz_topogeo_ids(
          topogeo_id
         ,face_ids
         ,face_count
      )
      SELECT
       b.topogeo_id
      ,dz_topo.array_compress(ARRAY_AGG(a.face_id)) AS face_ids
      ,COUNT(*) AS face_count
      FROM 
      tmp_dz_overlapped_faces a
      JOIN
      ' || quote_ident(rec_topo.name) || '.relation b
      ON
      a.face_id = b.element_id
      GROUP BY
      b.topogeo_id';
      
   EXECUTE str_sql;

   GET DIAGNOSTICS int_count = ROW_COUNT;
   
   IF int_count > 50000
   THEN
      ANALYZE tmp_dz_topogeo_ids;
      
   END IF;
   
   ----------------------------------------------------------------------------
   -- Step 40
   -- Search for topogeo_ids with the same set of face ids
   ---------------------------------------------------------------------------- 
   ary_topogeo_chk := '{}';
   
   FOR rec IN (
      SELECT
       a.topogeo_id
      ,a.face_ids
      ,a.face_count
      FROM
      tmp_dz_topogeo_ids a
   ) 
   LOOP
      IF rec.topogeo_id != ALL(ary_topogeo_chk)
      THEN
         SELECT
         ARRAY_AGG(b.topogeo_id)
         INTO
         ary_topogeo_ids
         FROM
         tmp_dz_topogeo_ids b
         WHERE
             b.topogeo_id != rec.topogeo_id
         AND b.face_count =  rec.face_count
         AND b.face_ids   =  rec.face_ids;
         
         IF ary_topogeo_ids IS NOT NULL
         AND ARRAY_LENGTH(ary_topogeo_ids,1) > 0
         THEN
            INSERT INTO tmp_dz_topogeo_rez(
               topogeo_dups
            ) VALUES (
               ARRAY_APPEND(ary_topogeo_ids,rec.topogeo_id)
            );
            
            ary_topogeo_chk := ary_topogeo_chk || ary_topogeo_ids;
         
         END IF;
         
         ary_topogeo_chk := array_append(ary_topogeo_chk,rec.topogeo_id);
         
      END IF;
      
   END LOOP;
   
   ----------------------------------------------------------------------------
   -- Step 50
   -- Add areasqkm and shape information to the duplicates for QA purposes
   ---------------------------------------------------------------------------- 
   EXECUTE '
   UPDATE tmp_dz_topogeo_rez a
   SET
   shape = (
      SELECT
      b.' || quote_ident(p_layer_column) || '::GEOMETRY
      FROM
      ' || quote_ident(p_layer_schema) || '.' || quote_ident(p_layer_table) || ' b
      WHERE
      (b.' || quote_ident(p_layer_column) || ').id = a.topogeo_dups[1]
   )';
   
   UPDATE tmp_dz_topogeo_rez a
   SET areasqkm = ST_AREA(ST_TRANSFORM(a.shape::GEOMETRY,4326))::NUMERIC / 1000000;
   
   ----------------------------------------------------------------------------
   -- Step 60
   -- Return results
   ---------------------------------------------------------------------------- 
   RETURN QUERY 
   SELECT
    CAST(ROW_NUMBER() OVER () AS INTEGER) AS objectid
   ,a.topogeo_dups
   ,a.areasqkm
   ,a.shape
   FROM 
   tmp_dz_topogeo_rez a;
           
END;
$BODY$
LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION dz_topo.find_topogeo_duplicates(
    VARCHAR
   ,VARCHAR
   ,VARCHAR
   ,GEOMETRY
   ,INTEGER
) TO PUBLIC;
