-module(sql).
-compile(export_all).

systems() ->
    "SELECT
      o.asset_type,
      regexp_replace(o.name, E'&', 'And', 'g') system_name,
      MAX(CASE WHEN att_t.name = 'rationalisationCategory' THEN att.attributevalue ELSE NULL END) rationalisation_category,
      MAX(CASE WHEN att_t.name = 'aliases' THEN att.attributevalue ELSE NULL END) aliases,
      MAX(CASE WHEN att_t.name = 'salsaId' THEN att.attributevalue ELSE NULL END) salsa_id,
      MAX(CASE WHEN att_t.name = 'activeStatus'
          THEN
          CASE WHEN att.attributevalue = 'true'
            THEN 1
            ELSE 0
          END
          ELSE NULL
      END) active_status
    FROM btber.typed_assets o
      INNER JOIN btber.attribute att ON o.key = att.asset_key
      INNER JOIN btber.attributetype att_t ON att.type_key = att_t.key
    WHERE o.asset_type = 'System'
    GROUP BY o.asset_type, o.name
    ".

assoc_small() ->
    "
    WITH platform AS (
    SELECT 
        o.asset_type,
        o.key,
        -1 as parent,
        regexp_replace(o.name, E'&', 'And', 'g') as name,
        ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
        (o.key)::text as path,
        1 as depth,
        1 as lvl,
        o.key as root,
        ('platform')::varchar as context,
        ('roots')::varchar as node
    FROM btber.typed_assets o
    WHERE o.key = 28889753
    ), services AS (
    SELECT 
        o.asset_type,
        o.key,
        platform.key as parent,
        regexp_replace(o.name, E'&', 'And', 'g') as name,
        ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
        (platform.path || '/' || o.key)::text as path,
        2 as depth,
        2 as lvl,
        platform.root as root,
        (a.association_name)::varchar as context,
        ('services')::varchar as node
    FROM btber.typed_assets o
    	INNER JOIN btber.assoc a ON o.key = a.consumer_key
    	INNER JOIN platform ON a.provider_key = platform.key
    WHERE o.asset_type = 'Service' AND a.association_name = 'platform-service'
    	AND o.name LIKE '%Manage%'
    ), interfaces AS (
    SELECT 
        o.asset_type,
        o.key,
        regexp_replace(o.name, E'&', 'And', 'g') as name,
        ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
        (services.path || '/' || o.key)::text as path,
        3 as depth,
        3 as lvl,
        services.root as root,
        (a.association_name)::varchar as context,
        ('interfaces')::varchar as node
    FROM btber.typed_assets o
    	INNER JOIN btber.assoc a ON o.key = a.consumer_key
    	INNER JOIN services ON a.provider_key = services.key
    WHERE o.asset_type = 'Interface' AND a.association_name = 'service-interface'
    ), unified AS (
    SELECT * FROM platform
    	UNION ALL
    SELECT * FROM services
    	UNION ALL
    SELECT * FROM interfaces
    )

    SELECT *
    FROM unified
    ORDER BY root, path, depth
    ".

assoc() ->
    "WITH platform AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (o.key)::text as path,
    	    1 as depth,
    	    1 as lvl,
    	    o.key as root,
    	    ('platform')::varchar as context,
    	    ('')::varchar as node
    	FROM btber.typed_assets o
    	WHERE o.key = 28889753
    ), services AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (platform.path || '/' || o.key)::text as path,
    	    2 as depth,
    	    2 as lvl,
    	    platform.root as root,
    	    (a.association_name)::varchar as context,
    	    ('services')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN platform ON a.provider_key = platform.key
    	WHERE o.asset_type = 'Service' AND a.association_name = 'platform-service'
    ), interfaces AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (services.path || '/' || o.key)::text as path,
    	    3 as depth,
    	    3 as lvl,
    	    services.root as root,
    	    (a.association_name)::varchar as context,
    	    ('interfaces')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN services ON a.provider_key = services.key
    	WHERE o.asset_type = 'Interface' AND a.association_name = 'service-interface'
    ), implementations AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (interfaces.path || '/' || o.key)::text as path,
    	    4 as depth,
    	    4 as lvl,
    	    interfaces.root as root,
    	    (a.association_name)::varchar as context,
    	    ('implementations')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN interfaces ON a.provider_key = interfaces.key
    	WHERE o.asset_type = 'Implementation' AND a.association_name = 'interface-implementation'
    ), exposures AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (implementations.path || '/' || o.key)::text as path,
    	    5 as depth,
    	    5 as lvl,
    	    implementations.root as root,
    	    (a.association_name)::varchar as context,
    	    ('exposures')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN implementations ON a.provider_key = implementations.key
    	WHERE o.asset_type = 'Exposure' AND a.association_name = 'implementation-exposure'
    ), systems AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (exposures.path || '/' || o.key)::text as path,
    	    6 as depth,
    	    6 as lvl,
    	    exposures.root as root,
    	    (a.association_name)::varchar as context,
    	    ('exposedSystems')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN exposures ON a.provider_key = exposures.key
    	WHERE o.asset_type = 'System' AND a.association_name = 'exposure-system'
    ), consuming_systems AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (exposures.path || '/' || o.key)::text as path,
    	    7 as depth,
    	    6 as lvl,
    	    exposures.root as root,
    	    (a.association_name)::varchar as context,
    	    ('consumingSystems')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.consumer_key
    		INNER JOIN exposures ON a.provider_key = exposures.key
    	WHERE o.asset_type = 'System' AND a.association_name = 'systems-consuming-exposures'
    ), owning_platforms AS (
    	SELECT 
    	    o.asset_type,
    	    o.key,
    	    regexp_replace(o.name, E'&', 'And', 'g') as name,
    	    ('v' || o.major_vsn || '.' || o.minor_vsn || '.' || o.revision_vsn)::varchar as version,
    	    (consuming_systems.path || '/' || o.key)::text as path,
    	    8 as depth,
    	    7 as lvl,
    	    consuming_systems.root as root,
    	    ('!' || a.association_name)::varchar as context,
    	    ('owningPlatform')::varchar as node
    	FROM btber.typed_assets o
    		INNER JOIN btber.assoc a ON o.key = a.provider_key
    		INNER JOIN consuming_systems ON a.consumer_key = consuming_systems.key
    	WHERE o.asset_type = 'Platform' AND a.association_name = 'platform-system'
    ), unified AS (
    	SELECT * FROM platform
    		UNION ALL
    	SELECT * FROM services
    		UNION ALL
    	SELECT * FROM interfaces
    		UNION ALL
    	SELECT * FROM implementations
    		UNION ALL
    	SELECT * FROM exposures
    		UNION ALL
    	SELECT * FROM systems
    		UNION ALL
    	SELECT * FROM consuming_systems
    		UNION ALL
    	SELECT * FROM owning_platforms
    )

    SELECT *
    FROM unified
    ORDER BY root, path, depth
    ".

