-- ============================================================================
-- SQL Migration Script: Style Management Tables
-- ============================================================================
-- This script creates tables for the metadata-driven styling system.
-- Run this against your PostGIS database before using the styles API.
-- ============================================================================

-- Create extension for UUID if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Table: style_metadata
-- Stores configuration and metadata for layer styles
-- ============================================================================
CREATE TABLE IF NOT EXISTS style_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Layer identification
    layer_name VARCHAR(255) NOT NULL,
    workspace VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) DEFAULT 'public',
    
    -- Style configuration
    style_column VARCHAR(255) NOT NULL,
    style_name VARCHAR(255) NOT NULL UNIQUE,
    geometry_type VARCHAR(50) NOT NULL CHECK (geometry_type IN ('polygon', 'line', 'point', 'Polygon', 'Line', 'Point', 'MultiPolygon', 'MultiLineString', 'MultiPoint')),
    
    -- Classification settings
    classification_method VARCHAR(50) NOT NULL CHECK (classification_method IN ('equal_interval', 'quantile', 'jenks', 'categorical')),
    num_classes INTEGER DEFAULT 5,
    color_palette VARCHAR(100) DEFAULT 'YlOrRd',
    
    -- Style properties
    fill_opacity FLOAT DEFAULT 0.7,
    stroke_color VARCHAR(20) DEFAULT '#333333',
    stroke_width FLOAT DEFAULT 1.0,
    stroke_opacity FLOAT DEFAULT 1.0,
    
    -- Computed data (stored for legend generation)
    class_breaks JSONB,  -- Array of break values for numeric
    class_labels JSONB,  -- Array of labels for each class
    class_colors JSONB,  -- Array of colors used
    
    -- Full MBStyle JSON (cached)
    mbstyle_json JSONB,
    
    -- GeoServer status
    published_to_geoserver BOOLEAN DEFAULT FALSE,
    geoserver_style_name VARCHAR(255),
    is_default_style BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- User tracking (optional)
    created_by VARCHAR(255),
    
    -- Constraints
    CONSTRAINT unique_layer_style UNIQUE (workspace, layer_name, style_column)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_style_metadata_layer ON style_metadata(workspace, layer_name);
CREATE INDEX IF NOT EXISTS idx_style_metadata_style_name ON style_metadata(style_name);

-- ============================================================================
-- Table: style_audit_log
-- Tracks changes to styles for audit purposes
-- ============================================================================
CREATE TABLE IF NOT EXISTS style_audit_log (
    id SERIAL PRIMARY KEY,
    style_id UUID REFERENCES style_metadata(id) ON DELETE CASCADE,
    action VARCHAR(50) NOT NULL CHECK (action IN ('created', 'updated', 'deleted', 'published', 'unpublished', 'regenerated')),
    action_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Change details
    previous_value JSONB,  -- Previous state (for updates)
    new_value JSONB,       -- New state
    changed_fields TEXT[], -- List of changed fields
    
    -- User tracking
    performed_by VARCHAR(255),
    ip_address VARCHAR(45),
    user_agent TEXT
);

-- Create index for faster audit queries
CREATE INDEX IF NOT EXISTS idx_style_audit_log_style_id ON style_audit_log(style_id);
CREATE INDEX IF NOT EXISTS idx_style_audit_log_timestamp ON style_audit_log(action_timestamp);

-- ============================================================================
-- Table: style_cache
-- Caches expensive query results (e.g., Jenks breaks, statistics)
-- ============================================================================
CREATE TABLE IF NOT EXISTS style_cache (
    id SERIAL PRIMARY KEY,
    
    -- Cache key components
    cache_key VARCHAR(512) NOT NULL UNIQUE,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) DEFAULT 'public',
    cache_type VARCHAR(50) NOT NULL CHECK (cache_type IN ('statistics', 'quantile_breaks', 'jenks_breaks', 'distinct_values')),
    
    -- Cached data
    cached_data JSONB NOT NULL,
    row_count INTEGER,  -- Number of rows at cache time
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    
    -- Validity
    is_valid BOOLEAN DEFAULT TRUE
);

-- Create indexes for cache lookups
CREATE INDEX IF NOT EXISTS idx_style_cache_key ON style_cache(cache_key);
CREATE INDEX IF NOT EXISTS idx_style_cache_table ON style_cache(schema_name, table_name, column_name);
CREATE INDEX IF NOT EXISTS idx_style_cache_expiry ON style_cache(expires_at) WHERE is_valid = TRUE;

-- ============================================================================
-- Function: Update timestamp trigger
-- ============================================================================
CREATE OR REPLACE FUNCTION update_style_metadata_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for automatic timestamp update
DROP TRIGGER IF EXISTS trigger_update_style_metadata_timestamp ON style_metadata;
CREATE TRIGGER trigger_update_style_metadata_timestamp
    BEFORE UPDATE ON style_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_style_metadata_timestamp();

-- ============================================================================
-- Function: Invalidate cache when source table changes
-- (Optional - call this when you know data has changed)
-- ============================================================================
CREATE OR REPLACE FUNCTION invalidate_style_cache(
    p_table_name VARCHAR,
    p_schema_name VARCHAR DEFAULT 'public'
)
RETURNS INTEGER AS $$
DECLARE
    invalidated_count INTEGER;
BEGIN
    UPDATE style_cache 
    SET is_valid = FALSE 
    WHERE table_name = p_table_name 
    AND schema_name = p_schema_name 
    AND is_valid = TRUE;
    
    GET DIAGNOSTICS invalidated_count = ROW_COUNT;
    RETURN invalidated_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Function: Clean expired cache entries
-- ============================================================================
CREATE OR REPLACE FUNCTION clean_expired_style_cache()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM style_cache 
    WHERE expires_at < CURRENT_TIMESTAMP 
    OR is_valid = FALSE;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Sample usage comments
-- ============================================================================
/*
-- Generate a style for a layer:
POST /styles/generate
{
    "layer_name": "health_cases",
    "workspace": "my_workspace",
    "table_name": "health_cases",
    "schema_name": "public",
    "style_column": "cases_count",
    "geometry_type": "polygon",
    "classification_method": "quantile",
    "num_classes": 5,
    "color_palette": "YlOrRd",
    "publish_to_geoserver": true,
    "set_as_default": true
}

-- Preview style without saving:
POST /styles/preview
{
    "table_name": "health_cases",
    "schema_name": "public",
    "style_column": "cases_count",
    "geometry_type": "polygon",
    "classification_method": "equal_interval",
    "num_classes": 7,
    "color_palette": "Blues"
}

-- Get legend for a style:
GET /styles/legend/{style_id}

-- List available palettes:
GET /styles/palettes

-- Regenerate style (recompute from data):
POST /styles/regenerate/{style_id}
*/

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL ON style_metadata TO your_app_user;
-- GRANT ALL ON style_audit_log TO your_app_user;
-- GRANT ALL ON style_cache TO your_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_app_user;

COMMENT ON TABLE style_metadata IS 'Stores style configurations for GeoServer layers';
COMMENT ON TABLE style_audit_log IS 'Audit trail for style changes';
COMMENT ON TABLE style_cache IS 'Cache for expensive classification computations';
