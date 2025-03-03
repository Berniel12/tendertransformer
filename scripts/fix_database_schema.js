/**
 * Simple script to update the database schema directly at Actor startup
 * This can be called from the Actor's main.js before processing starts
 */

const { Pool } = require('pg');

// Define the columns we need
const requiredColumns = [
    { name: 'quality_score', type: 'INTEGER' },
    { name: 'quality_validated', type: 'BOOLEAN', default: 'FALSE' },
    { name: 'has_validation_issues', type: 'BOOLEAN', default: 'FALSE' }
];

async function ensureSchemaUpdated() {
    console.log('Checking database schema for quality validation fields...');
    
    // Get connection details from environment
    const connectionString = process.env.DATABASE_URL || process.env.APIFY_DATABASE_URL;
    
    if (!connectionString) {
        console.error('No database connection string found in environment variables');
        return false;
    }
    
    const pool = new Pool({ connectionString });
    let client;
    
    try {
        client = await pool.connect();
        
        // Check which columns already exist
        const existingColumnsResult = await client.query(`
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'unified_tenders'
            AND column_name IN ('quality_score', 'quality_validated', 'has_validation_issues');
        `);
        
        const existingColumns = new Set(existingColumnsResult.rows.map(row => row.column_name));
        console.log(`Found ${existingColumns.size} of 3 expected quality validation columns`);
        
        if (existingColumns.size === 3) {
            console.log('Schema is already updated with all required columns');
            return true;
        }
        
        // Add missing columns
        for (const column of requiredColumns) {
            if (!existingColumns.has(column.name)) {
                console.log(`Adding missing column: ${column.name}`);
                let defaultClause = column.default ? ` DEFAULT ${column.default}` : '';
                
                await client.query(`
                    ALTER TABLE unified_tenders
                    ADD COLUMN ${column.name} ${column.type}${defaultClause};
                `);
                console.log(`Added ${column.name} column successfully`);
            }
        }
        
        console.log('Schema update completed successfully');
        return true;
        
    } catch (error) {
        console.error('Error updating database schema:', error);
        return false;
    } finally {
        if (client) client.release();
        await pool.end();
    }
}

// Allow direct execution
if (require.main === module) {
    ensureSchemaUpdated()
        .then(success => {
            console.log(`Schema update ${success ? 'completed successfully' : 'failed'}`);
            process.exit(success ? 0 : 1);
        })
        .catch(error => {
            console.error('Uncaught error during schema update:', error);
            process.exit(1);
        });
}

module.exports = { ensureSchemaUpdated }; 