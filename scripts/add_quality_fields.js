/**
 * Script to add quality validation fields to the unified_tenders table
 * 
 * Run with: node scripts/add_quality_fields.js
 */

const { Client } = require('pg');
require('dotenv').config();

async function main() {
    const client = new Client({
        connectionString: process.env.DATABASE_URL,
    });

    try {
        await client.connect();
        console.log('Connected to database');

        // Check if the fields already exist
        const checkQuery = `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'unified_tenders'
            AND column_name IN ('quality_score', 'quality_validated', 'has_validation_issues');
        `;
        
        const existingColumns = await client.query(checkQuery);
        const columnSet = new Set(existingColumns.rows.map(row => row.column_name));
        
        if (columnSet.size === 3) {
            console.log('All quality validation fields already exist in the table');
            return;
        }
        
        console.log('Adding missing quality validation fields to unified_tenders table...');
        
        // Add each field if it doesn't exist
        if (!columnSet.has('quality_score')) {
            console.log('Adding quality_score column...');
            await client.query(`
                ALTER TABLE unified_tenders
                ADD COLUMN quality_score INTEGER;
            `);
        }
        
        if (!columnSet.has('quality_validated')) {
            console.log('Adding quality_validated column...');
            await client.query(`
                ALTER TABLE unified_tenders
                ADD COLUMN quality_validated BOOLEAN DEFAULT FALSE;
            `);
        }
        
        if (!columnSet.has('has_validation_issues')) {
            console.log('Adding has_validation_issues column...');
            await client.query(`
                ALTER TABLE unified_tenders
                ADD COLUMN has_validation_issues BOOLEAN DEFAULT FALSE;
            `);
        }
        
        console.log('Successfully added quality validation fields');

    } catch (error) {
        console.error('Error adding quality validation fields:', error);
    } finally {
        await client.end();
        console.log('Database connection closed');
    }
}

main().catch(console.error); 