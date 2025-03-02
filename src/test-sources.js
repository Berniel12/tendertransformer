/**
 * test-sources.js
 * Script to check the status of all source tables
 */
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const sourceRegistry = require('./services/sourceRegistry');

async function checkSourceTables() {
  try {
    const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
    const sources = sourceRegistry.getRegisteredSources();
    
    console.log('=== SOURCE TABLE STATUS ===');
    console.log(`Found ${sources.length} registered sources: ${sources.join(', ')}`);
    
    for (const source of sources) {
      try {
        const { data, error, count } = await supabase
          .from(source)
          .select('*', { count: 'exact', head: true });
          
        if (error) {
          console.log(`${source}: ERROR - ${error.message}`);
          continue;
        }
        
        console.log(`\n${source}: ${count} records available`);
        
        // Check processed status if the table has last_processed_at
        const { data: metadata } = await supabase
          .from('information_schema.columns')
          .select('column_name')
          .eq('table_name', source)
          .in('column_name', ['last_processed_at']);
          
        const hasProcessedField = metadata && metadata.length > 0;
        
        if (hasProcessedField) {
          const { data: unprocessed, error: upError, count: upCount } = await supabase
            .from(source)
            .select('*', { count: 'exact', head: true })
            .is('last_processed_at', null);
            
          if (!upError) {
            console.log(`  - Unprocessed records: ${upCount}`);
          }
        } else {
          console.log(`  - No last_processed_at field (all records will be processed)`);
        }
        
        // Get sample data
        const { data: sample } = await supabase
          .from(source)
          .select('*')
          .limit(1);
          
        if (sample && sample.length > 0) {
          console.log(`  - Sample data available (first record ID: ${sample[0].id})`);
        } else {
          console.log(`  - No sample data available`);
        }
      } catch (err) {
        console.error(`Error checking source table ${source}:`, err);
      }
    }
  } catch (err) {
    console.error('Error checking source tables:', err);
  }
}

// Run the check
console.log('Starting source table check...');
checkSourceTables(); 