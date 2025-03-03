/**
 * index.js
 * Main entry point for the tender processing application
 */

// Load environment variables
require('dotenv').config();

// Import required libraries and services
const { createClient } = require('@supabase/supabase-js');
const sourceRegistry = require('./services/sourceRegistry');
const {
    processTendersFromTable,
    processTendersFromAllSources,
    processNewestTendersFromAllSources,
    processAllUnprocessedTenders
} = require('./services/processingService');

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
    console.error('Error: SUPABASE_URL and SUPABASE_KEY must be provided');
    process.exit(1);
}

const supabaseAdmin = createClient(supabaseUrl, supabaseServiceKey, {
    auth: {
        autoRefreshToken: false,
        persistSession: false
    }
});

/**
 * Process tenders from a specific source
 * @param {string} sourceName - Name of the source to process
 * @returns {Promise<Object>} Processing results
 */
async function processTendersFromSource(sourceName) {
    console.log(`Processing tenders from source: ${sourceName}`);
    
    // Validate that the source exists and has a registered adapter
    if (!sourceRegistry.hasAdapter(sourceName)) {
        console.error(`Error: No adapter registered for source ${sourceName}`);
        return { success: false, error: 'Source not supported' };
    }
    
    try {
        // Process the tenders using the processing service
        const results = await processTendersFromTable(supabaseAdmin, sourceName);
        
        // Log the results
        if (results.success) {
            console.log(`Successfully processed ${results.processed} tenders from ${sourceName}`);
            console.log(`Skipped ${results.skipped} tenders`);
            console.log(`Encountered ${results.errors} errors`);
            if (results.fallback) {
                console.log(`Used fallback normalization for ${results.fallback} tenders`);
            }
        } else {
            console.error(`Failed to process tenders from ${sourceName}:`, results.error);
        }
        
        return results;
    } catch (error) {
        console.error(`Error processing tenders from ${sourceName}:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Process tenders from all registered sources sequentially
 * @returns {Promise<Object>} Processing results
 */
async function processAllSources() {
    console.log('Processing tenders from all sources sequentially');
    
    const sources = sourceRegistry.getRegisteredSources();
    console.log(`Found ${sources.length} registered sources:`, sources);
    
    const results = {
        success: true,
        sources: {},
        totalProcessed: 0,
        totalSkipped: 0,
        totalErrors: 0,
        totalFallback: 0
    };
    
    for (const source of sources) {
        console.log(`\n=== Processing source: ${source} ===\n`);
        
        const sourceResult = await processTendersFromSource(source);
        results.sources[source] = sourceResult;
        
        if (sourceResult.success) {
            results.totalProcessed += sourceResult.processed || 0;
            results.totalSkipped += sourceResult.skipped || 0;
            results.totalErrors += sourceResult.errors || 0;
            results.totalFallback += sourceResult.fallback || 0;
        } else {
            results.success = false;
        }
    }
    
    console.log('\n=== Processing Summary ===');
    console.log(`Total tenders processed: ${results.totalProcessed}`);
    console.log(`Total tenders skipped: ${results.totalSkipped}`);
    console.log(`Total errors encountered: ${results.totalErrors}`);
    console.log(`Total fallback normalizations: ${results.totalFallback}`);
    
    return results;
}

/**
 * Process tenders from all sources in round-robin fashion
 * This is more efficient as it distributes processing across all sources
 * @returns {Promise<Object>} Processing results
 */
async function processAllSourcesRoundRobin() {
    console.log('Processing tenders from all sources in round-robin mode');
    
    try {
        // Process tenders using the round-robin processing service
        const results = await processTendersFromAllSources(supabaseAdmin);
        
        return results;
    } catch (error) {
        console.error(`Error processing tenders in round-robin mode:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Run continuous processing in a loop
 * This will repeatedly process tenders from all sources in a round-robin fashion
 * @returns {Promise<void>}
 */
async function runContinuousProcessingWrapper() {
    console.log('Starting continuous processing mode');
    
    try {
        // Use the enhanced continuous processing from the processing service
        const options = {
            tendersPerSource: parseInt(process.env.TENDERS_PER_SOURCE || '1000', 10),
            waitMinutes: parseInt(process.env.PROCESSING_INTERVAL_MINUTES || '5', 10),
            continuous: true
        };
        
        await processNewestTendersFromAllSources(supabaseAdmin, options);
    } catch (error) {
        console.error('Error in continuous processing:', error);
        throw error;
    }
}

/**
 * Process all unprocessed tenders from all sources with pagination
 * @returns {Promise<void>}
 */
async function processAllUnprocessed() {
    console.log('Processing ALL unprocessed tenders from all sources using pagination');
    
    try {
        const results = await processAllUnprocessedTenders(supabaseAdmin);
        
        console.log('\n=== Final Processing Summary ===');
        console.log(`Total tenders processed: ${results.processed}`);
        console.log(`Total tenders skipped: ${results.skipped}`);
        console.log(`Total tenders updated: ${results.updated}`);
        console.log(`Total errors encountered: ${results.errors}`);
        console.log(`Total fallback normalizations: ${results.fallback}`);
        console.log(`Total fast normalizations: ${results.fastNormalization}`);
    } catch (error) {
        console.error(`Error processing all unprocessed tenders:`, error);
    }
}

/**
 * Main function to run the application
 */
async function main() {
    console.log('Starting tender processing application');
    
    try {
        // Register default adapters
        sourceRegistry.registerDefaults();
        
        // Get command line arguments
        const args = process.argv.slice(2);
        const command = args[0];
        const sourceName = args[1];
        
        if (command === 'process' && sourceName) {
            // Process tenders from a specific source
            await processTendersFromSource(sourceName);
        } else if (command === 'process-all') {
            // Process tenders from all sources sequentially
            await processAllSources();
        } else if (command === 'process-round-robin') {
            // Process tenders from all sources in round-robin fashion
            await processAllSourcesRoundRobin();
        } else if (command === 'process-all-unprocessed') {
            // Process ALL unprocessed tenders from all sources using pagination
            await processAllUnprocessed();
        } else if (command === 'help' || command === '--help' || command === '-h') {
            // Show usage instructions
            showUsage();
        } else {
            // Default behavior: run continuous processing
            await runContinuousProcessingWrapper();
        }
    } catch (error) {
        console.error('Error running tender processing application:', error);
        process.exit(1);
    }
}

/**
 * Display usage instructions
 */
function showUsage() {
    console.log('Usage: node src/index.js [command] [source]');
    console.log('\nCommands:');
    console.log('  process <source>            - Process tenders from a specific source');
    console.log('  process-all                 - Process tenders from all sources sequentially');
    console.log('  process-round-robin         - Process tenders from all sources in round-robin fashion');
    console.log('  process-all-unprocessed     - Process ALL unprocessed tenders from all sources (uses pagination)');
    console.log('  help                        - Show this help message');
    console.log('\nSources:');
    console.log('  ' + sourceRegistry.getRegisteredSources().join(', '));
    console.log('\nEnvironment Variables:');
    console.log('  SUPABASE_URL                - Supabase project URL');
    console.log('  SUPABASE_SERVICE_KEY        - Supabase service role key');
    console.log('  OPENAI_API_KEY              - OpenAI API key');
    console.log('  TENDERS_PER_SOURCE          - Number of tenders to process from each source per round (default: 1000)');
    console.log('  PROCESSING_INTERVAL_MINUTES - Minutes to wait between processing rounds (default: 5)');
    
    process.exit(0);
}

// Run the application if this file is executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('Fatal error:', error);
        process.exit(1);
    });
}

// Export functions for testing or programmatic use
module.exports = {
    processTendersFromSource,
    processAllSources,
    processAllSourcesRoundRobin,
    runContinuousProcessingWrapper
};