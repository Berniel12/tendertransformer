/**
 * apify-main.js
 * Apify-specific entry point for the tender processing application
 */

const { Actor, log } = require('apify');
const sourceRegistry = require('./services/sourceRegistry');
const { processTendersFromSource, processAllSources } = require('./index');

// Initialize Apify
Actor.main(async () => {
    log.info('Starting Tender Processing Actor');
    
    try {
        // Get input from the actor
        const input = await Actor.getInput();
        log.info('Actor input:', input);
        
        // Register source adapters
        sourceRegistry.registerDefaults();
        
        // Always process all sources regardless of input
        log.info('Processing all sources');
        const result = await processAllSources();
        
        // Save the result to the default dataset
        await Actor.pushData({
            success: result.success,
            message: result.success ? 'Processing completed successfully' : 'Processing encountered errors',
            stats: {
                processed: result.totalProcessed || result.processed || 0,
                skipped: result.totalSkipped || result.skipped || 0,
                errors: result.totalErrors || result.errors || 0
            },
            timestamp: new Date().toISOString(),
            details: result
        });
        
        log.info('Actor finished successfully');
    } catch (error) {
        log.error('Actor failed:', error);
        throw error;
    }
});