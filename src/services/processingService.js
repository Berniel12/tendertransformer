/**
 * processingService.js
 * Service for processing tenders from various sources
 */

const sourceRegistry = require('./sourceRegistry');
const { normalizeTender, evaluateNormalizationNeeds } = require('./tenderNormalizer');

// Performance tracking variables
const performanceStats = {
    totalProcessed: 0,
    llmUsed: 0,
    fastNormalization: 0,
    fallbackNormalization: 0,
    startTime: Date.now(),
    processingTimes: [],
    bySource: {}
};

/**
 * Extract numeric value from a currency string
 * @param {string|number} value - The currency value to parse
 * @returns {number|null} The numeric value or null if invalid
 */
function extractNumericValue(value) {
    if (value === null || value === undefined) return null;
    
    try {
        // If already a number, return it
        if (typeof value === 'number') return value;
        
        // Convert to string and clean it up
        let cleanValue = value.toString()
            // Remove currency codes (e.g., USD, EUR, LKR)
            .replace(/[A-Z]{3}\s*/g, '')
            // Remove currency symbols
            .replace(/[$€£¥]/g, '')
            // Remove commas and spaces
            .replace(/[,\s]/g, '')
            // Trim whitespace
            .trim();
            
        // Parse the cleaned value
        const numericValue = parseFloat(cleanValue);
        
        // Ensure we have a valid number
        if (isNaN(numericValue)) {
            console.warn(`Could not parse numeric value from: ${value}`);
            return null;
        }
        
        return numericValue;
    } catch (error) {
        console.warn(`Failed to extract numeric value from: ${value}`, error);
        return null;
    }
}

/**
 * Process tenders from all sources in a round-robin fashion
 * @param {Object} supabaseAdmin - Supabase admin client
 * @param {Object} options - Processing options
 * @param {number} options.tendersPerSource - Number of tenders to process from each source in one round
 * @param {boolean} options.continuous - Whether to run in continuous mode
 * @returns {Promise<Object>} Processing results
 */
async function processTendersFromAllSources(supabaseAdmin, options = {}) {
    const { tendersPerSource = 20, continuous = false } = options;
    
    console.log(`Starting to process tenders from all sources in round-robin mode${continuous ? ' (continuous)' : ''}`);
    
    // Get all available source adapters
    const sources = sourceRegistry.getRegisteredSources();
    if (!sources || sources.length === 0) {
        console.error('No source adapters available');
        return { 
            success: false, 
            error: 'No source adapters registered' 
        };
    }
    
    console.log(`Found ${sources.length} sources: ${sources.join(', ')}`);
    
    // Define processing parameters
    const totalResults = {
        success: true,
        processed: 0,
        skipped: 0,
        updated: 0,
        errors: 0,
        fallback: 0,
        fastNormalization: 0,
        bySource: {}
    };
    
    // Initialize source results
    for (const sourceTable of sources) {
        totalResults.bySource[sourceTable] = {
            processed: 0,
            skipped: 0,
            updated: 0,
            errors: 0,
            fallback: 0,
            fastNormalization: 0
        };
    }
    
    // Process sources in smaller batches to ensure fair distribution
    const batchSize = Math.min(5, Math.ceil(tendersPerSource / 4)); // Process 1/4 of tendersPerSource at a time
    let remainingTenders = tendersPerSource;
    
    while (remainingTenders > 0) {
        const currentBatchSize = Math.min(batchSize, remainingTenders);
        console.log(`\nProcessing batch of ${currentBatchSize} tenders from each source (${remainingTenders} remaining per source)`);
        
        // Process each source
        for (const sourceTable of sources) {
            console.log(`\nProcessing ${sourceTable}...`);
            
            const result = await processTendersFromTable(supabaseAdmin, sourceTable, currentBatchSize);
            
            // Aggregate results
            totalResults.processed += result.processed;
            totalResults.skipped += result.skipped;
            totalResults.updated += result.updated || 0;
            totalResults.errors += result.errors;
            totalResults.fallback += result.fallback || 0;
            totalResults.fastNormalization += result.fastNormalization || 0;
            
            // Update source-specific results
            totalResults.bySource[sourceTable].processed += result.processed;
            totalResults.bySource[sourceTable].skipped += result.skipped;
            totalResults.bySource[sourceTable].updated += result.updated || 0;
            totalResults.bySource[sourceTable].errors += result.errors;
            totalResults.bySource[sourceTable].fallback += result.fallback || 0;
            totalResults.bySource[sourceTable].fastNormalization += result.fastNormalization || 0;
            
            console.log(`Completed batch from ${sourceTable}:`);
            console.log(`- Processed: ${result.processed}`);
            console.log(`- Skipped: ${result.skipped}`);
            console.log(`- Updated: ${result.updated || 0}`);
            console.log(`- Errors: ${result.errors}`);
            console.log(`- Normalization: ${result.fastNormalization || 0} fast, ${result.fallback || 0} fallbacks`);
            
            // If in continuous mode and we've processed enough from this source, move to next
            if (continuous && result.processed + result.skipped === 0) {
                console.log(`No more tenders to process from ${sourceTable}, moving to next source`);
                continue;
            }
        }
        
        remainingTenders -= currentBatchSize;
    }
    
    // Log overall results
    console.log('\n=== PROCESSING SUMMARY ===');
    console.log(`Total tenders processed: ${totalResults.processed}`);
    console.log(`Total tenders skipped: ${totalResults.skipped}`);
    console.log(`Total tenders updated: ${totalResults.updated}`);
    console.log(`Total errors: ${totalResults.errors}`);
    console.log(`Normalization methods: ${totalResults.fastNormalization} fast, ${totalResults.fallback} fallbacks`);
    console.log('\nResults by source:');
    
    for (const [source, results] of Object.entries(totalResults.bySource)) {
        console.log(`\n${source}:`);
        console.log(`- Processed: ${results.processed}`);
        console.log(`- Skipped: ${results.skipped}`);
        console.log(`- Updated: ${results.updated}`);
        console.log(`- Errors: ${results.errors}`);
        console.log(`- Normalization: ${results.fastNormalization} fast, ${results.fallback} fallbacks`);
    }
    
    return totalResults;
}

/**
 * Process a limited number of tenders from a specific table
 * @param {Object} supabaseAdmin - Supabase admin client
 * @param {string} tableName - Name of the source table
 * @param {number} limit - Maximum number of successful normalizations to achieve
 * @param {boolean} forceReprocess - Whether to reprocess tenders that already exist in the database
 * @returns {Promise<Object>} Processing results
 */
async function processTendersFromTable(supabaseAdmin, tableName, limit = 100, forceReprocess = false) {
    console.log(`Starting to process until ${limit} successful normalizations from ${tableName}${forceReprocess ? ' (force reprocessing enabled)' : ''}`);
    
    // Get the appropriate adapter
    const adapter = sourceRegistry.getAdapter(tableName);
    if (!adapter) {
        console.error(`No adapter available for ${tableName}`);
        return { 
            success: false, 
            error: `No adapter registered for ${tableName}` 
        };
    }
    
    // Reset counters for this batch
    let processedCount = 0; // Successfully normalized tenders
    let skippedCount = 0;
    let errorCount = 0;
    let fallbackCount = 0; // Track number of fallback normalizations
    let fastNormalizationCount = 0; // Track number of fast normalizations
    let updatedCount = 0; // Track number of updated records
    let attemptCount = 0; // Track total attempts
    
    // Check if table has timestamp fields for incremental processing
    const tableInfo = await supabaseAdmin
        .from('information_schema.columns')
        .select('column_name')
        .eq('table_name', tableName)
        .in('column_name', ['created_at', 'updated_at', 'last_processed_at']);
    
    const hasTimestampFields = tableInfo.data && tableInfo.data.length > 0;
    const timestampFields = hasTimestampFields ? tableInfo.data.map(col => col.column_name) : [];
    console.log(`Found timestamp fields for table ${tableName}:`, timestampFields);
    
    // Define a chunk size for batch processing
    const chunkSize = 20;
    const concurrency = 5;
    
    // Process in chunks to avoid overwhelming the system
    const processChunk = async (offset) => {
        console.log(`Processing chunk from ${offset} to ${Math.min(offset + chunkSize, limit)} of ${limit} target normalizations`);
        
        // Get a batch of tenders that haven't been processed yet
        let queryBatch = supabaseAdmin.from(tableName)
            .select('*');
            
        // Always sort by created_at desc first to prioritize newer tenders
        if (timestampFields.includes('created_at')) {
            queryBatch = queryBatch.order('created_at', { ascending: false });
        } else if (timestampFields.includes('updated_at')) {
            // Fallback to updated_at if created_at is not available
            queryBatch = queryBatch.order('updated_at', { ascending: false });
        }
        
        // Then filter for unprocessed tenders
        if (timestampFields.includes('last_processed_at')) {
            queryBatch = queryBatch.is('last_processed_at', null);
        }
        
        // Finally add pagination
        queryBatch = queryBatch.range(offset, offset + chunkSize - 1);
        
        const { data: tenders, error: fetchError } = await queryBatch;
        
        if (fetchError) {
            console.error(`Error fetching tenders from ${tableName}:`, fetchError);
            return { success: false, error: fetchError.message };
        }
        
        if (!tenders || tenders.length === 0) {
            console.log(`No more unprocessed tenders found from ${offset}`);
            return { success: true, processed: 0, skipped: 0, errors: 0, fallback: 0 };
        }
        
        // Log the date range of tenders being processed
        if (tenders.length > 0) {
            const dateField = timestampFields.includes('created_at') ? 'created_at' : 'updated_at';
            if (timestampFields.includes(dateField)) {
                const newestDate = new Date(tenders[0][dateField]).toISOString();
                const oldestDate = new Date(tenders[tenders.length - 1][dateField]).toISOString();
                console.log(`Processing batch of ${tenders.length} tenders ${dateField} between:`);
                console.log(`  Newest: ${newestDate}`);
                console.log(`  Oldest: ${oldestDate}`);
            }
        }
        
        // Pre-check which tenders already exist to avoid processing them
        const sourceIds = tenders.map(tender => adapter.getSourceId(tender)).filter(id => id);
        const { data: existingTenders } = await supabaseAdmin
            .from('unified_tenders')
            .select('source_id')
            .eq('source_table', tableName)
            .in('source_id', sourceIds);
        
        const existingSourceIds = new Set(existingTenders?.map(t => t.source_id) || []);
        
        // Process tenders with controlled concurrency
        const results = await Promise.all(
            tenders.map(async (tender, index) => {
                // Simple concurrency control - wait before processing based on index
                await new Promise(resolve => setTimeout(resolve, Math.floor(index / concurrency) * 500));
                
                try {
                    attemptCount++;
                    
                    // Get source ID using the adapter
                    let sourceId = adapter.getSourceId(tender);
                    
                    if (!sourceId) {
                        console.warn('Could not determine a source ID for tender, using record index');
                        sourceId = `${tableName}_${offset + index}`;
                    }
                    
                    // Skip if tender already exists and we're not force reprocessing
                    if (!forceReprocess && existingSourceIds.has(sourceId)) {
                        console.log(`Tender ${sourceId} from ${tableName} already exists, skipping`);
                        
                        // Update the last_processed_at timestamp even for skipped records
                        if (timestampFields.includes('last_processed_at')) {
                            await supabaseAdmin
                                .from(tableName)
                                .update({ last_processed_at: new Date().toISOString() })
                                .eq('id', tender.id);
                        }
                        
                        skippedCount++;
                        return { success: true, skipped: true };
                    }
                    
                    // Log the tender's creation date if available
                    if (tender.created_at) {
                        console.log(`Processing tender ${sourceId} created at ${new Date(tender.created_at).toISOString()}`);
                    }
                    
                    console.log(`Processing tender ${offset + index + 1} from ${tableName} (Attempt ${attemptCount}, Successful: ${processedCount}/${limit})`);
                    
                    // Use the adapter to process the tender
                    const startTime = Date.now();
                    const normalizedTender = await adapter.processTender(tender, normalizeTender);
                    const processingTime = Date.now() - startTime;
                    
                    // Track performance statistics
                    trackPerformance(tableName, normalizedTender, processingTime);
                    
                    // Log normalization method
                    if (normalizedTender.normalized_method === 'rule-based-fallback') {
                        console.log(`Tender ${offset + index + 1} from ${tableName} normalized using rule-based fallback`);
                        fallbackCount++;
                    } else if (normalizedTender.normalized_method === 'rule-based-fast') {
                        console.log(`Tender ${offset + index + 1} from ${tableName} normalized using fast method`);
                        fastNormalizationCount++;
                    }
                    
                    // Check if an error occurred during normalization
                    if (normalizedTender.status === 'error') {
                        console.error(`Error normalizing tender: ${normalizedTender.description}`);
                        errorCount++;
                        return { success: false, error: normalizedTender.description };
                    }
                    
                    // CRITICAL FIX: Set url, tender_type, and other fields to null if they are empty strings
                    // Also handle currency fields
                    Object.keys(normalizedTender).forEach(key => {
                        const value = normalizedTender[key];
                        
                        // Handle empty strings
                        if (value === '') {
                            normalizedTender[key] = null;
                            return;
                        }
                        
                        // Handle currency/numeric fields
                        if (key === 'estimated_value' || key === 'contract_value' || key.includes('amount') || key.includes('value')) {
                            const numericValue = extractNumericValue(value);
                            if (numericValue !== null) {
                                normalizedTender[key] = numericValue;
                            } else {
                                console.warn(`Could not extract numeric value from ${key}: ${value}, setting to null`);
                                normalizedTender[key] = null;
                            }
                        }
                    });
                    
                    // CRITICAL FIX: Make sure source_table and source_id are set
                    normalizedTender.source_table = tableName;
                    normalizedTender.source_id = sourceId;
                    
                    // Insert the normalized tender in the database
                    try {
                        const { error: insertError } = await supabaseAdmin
                            .from('unified_tenders')
                            .insert(normalizedTender);
                        
                        if (insertError) {
                            console.error(`Error inserting unified tender: ${insertError.message}`);
                            errorCount++;
                            return { success: false, error: insertError.message };
                        }
                        processedCount++;
                        
                        // Update the last_processed_at timestamp
                        if (timestampFields.includes('last_processed_at')) {
                            await supabaseAdmin
                                .from(tableName)
                                .update({ last_processed_at: new Date().toISOString() })
                                .eq('id', tender.id);
                        }
                        
                        console.log(`Successfully processed tender ${sourceId} from ${tableName}`);
                        return { success: true };
                    } catch (error) {
                        console.error(`Error saving tender: ${error.message}`);
                        errorCount++;
                        return { success: false, error: error.message };
                    }
                } catch (error) {
                    console.error(`Error processing tender from ${tableName}:`, error);
                    errorCount++;
                    return { success: false, error: error.message };
                }
            })
        );
        
        return {
            success: true,
            processed: results.filter(r => r.success && !r.skipped).length,
            skipped: results.filter(r => r.success && r.skipped).length,
            errors: results.filter(r => !r.success).length,
            fallback: fallbackCount,
            fastNormalization: fastNormalizationCount
        };
    };
    
    // Process tenders in chunks until we reach the limit of successful normalizations
    let offset = 0;
    let moreToProcess = true;
    
    while (moreToProcess && processedCount < limit) {
        const chunkResult = await processChunk(offset);
        if (!chunkResult.success) {
            console.error(`Error processing chunk at offset ${offset}:`, chunkResult.error);
            return { 
                success: false, 
                error: chunkResult.error,
                processed: processedCount,
                skipped: skippedCount,
                errors: errorCount,
                fallback: fallbackCount,
                fastNormalization: fastNormalizationCount,
                attempts: attemptCount
            };
        }
        
        // Update counters
        processedCount += chunkResult.processed;
        skippedCount += chunkResult.skipped;
        errorCount += chunkResult.errors;
        fallbackCount += chunkResult.fallback || 0;
        fastNormalizationCount += chunkResult.fastNormalization || 0;
        
        // If we processed fewer items than the chunk size, we're done
        if (chunkResult.processed + chunkResult.skipped + chunkResult.errors < chunkSize) {
            moreToProcess = false;
        }
        
        // If we've skipped too many records without finding new ones to process, stop
        if (skippedCount > limit * 2) {
            console.log(`Skipped ${skippedCount} tenders while only finding ${processedCount} new ones. Stopping to avoid excessive processing.`);
            moreToProcess = false;
        }
        
        offset += chunkSize;
    }
    
    console.log(`Completed processing batch from ${tableName}:`);
    console.log(`- Successfully processed: ${processedCount} new tenders`);
    console.log(`- Updated: ${updatedCount} existing tenders`);
    console.log(`- Skipped: ${skippedCount} tenders`);
    console.log(`- Errors: ${errorCount}`);
    console.log(`- Total attempts: ${attemptCount}`);
    console.log(`- Normalization methods: ${fastNormalizationCount} fast, ${fallbackCount} fallbacks`);
    
    return {
        success: true,
        processed: processedCount,
        skipped: skippedCount,
        updated: updatedCount,
        errors: errorCount,
        fallback: fallbackCount,
        fastNormalization: fastNormalizationCount,
        attempts: attemptCount
    };
}

/**
 * Tracks performance statistics for tender normalization
 * @param {string} sourceTable - The source table
 * @param {Object} normalizedTender - The normalized tender data
 * @param {number} processingTime - Time taken to process in milliseconds
 */
function trackPerformance(sourceTable, normalizedTender, processingTime) {
    try {
        // Initialize source stats if not already present
        if (!performanceStats.bySource[sourceTable]) {
            performanceStats.bySource[sourceTable] = {
                totalProcessed: 0,
                llmUsed: 0,
                fastNormalization: 0,
                fallbackNormalization: 0,
                processingTimes: []
            };
        }
        
        // Update global stats
        performanceStats.totalProcessed++;
        performanceStats.processingTimes.push(processingTime);
        
        // Update source-specific stats
        performanceStats.bySource[sourceTable].totalProcessed++;
        performanceStats.bySource[sourceTable].processingTimes.push(processingTime);
        
        // Track normalization method
        if (normalizedTender.normalized_method === 'llm') {
            performanceStats.llmUsed++;
            performanceStats.bySource[sourceTable].llmUsed++;
        } else if (normalizedTender.normalized_method === 'rule-based-fast') {
            performanceStats.fastNormalization++;
            performanceStats.bySource[sourceTable].fastNormalization++;
        } else if (normalizedTender.normalized_method === 'rule-based-fallback') {
            performanceStats.fallbackNormalization++;
            performanceStats.bySource[sourceTable].fallbackNormalization++;
        }
    } catch (error) {
        // Silently ignore errors in stats tracking
        console.warn('Error tracking performance stats:', error.message);
    }
}

/**
 * Logs performance statistics
 */
function logPerformanceStats() {
    try {
        const totalProcessed = performanceStats.totalProcessed;
        if (totalProcessed === 0) return;
        
        const runningTime = (Date.now() - performanceStats.startTime) / 1000; // seconds
        const avgProcessingTime = performanceStats.processingTimes.reduce((a, b) => a + b, 0) / performanceStats.processingTimes.length;
        
        console.log('\n=== PERFORMANCE STATISTICS ===');
        console.log(`Total tenders processed: ${totalProcessed}`);
        console.log(`Total running time: ${runningTime.toFixed(2)} seconds`);
        console.log(`Average processing time: ${avgProcessingTime.toFixed(2)} ms per tender`);
        console.log(`Processing rate: ${(totalProcessed / runningTime * 60).toFixed(2)} tenders per minute`);
        
        // Normalization method breakdown
        const llmPct = Math.round(performanceStats.llmUsed / totalProcessed * 100);
        const fastPct = Math.round(performanceStats.fastNormalization / totalProcessed * 100);
        const fallbackPct = Math.round(performanceStats.fallbackNormalization / totalProcessed * 100);
        
        console.log(`Normalization methods:`);
        console.log(`  - LLM: ${performanceStats.llmUsed} (${llmPct}%)`);
        console.log(`  - Fast: ${performanceStats.fastNormalization} (${fastPct}%)`);
        console.log(`  - Fallback: ${performanceStats.fallbackNormalization} (${fallbackPct}%)`);
        
        // Source-specific stats
        console.log('\nBreakdown by source:');
        Object.entries(performanceStats.bySource).forEach(([source, stats]) => {
            if (stats.totalProcessed === 0) return;
            
            const sourceAvgTime = stats.processingTimes.reduce((a, b) => a + b, 0) / stats.processingTimes.length;
            const sourceLlmPct = Math.round(stats.llmUsed / stats.totalProcessed * 100);
            const sourceFastPct = Math.round(stats.fastNormalization / stats.totalProcessed * 100);
            const sourceFallbackPct = Math.round(stats.fallbackNormalization / stats.totalProcessed * 100);
            
            console.log(`\n${source}:`);
            console.log(`  - Total processed: ${stats.totalProcessed}`);
            console.log(`  - Average processing time: ${sourceAvgTime.toFixed(2)} ms`);
            console.log(`  - Normalization: ${sourceLlmPct}% LLM, ${sourceFastPct}% fast, ${sourceFallbackPct}% fallback`);
        });
        
        // Estimated cost savings
        // Assuming $0.01 per LLM call saved through fast normalization
        const costSaved = performanceStats.fastNormalization * 0.01;
        console.log(`\nEstimated cost savings: $${costSaved.toFixed(2)} (based on $0.01 per LLM call saved)`);
        
        // Potential improvements
        const potentialSavings = (performanceStats.llmUsed * 0.01).toFixed(2);
        console.log(`Potential additional cost savings: $${potentialSavings} (if all remaining LLM calls could be optimized)`);
        
        // Reset temporary counters but keep running totals
        performanceStats.processingTimes = [];
        Object.keys(performanceStats.bySource).forEach(source => {
            performanceStats.bySource[source].processingTimes = [];
        });
    } catch (error) {
        console.warn('Error logging performance stats:', error.message);
    }
}

// Add a function to get all registered source adapters
function getAllSourceTables() {
    return sourceRegistry.getRegisteredSources();
}

/**
 * Run continuous processing of tenders from all sources until stopped
 * This function will process tenders in a round-robin fashion indefinitely
 * @param {Object} supabaseAdmin - Supabase admin client
 * @param {Object} options - Processing options
 * @param {number} options.tendersPerSource - Number of tenders to process from each source in each round
 * @param {number} options.waitMinutes - Minutes to wait between processing rounds
 * @returns {Promise<void>}
 */
async function runContinuousProcessing(supabaseAdmin, options = {}) {
    const { 
        tendersPerSource = 50,  // Process more tenders per source in continuous mode
        waitMinutes = process.env.PROCESSING_INTERVAL_MINUTES ? 
            parseInt(process.env.PROCESSING_INTERVAL_MINUTES) : 5
    } = options;
    
    console.log(`Starting continuous processing of all sources. Will process up to ${tendersPerSource} tenders per source in each round.`);
    console.log(`Will wait ${waitMinutes} minutes between processing rounds.`);
    console.log('Press Ctrl+C to stop processing.\n');
    
    // Setup graceful shutdown
    let shouldContinue = true;
    
    const handleShutdown = async () => {
        console.log('\nGraceful shutdown initiated. Completing current processing...');
        shouldContinue = false;
        // Don't exit right away, let the current round finish
    };
    
    // Handle SIGINT (Ctrl+C) and SIGTERM
    process.on('SIGINT', handleShutdown);
    process.on('SIGTERM', handleShutdown);
    
    let round = 1;
    let totalProcessed = 0;
    let startTime = Date.now();
    
    try {
        while (shouldContinue) {
            console.log(`\n=== Starting processing round ${round} ===`);
            const roundStartTime = Date.now();
            
            const results = await processTendersFromAllSources(supabaseAdmin, {
                tendersPerSource,
                continuous: true
            });
            
            const roundEndTime = Date.now();
            const roundDurationMinutes = ((roundEndTime - roundStartTime) / 1000 / 60).toFixed(2);
            
            totalProcessed += results.processed;
            const totalProcessedPerMinute = (totalProcessed / ((roundEndTime - startTime) / 1000 / 60)).toFixed(2);
            
            console.log(`\n=== Completed processing round ${round} ===`);
            console.log(`Round duration: ${roundDurationMinutes} minutes`);
            console.log(`Total tenders processed since start: ${totalProcessed}`);
            console.log(`Overall processing rate: ${totalProcessedPerMinute} tenders per minute`);
            
            if (!shouldContinue) {
                console.log('Shutting down as requested...');
                break;
            }
            
            // Adjust wait time based on whether tenders were processed
            // If nothing was processed, we might want to wait longer
            const actualWaitMinutes = results.processed > 0 ? waitMinutes : Math.min(waitMinutes * 2, 30);
            
            if (actualWaitMinutes > 0) {
                console.log(`Waiting ${actualWaitMinutes} minutes before next processing round...`);
                await new Promise(resolve => setTimeout(resolve, actualWaitMinutes * 60 * 1000));
            }
            
            // Check if we should continue
            if (!shouldContinue) {
                console.log('Shutting down as requested during wait period...');
                break;
            }
            
            round++;
        }
    } catch (error) {
        console.error('Error during continuous processing:', error);
        throw error;
    } finally {
        // Remove event listeners
        process.removeListener('SIGINT', handleShutdown);
        process.removeListener('SIGTERM', handleShutdown);
        
        console.log('\nContinuous processing completed or stopped.');
        console.log(`Total rounds: ${round}`);
        console.log(`Total tenders processed: ${totalProcessed}`);
        
        const totalDurationHours = ((Date.now() - startTime) / 1000 / 60 / 60).toFixed(2);
        console.log(`Total runtime: ${totalDurationHours} hours`);
    }
}

module.exports = {
    processTendersFromTable,
    processTendersFromAllSources,
    runContinuousProcessing,
    getAllSourceTables
};