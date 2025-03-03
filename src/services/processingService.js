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
            // Remove commas, spaces, and other separators
            .replace(/[,\s]/g, '')
            // Remove any trailing currency codes that might remain
            .replace(/\s*[A-Z]{3}$/g, '')
            // Remove any remaining non-numeric characters except decimal point and minus sign
            .replace(/[^\d.-]/g, '')
            // Trim whitespace
            .trim();
            
        // Handle edge cases
        if (cleanValue === '' || cleanValue === '.' || cleanValue === '-') {
            console.warn(`Invalid numeric string after cleaning: "${cleanValue}" from original value: "${value}"`);
            return null;
        }
        
        // Parse the cleaned value
        const numericValue = parseFloat(cleanValue);
        
        // Ensure we have a valid number
        if (isNaN(numericValue)) {
            console.warn(`Could not parse numeric value from: "${value}", cleaned value was: "${cleanValue}"`);
            return null;
        }
        
        // Ensure the value is reasonable (no extremely large or small numbers)
        if (!Number.isFinite(numericValue) || Math.abs(numericValue) > 1e15) {
            console.warn(`Numeric value out of reasonable range: ${numericValue} from: "${value}"`);
            return null;
        }
        
        return numericValue;
    } catch (error) {
        console.warn(`Failed to extract numeric value from: "${value}"`, error);
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
    
    // Calculate batch size for round-robin processing
    const batchSize = Math.min(5, Math.floor(tendersPerSource / 4)); // Process smaller batches for better distribution
    
    // Process sources in rounds
    let remainingTenders = tendersPerSource;
    while (remainingTenders > 0) {
        const currentBatchSize = Math.min(batchSize, remainingTenders);
        console.log(`\nProcessing round with batch size ${currentBatchSize}, remaining tenders per source: ${remainingTenders}`);
        
        for (const sourceTable of sources) {
            console.log(`\nProcessing batch from ${sourceTable}...`);
            
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
            
            // If in continuous mode and we've processed nothing from this source, skip it in future rounds
            if (continuous && result.processed + result.skipped === 0) {
                console.log(`No more tenders to process from ${sourceTable}, removing from round-robin`);
                sources.splice(sources.indexOf(sourceTable), 1);
                if (sources.length === 0) {
                    console.log('No more sources with tenders to process');
                    return totalResults;
                }
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
    let processedCount = 0;
    let skippedCount = 0;
    let errorCount = 0;
    let fallbackCount = 0;
    let fastNormalizationCount = 0;
    let updatedCount = 0;
    let attemptCount = 0;
    
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
    const chunkSize = 20; // Reduced for better control
    const concurrency = 5; // Reduced for better stability
    
    // Get all unprocessed tenders first
    let queryBatch = supabaseAdmin.from(tableName)
        .select('*');
        
    // Always sort by created_at desc first to prioritize newer tenders
    if (timestampFields.includes('created_at')) {
        queryBatch = queryBatch.order('created_at', { ascending: false });
    } else if (timestampFields.includes('updated_at')) {
        queryBatch = queryBatch.order('updated_at', { ascending: false });
    }
    
    // Filter for unprocessed tenders
    if (timestampFields.includes('last_processed_at') && !forceReprocess) {
        queryBatch = queryBatch.is('last_processed_at', null);
    }
    
    // Get all potential tenders first
    const { data: allTenders, error: fetchError } = await queryBatch;
    
    if (fetchError) {
        console.error(`Error fetching tenders from ${tableName}:`, fetchError);
        return { success: false, error: fetchError.message };
    }
    
    if (!allTenders || allTenders.length === 0) {
        console.log(`No unprocessed tenders found in ${tableName}`);
        return { success: true, processed: 0, skipped: 0, errors: 0, fallback: 0 };
    }
    
    // Log the date range of tenders being processed
    if (allTenders.length > 0) {
        const dateField = timestampFields.includes('created_at') ? 'created_at' : 'updated_at';
        if (timestampFields.includes(dateField)) {
            const newestDate = new Date(allTenders[0][dateField]).toISOString();
            const oldestDate = new Date(allTenders[allTenders.length - 1][dateField]).toISOString();
            console.log(`Found ${allTenders.length} unprocessed tenders ${dateField} between:`);
            console.log(`  Newest: ${newestDate}`);
            console.log(`  Oldest: ${oldestDate}`);
        }
    }
    
    // Pre-check which tenders already exist
    const sourceIds = allTenders.map(tender => adapter.getSourceId(tender)).filter(id => id);
    const { data: existingTenders } = await supabaseAdmin
        .from('unified_tenders')
        .select('source_id')
        .eq('source_table', tableName)
        .in('source_id', sourceIds);
    
    const existingSourceIds = new Set(existingTenders?.map(t => t.source_id) || []);
    
    // Filter out existing tenders before processing
    const tendersToProcess = allTenders.filter(tender => {
        const sourceId = adapter.getSourceId(tender);
        if (!forceReprocess && existingSourceIds.has(sourceId)) {
            skippedCount++;
            // Update last_processed_at for skipped tenders to avoid reprocessing
            if (timestampFields.includes('last_processed_at')) {
                supabaseAdmin
                    .from(tableName)
                    .update({ last_processed_at: new Date().toISOString() })
                    .eq('id', tender.id)
                    .then(() => {
                        console.log(`Updated last_processed_at for skipped tender ${sourceId}`);
                    })
                    .catch(error => {
                        console.warn(`Failed to update last_processed_at for skipped tender ${sourceId}:`, error.message);
                    });
            }
            return false;
        }
        return true;
    });
    
    console.log(`Found ${tendersToProcess.length} new tenders to process after filtering existing ones`);
    
    // Update normalization evaluation logic
    const shouldUseFastNormalization = (tender, normalizationNeeds) => {
        // Always use fast normalization for SAM.gov
        if (tableName === 'sam_gov') {
            return true;
        }

        // Use fast normalization for English tenders with simple content
        if (normalizationNeeds.language === 'en') {
            // If content is minimal, use fast normalization
            if (normalizationNeeds.minimalContent) {
                return true;
            }
            
            // If no complex fields and no translation needed, use fast normalization
            if (!normalizationNeeds.complexFields && !normalizationNeeds.needsTranslation) {
                return true;
            }
        }

        return false;
    };

    // Process tenders in chunks with controlled concurrency
    for (let i = 0; i < tendersToProcess.length && processedCount < limit; i += chunkSize) {
        const chunk = tendersToProcess.slice(i, i + chunkSize);
        console.log(`Processing chunk from ${i} to ${i + chunk.length} of ${tendersToProcess.length}`);
        
        const results = await Promise.all(
            chunk.map(async (tender, index) => {
                // Simple concurrency control
                await new Promise(resolve => setTimeout(resolve, Math.floor(index / concurrency) * 100));
                
                try {
                    attemptCount++;
                    const sourceId = adapter.getSourceId(tender) || `${tableName}_${i + index}`;
                    
                    // Log the tender's creation date if available
                    if (tender.created_at) {
                        console.log(`Processing tender ${attemptCount}/${tendersToProcess.length} from ${tableName}`);
                    }
                    
                    // Evaluate normalization needs first
                    const normalizationNeeds = await evaluateNormalizationNeeds(tender);
                    
                    // Determine normalization method
                    const useFastNormalization = shouldUseFastNormalization(tender, normalizationNeeds);
                    
                    // Use the adapter to process the tender with determined method
                    const startTime = Date.now();
                    const normalizedTender = await adapter.processTender(tender, normalizeTender, useFastNormalization);
                    const processingTime = Date.now() - startTime;
                    
                    // Track performance statistics
                    trackPerformance(tableName, normalizedTender, processingTime);
                    
                    // Log normalization completion with single message
                    if (normalizedTender.normalized_method === 'rule-based-fast') {
                        console.log(`Fast normalization completed in ${(processingTime / 1000).toFixed(3)} seconds`);
                        fastNormalizationCount++;
                    } else if (normalizedTender.normalized_method === 'llm') {
                        console.log(`LLM normalization completed in ${(processingTime / 1000).toFixed(3)} seconds`);
                    } else {
                        console.log(`Using fallback normalization for ${tableName}`);
                        fallbackCount++;
                    }
                    
                    if (normalizedTender.status === 'error') {
                        console.error(`Error normalizing tender: ${normalizedTender.description}`);
                        errorCount++;
                        return { success: false, error: normalizedTender.description };
                    }
                    
                    // Clean up fields and ensure schema compatibility
                    const schemaFields = {
                        estimated_value: true,
                        contract_value: true,
                        award_value: true, // Using award_value instead of award_amount
                        potential_value: true // Using potential_value instead of potential_award_amount
                    };
                    
                    // Clean up fields
                    Object.keys(normalizedTender).forEach(key => {
                        const value = normalizedTender[key];
                        if (value === '') {
                            normalizedTender[key] = null;
                        } else if (schemaFields[key] || key.includes('value') || key.includes('amount')) {
                            try {
                                // Always convert numeric fields using extractNumericValue
                                const numericValue = extractNumericValue(value);
                                if (numericValue === null && value !== null && value !== undefined) {
                                    console.warn(`Could not extract numeric value for ${key} from: ${value}, setting to null`);
                                }
                                normalizedTender[key] = numericValue;
                            } catch (error) {
                                console.warn(`Error processing numeric value for ${key}: ${error.message}`);
                                normalizedTender[key] = null;
                            }
                        }
                    });
                    
                    // Ensure schema compatibility
                    if ('award_amount' in normalizedTender) {
                        normalizedTender.award_value = normalizedTender.award_amount;
                        delete normalizedTender.award_amount;
                    }
                    if ('potential_award_amount' in normalizedTender) {
                        normalizedTender.potential_value = normalizedTender.potential_award_amount;
                        delete normalizedTender.potential_award_amount;
                    }
                    
                    normalizedTender.source_table = tableName;
                    normalizedTender.source_id = sourceId;
                    
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
                    console.error(`Error processing tender:`, error);
                    errorCount++;
                    return { success: false, error: error.message };
                }
            })
        );
        
        // If we've reached the limit or processed all tenders, break
        if (processedCount >= limit || i + chunkSize >= tendersToProcess.length) {
            break;
        }
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