/**
 * processingService.js
 * Service for processing tenders from various sources
 */

// Import required dependencies
const sourceRegistry = require('./sourceRegistry');
const { normalizeTender, evaluateNormalizationNeeds } = require('./tenderNormalizer');

// Configuration options
const CONFIG = {
    // Set to true to use fast normalization for World Bank tenders to improve performance
    useWbFastNormalization: true,
    // Set to true to use fast normalization for ADB tenders to prevent timeouts
    useAdbFastNormalization: true,
    // Timeout for LLM normalization in milliseconds (default: 20 seconds)
    llmNormalizationTimeout: 20000
};

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
 * Determines if fast normalization should be used
 * @param {string} sourceTable - The source table name
 * @param {Object} normalizationNeeds - The evaluated normalization needs
 * @returns {boolean} Whether to use fast normalization
 */
function shouldUseFastNormalization(sourceTable, normalizationNeeds) {
    // SAM.gov tenders always use fast normalization
    if (sourceTable === 'sam_gov') {
        return true;
    }
    
    // Use fast normalization for World Bank tenders if configured
    if (sourceTable === 'wb' && CONFIG.useWbFastNormalization) {
        return true;
    }
    
    // Use fast normalization for ADB tenders if configured
    if (sourceTable === 'adb' && CONFIG.useAdbFastNormalization) {
        return true;
    }

    // Use fast normalization for English tenders with simple content
    if (normalizationNeeds.language === 'en') {
        if (normalizationNeeds.minimalContent || (!normalizationNeeds.complexFields && !normalizationNeeds.needsTranslation)) {
            return true;
        }
    }

    return false;
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
    const unlimitedProcessing = tendersPerSource <= 0;
    
    console.log(`Starting to process tenders from all sources in round-robin mode${continuous ? ' (continuous)' : ''}${unlimitedProcessing ? ' (unlimited processing)' : ''}`);
    
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
    
    if (unlimitedProcessing) {
        // Process all tenders from each source sequentially
        for (const sourceTable of sources) {
            console.log(`\n=== Processing source: ${sourceTable} ===\n`);
            
            try {
                // Process all tenders from this source (pass 0 to process all)
                const result = await processTendersFromTable(supabaseAdmin, sourceTable, 0);
                
                // Update total counts
                totalResults.processed += result.processed || 0;
                totalResults.skipped += result.skipped || 0;
                totalResults.updated += result.updated || 0;
                totalResults.errors += result.errors || 0;
                totalResults.fallback += result.fallback || 0;
                totalResults.fastNormalization += result.fastNormalization || 0;
                
                // Update source-specific counts
                totalResults.bySource[sourceTable].processed += result.processed || 0;
                totalResults.bySource[sourceTable].skipped += result.skipped || 0;
                totalResults.bySource[sourceTable].updated += result.updated || 0;
                totalResults.bySource[sourceTable].errors += result.errors || 0;
                totalResults.bySource[sourceTable].fallback += result.fallback || 0;
                totalResults.bySource[sourceTable].fastNormalization += result.fastNormalization || 0;
                
                console.log(`Processed ${result.processed || 0} tenders from ${sourceTable}`);
            } catch (error) {
                console.error(`Error processing ${sourceTable}:`, error);
                totalResults.success = false;
                totalResults.errors++;
            }
        }
    } else {
        // Calculate batch size for round-robin processing
        const batchSize = Math.min(5, Math.floor(tendersPerSource / 4)); // Process smaller batches for better distribution
        
        // Process sources in rounds with limits
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
    }
    
    // Log performance stats
    logPerformanceStats();
    
    // Provide summary before finishing
    console.log('\n=== Processing Summary ===');
    console.log(`- Total processed: ${totalResults.processed}`);
    console.log(`- Total skipped: ${totalResults.skipped}`);
    console.log(`- Total updated: ${totalResults.updated}`);
    console.log(`- Total errors: ${totalResults.errors}`);
    console.log(`- Normalization: ${totalResults.fastNormalization} fast, ${totalResults.fallback} fallbacks`);
    
    // Per-source summary
    console.log('\n=== Source Breakdowns ===');
    for (const sourceTable of Object.keys(totalResults.bySource)) {
        const sourceStats = totalResults.bySource[sourceTable];
        console.log(`\n${sourceTable}:`);
        console.log(`- Processed: ${sourceStats.processed}`);
        console.log(`- Skipped: ${sourceStats.skipped}`);
        console.log(`- Updated: ${sourceStats.updated}`);
        console.log(`- Errors: ${sourceStats.errors}`);
        console.log(`- Normalization: ${sourceStats.fastNormalization} fast, ${sourceStats.fallback} fallbacks`);
    }
    
    if (continuous) {
        console.log('\nContinuous processing enabled, will run again after a short delay...');
        // Schedule next run with a delay to prevent hammering the database
        setTimeout(() => {
            processTendersFromAllSources(supabaseAdmin, options);
        }, 5000); // 5 second delay between runs
    }
    
    return totalResults;
}

/**
 * Process a limited number of tenders from a specific table
 * @param {Object} supabaseAdmin - Supabase admin client
 * @param {string} tableName - Name of the source table
 * @param {number} limit - Maximum number of successful normalizations to achieve
 * @param {boolean} forceReprocess - Whether to reprocess tenders that already exist in the database
 * @param {Array} preFilteredTenders - Pre-filtered tenders to use instead of querying the database
 * @returns {Promise<Object>} Processing results
 */
async function processTendersFromTable(supabaseAdmin, tableName, limit = 100, forceReprocess = false, preFilteredTenders = null) {
    // Check if we should process without limit
    const unlimitedProcessing = limit <= 0;
    console.log(`Starting to process ${unlimitedProcessing ? 'ALL' : `until ${limit} successful normalizations`} from ${tableName}${forceReprocess ? ' (force reprocessing enabled)' : ''}${preFilteredTenders ? ' (using pre-filtered tenders)' : ''}`);
    
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
    
    // Define chunk size and concurrency for batch processing
    const chunkSize = 25;
    const concurrency = 5;
    
    // Initialize the existingTenderMap at function scope
    let existingTenderMap = new Map();
    
    // Initialize timestampFields at function scope
    let timestampFields = [];
    
    // If we have pre-filtered tenders, use those directly instead of querying the database
    let tendersToProcess = [];
    
    if (preFilteredTenders) {
        // Use the pre-filtered tenders provided
        console.log(`Using ${preFilteredTenders.length} pre-filtered tenders for ${tableName}`);
        tendersToProcess = preFilteredTenders;
        
        // Still need to check which tenders already exist
        const sourceIds = tendersToProcess.map(tender => adapter.getSourceId(tender)).filter(id => id);
        if (sourceIds.length > 0) {
            const { data: existingTenders } = await supabaseAdmin
                .from('unified_tenders')
                .select('source_id, updated_at')
                .eq('source_table', tableName)
                .in('source_id', sourceIds);
            
            existingTenderMap = new Map(existingTenders?.map(t => [t.source_id, t]) || []);
        }
        
        // Check if table has timestamp fields for incremental processing
        const tableInfo = await supabaseAdmin
            .from('information_schema.columns')
            .select('column_name')
            .eq('table_name', tableName)
            .in('column_name', ['created_at', 'updated_at', 'last_processed_at']);
        
        const hasTimestampFields = tableInfo.data && tableInfo.data.length > 0;
        timestampFields = hasTimestampFields ? tableInfo.data.map(col => col.column_name) : [];
        console.log(`Found timestamp fields for table ${tableName}:`, timestampFields);
    } else {
    // Check if table has timestamp fields for incremental processing
    const tableInfo = await supabaseAdmin
        .from('information_schema.columns')
        .select('column_name')
        .eq('table_name', tableName)
        .in('column_name', ['created_at', 'updated_at', 'last_processed_at']);
    
    const hasTimestampFields = tableInfo.data && tableInfo.data.length > 0;
    timestampFields = hasTimestampFields ? tableInfo.data.map(col => col.column_name) : [];
    console.log(`Found timestamp fields for table ${tableName}:`, timestampFields);
    
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
            .select('source_id, updated_at')
            .eq('source_table', tableName)
            .in('source_id', sourceIds);
        
        existingTenderMap = new Map(existingTenders?.map(t => [t.source_id, t]) || []);
        
        // Filter out existing tenders before processing
        tendersToProcess = allTenders.filter(tender => {
            const sourceId = adapter.getSourceId(tender);
            if (!sourceId) return false;

            const existingTender = existingTenderMap.get(sourceId);
            if (!existingTender) return true;

            if (forceReprocess) return true;

            // Skip if tender exists and hasn't been updated
            if (tender.updated_at && existingTender.updated_at) {
                const tenderDate = new Date(tender.updated_at);
                const existingDate = new Date(existingTender.updated_at);
                if (tenderDate <= existingDate) {
                    skippedCount++;
                    // Update last_processed_at for skipped tenders
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
            }

            skippedCount++;
            return false;
        });
        
        console.log(`Found ${tendersToProcess.length} new tenders to process after filtering existing ones`);
    }
    
    // Process tenders in chunks with controlled concurrency
    for (let i = 0; i < tendersToProcess.length && (unlimitedProcessing || processedCount < limit); i += chunkSize) {
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
                    console.log(`Evaluating normalization needs for tender from ${tableName}`);
                    const normalizationNeeds = await evaluateNormalizationNeeds(tender, tableName);
                    
                    // Determine normalization method
                    const useFastNormalization = shouldUseFastNormalization(tableName, normalizationNeeds);
                    
                    let normalizationReason = "";
                    if (tableName === 'sam_gov') {
                        normalizationReason = "SAM.gov tenders don't require translation or complex normalization";
                    } else if (tableName === 'wb' && CONFIG.useWbFastNormalization) {
                        normalizationReason = "World Bank tenders using optimized fast path (configured for performance)";
                    } else if (tableName === 'adb' && CONFIG.useAdbFastNormalization) {
                        normalizationReason = "ADB tenders using optimized fast path (configured for performance)";
                    } else if (tender.description && tender.description.length > 10000) {
                        normalizationReason = "Tender description exceeds optimal size for LLM processing, using chunked parsing";
                    } else if (normalizationNeeds.minimalContent) {
                        normalizationReason = "Tender has minimal content suitable for fast normalization";
                    } else if (normalizationNeeds.language === 'en' && !normalizationNeeds.complexFields) {
                        normalizationReason = "English tender with standard fields suitable for fast normalization";
                    }
                    
                    // Capture all console.log calls during normalization to control messaging
                    const originalConsoleLog = console.log;
                    const capturedLogs = [];
                    let methodUsed = useFastNormalization ? "Fast" : "LLM";
                    let fallbackUsed = false;
                    
                    console.log = (message, ...args) => {
                        if (typeof message === 'string') {
                            capturedLogs.push(message);
                            
                            // Only allow specific messages to be logged during processing
                            if (message.includes('fallback normalization')) {
                                fallbackUsed = true;
                                methodUsed = "Fallback";
                            }
                        }
                    };
                    
                    // Do not log the intent here to avoid conflicts with the actual method used
                    
                    // Use the adapter to process the tender
                    const startTime = Date.now();
                    let normalizedTender;
                    try {
                        // Add timeout to prevent hanging on LLM normalization
                        const processTenderPromise = adapter.processTender(tender, normalizeTender, useFastNormalization);
                        
                        // Only apply timeout if using LLM normalization (not fast normalization)
                        if (!useFastNormalization) {
                            normalizedTender = await promiseWithTimeout(
                                processTenderPromise,
                                CONFIG.llmNormalizationTimeout,
                                'LLM normalization timed out'
                            );
                        } else {
                            normalizedTender = await processTenderPromise;
                        }
                    } catch (error) {
                        console.warn(`Normalization error: ${error.message}, falling back to fast normalization`);
                        // If normalization fails, try fallback
                        fallbackUsed = true;
                        methodUsed = "Fallback";
                        normalizedTender = await adapter.processTender(tender, normalizeTender, true);
                    } finally {
                        // Restore original console
                        console.log = originalConsoleLog;
                    }
                    
                    const processingTime = Date.now() - startTime;
                    
                    // Now we know which method was actually used, so log it clearly
                    if (fallbackUsed) {
                        if (tableName === 'sam_gov') {
                            console.log(`Using fallback normalization for ${tableName} (normally would use fast normalization)`);
                        } else {
                            console.log(`Using fallback normalization for ${tableName} due to LLM unavailability`);
                        }
                    } else if (methodUsed === "Fast") {
                        console.log(`Using fast normalization for tender: ${normalizationReason}`);
                    } else {
                        console.log(`Using LLM normalization for tender from ${tableName}`);
                    }
                    
                    // Track performance statistics
                    trackPerformance(tableName, normalizedTender, processingTime);
                    
                    // Clean up fields and ensure schema compatibility
                    const schemaFields = {
                        estimated_value: true,
                        award_value: true,
                        potential_value: true
                    };
                    
                    // Clean up fields
                    Object.keys(normalizedTender).forEach(key => {
                        const value = normalizedTender[key];
                        if (value === '') {
                            normalizedTender[key] = null;
                        } else if (schemaFields[key]) {
                            const numericValue = extractNumericValue(value);
                            if (numericValue === null && value !== null && value !== undefined) {
                                console.warn(`Could not extract numeric value for ${key} from: ${value}, setting to null`);
                            }
                            normalizedTender[key] = numericValue;
                        }
                    });
                    
                    // Remove any fields not in schema
                    if ('contract_value' in normalizedTender) {
                        delete normalizedTender.contract_value;
                    }
                    
                    // Log normalization completion with timing based on the actual method used
                    console.log(`${methodUsed} normalization completed in ${(processingTime / 1000).toFixed(3)} seconds`);
                    
                    if (normalizedTender.status === 'error') {
                        console.error(`Error normalizing tender: ${normalizedTender.description}`);
                        errorCount++;
                        return { success: false, error: normalizedTender.description };
                    }
                    
                    normalizedTender.source_table = tableName;
                    normalizedTender.source_id = sourceId;
                    
                    try {
                        // Try to update first if the tender exists
                        if (existingTenderMap.has(sourceId)) {
                            const { error: updateError } = await supabaseAdmin
                                .from('unified_tenders')
                                .update(normalizedTender)
                                .eq('source_table', tableName)
                                .eq('source_id', sourceId);

                            if (!updateError) {
                                updatedCount++;
                                processedCount++;
                                console.log(`Successfully updated tender ${sourceId} from ${tableName}`);
                                return { success: true };
                            }
                        }

                        // If update fails or tender doesn't exist, try insert
                        const { error: insertError } = await supabaseAdmin
                            .from('unified_tenders')
                            .insert(normalizedTender);
                        
                        if (insertError) {
                            if (insertError.code === '23505') { // Unique violation
                                console.log(`Tender ${sourceId} from ${tableName} already exists, skipping`);
                                skippedCount++;
                                return { success: true };
                            }
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
        
        console.log('Normalization methods:');
        console.log(`  - LLM: ${performanceStats.llmUsed} (${llmPct}%)`);
        console.log(`  - Fast: ${performanceStats.fastNormalization} (${fastPct}%)`);
        console.log(`  - Fallback: ${performanceStats.fallbackNormalization} (${fallbackPct}%)`);
        
        // Reset temporary counters but keep running totals
        performanceStats.processingTimes = [];
        Object.keys(performanceStats.bySource).forEach(source => {
            performanceStats.bySource[source].processingTimes = [];
        });
    } catch (error) {
        console.warn('Error logging performance stats:', error.message);
    }
}

/**
 * Process newest tenders from all sources for daily updates
 * @param {object} supabaseAdmin - The Supabase admin client
 * @param {object} options - Processing options
 * @param {number} options.tendersPerSource - Maximum tenders to process per source (default: 1000)
 * @param {string} options.sinceDate - Process tenders created/updated since this date (ISO format)
 * @param {boolean} options.continuous - Whether to run in continuous mode
 * @returns {object} Processing results
 */
async function processNewestTendersFromAllSources(supabaseAdmin, options = {}) {
    const { tendersPerSource = 1000, sinceDate = null, continuous = false } = options;
    
    console.log(`Starting to process newest tenders from all sources${sinceDate ? ` since ${sinceDate}` : ''}${continuous ? ' (continuous)' : ''}`);
    
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
    
    // Process each source sequentially
    for (const sourceTable of sources) {
        console.log(`\n=== Processing source: ${sourceTable} ===\n`);
        
        try {
            // Get timestamp fields for this table
            const { data: tableInfo } = await supabaseAdmin
                .from('information_schema.columns')
                .select('column_name')
                .eq('table_name', sourceTable)
                .in('column_name', ['created_at', 'updated_at']);
            
            const timestampFields = tableInfo.map(col => col.column_name);
            
            // Prepare query for this source
            let query = supabaseAdmin.from(sourceTable)
                .select('*')
                .limit(tendersPerSource);
            
            // Apply date filter if provided
            if (sinceDate && timestampFields.length > 0) {
                const dateField = timestampFields.includes('updated_at') ? 'updated_at' : 'created_at';
                if (timestampFields.includes(dateField)) {
                    query = query.gte(dateField, sinceDate);
                    console.log(`Filtering ${sourceTable} tenders by ${dateField} >= ${sinceDate}`);
                }
            }
            
            // Sort by timestamp (newest first)
            if (timestampFields.includes('created_at')) {
                query = query.order('created_at', { ascending: false });
            } else if (timestampFields.includes('updated_at')) {
                query = query.order('updated_at', { ascending: false });
            }
            
            // Fetch tenders
            const { data: newestTenders, error } = await query;
            
            if (error) {
                console.error(`Error fetching newest tenders from ${sourceTable}:`, error);
                totalResults.success = false;
                totalResults.errors++;
                continue;
            }
            
            if (!newestTenders || newestTenders.length === 0) {
                console.log(`No new tenders found in ${sourceTable}`);
                continue;
            }
            
            console.log(`Found ${newestTenders.length} newest tenders from ${sourceTable}`);
            
            // Process these tenders
            const adapter = sourceRegistry.getAdapter(sourceTable);
            if (!adapter) {
                console.error(`No adapter available for ${sourceTable}`);
                continue;
            }
            
            // Pre-check which tenders already exist
            const sourceIds = newestTenders.map(tender => adapter.getSourceId(tender)).filter(id => id);
            const { data: existingTenders } = await supabaseAdmin
                .from('unified_tenders')
                .select('source_id, updated_at')
                .eq('source_table', sourceTable)
                .in('source_id', sourceIds);
                
            const existingTenderMap = new Map(existingTenders?.map(t => [t.source_id, t]) || []);
            
            // Process tenders with the existing function, forcing reprocessing for already existing tenders
            const result = await processTendersFromTable(
                supabaseAdmin, 
                sourceTable, 
                tendersPerSource, 
                true, // Force reprocessing to ensure newest data is captured
                newestTenders // Pass the pre-filtered tenders
            );
            
            // Update total counts
            totalResults.processed += result.processed || 0;
            totalResults.skipped += result.skipped || 0;
            totalResults.updated += result.updated || 0;
            totalResults.errors += result.errors || 0;
            totalResults.fallback += result.fallback || 0;
            totalResults.fastNormalization += result.fastNormalization || 0;
            
            // Update source-specific counts
            totalResults.bySource[sourceTable].processed += result.processed || 0;
            totalResults.bySource[sourceTable].skipped += result.skipped || 0;
            totalResults.bySource[sourceTable].updated += result.updated || 0;
            totalResults.bySource[sourceTable].errors += result.errors || 0;
            totalResults.bySource[sourceTable].fallback += result.fallback || 0;
            totalResults.bySource[sourceTable].fastNormalization += result.fastNormalization || 0;
            
            console.log(`Processed ${result.processed || 0} newest tenders from ${sourceTable}`);
        } catch (error) {
            console.error(`Error processing ${sourceTable}:`, error);
            totalResults.success = false;
            totalResults.errors++;
        }
    }
    
    // Log performance stats
    logPerformanceStats();
    
    // Provide summary before finishing
    console.log('\n=== Processing Summary ===');
    console.log(`- Total processed: ${totalResults.processed}`);
    console.log(`- Total skipped: ${totalResults.skipped}`);
    console.log(`- Total updated: ${totalResults.updated}`);
    console.log(`- Total errors: ${totalResults.errors}`);
    console.log(`- Normalization: ${totalResults.fastNormalization} fast, ${totalResults.fallback} fallbacks`);
    
    // Per-source summary
    console.log('\n=== Source Breakdowns ===');
    for (const sourceTable of Object.keys(totalResults.bySource)) {
        const sourceStats = totalResults.bySource[sourceTable];
        console.log(`\n${sourceTable}:`);
        console.log(`- Processed: ${sourceStats.processed}`);
        console.log(`- Skipped: ${sourceStats.skipped}`);
        console.log(`- Updated: ${sourceStats.updated}`);
        console.log(`- Errors: ${sourceStats.errors}`);
        console.log(`- Normalization: ${sourceStats.fastNormalization} fast, ${sourceStats.fallback} fallbacks`);
    }
    
    if (continuous) {
        console.log('\nContinuous processing enabled, will run again after a short delay...');
        // Schedule next run with a delay to prevent hammering the database
        setTimeout(() => {
            processNewestTendersFromAllSources(supabaseAdmin, options);
        }, 5000); // 5 second delay between runs
    }
    
    return totalResults;
}

/**
 * Helper function to add a timeout to a promise
 * @param {Promise} promise - The promise to add a timeout to
 * @param {number} timeoutMs - Timeout in milliseconds
 * @param {string} errorMessage - Error message to throw on timeout
 * @returns {Promise} A new promise with a timeout
 */
function promiseWithTimeout(promise, timeoutMs, errorMessage) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
            reject(new Error(errorMessage));
        }, timeoutMs);
    });

    return Promise.race([
        promise,
        timeoutPromise
    ]).finally(() => {
        clearTimeout(timeoutId);
    });
}

module.exports = {
    processTendersFromTable,
    processTendersFromAllSources,
    processNewestTendersFromAllSources
};