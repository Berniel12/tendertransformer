/**
 * tenderNormalizer.js
 * Service for normalizing tender data using LLM
 */

const { Actor } = require('apify');
const fetch = require('node-fetch');

// Performance tracking statistics
const performanceStats = {
    fastNormalization: 0,
    fallbackNormalization: 0
};

/**
 * Global counter to track processing for selective logging
 */
let tenderProcessingCounter = 0;

/**
 * Global stats to track normalization improvements
 */
const normalizationStats = {
    total: 0,
    fieldsProcessed: 0,
    fieldsFilledBefore: 0,
    fieldsFilledAfter: 0,
    titleChanges: 0,
    sectorAdded: 0,
    descriptionImproved: 0,
    contactInfoAdded: 0,
    valueAdded: 0,
    sources: {},
    startTime: Date.now()
};

/**
 * File-based logging function to avoid console logging limitations
 * @param {Object} logData - Log data to write to file
 */
function writeDetailedLog(phase, before, after, note) {
    try {
        // Only log a smaller sample (1 in 50) to avoid excessive logging
        if (tenderProcessingCounter % 50 !== 0) return;
        
        // Prepare the log entry as an object for structured logging
        const logEntry = {
            timestamp: new Date().toISOString(),
            phase,
            note,
            tenderId: after.source_id || after.id || 'unknown',
            source: after.source_table || 'unknown',
            changes: [],
            stats: {
                totalFields: Object.keys(after).length,
                filledBefore: 0,
                filledAfter: 0,
                emptyBefore: 0,
                emptyAfter: 0
            },
            emptyFields: []
        };
        
        // Track changed fields
        Object.keys(after).forEach(key => {
            // Skip metadata fields
            if (['normalized_at', 'normalized_by', 'normalized_method', 'processing_time_ms'].includes(key)) {
                return;
            }
            
            const beforeValue = before[key];
            const afterValue = after[key];
            
            // Count filled/empty fields before
            if (beforeValue === null || beforeValue === undefined || beforeValue === '') {
                logEntry.stats.emptyBefore++;
            } else {
                logEntry.stats.filledBefore++;
            }
            
            // Count filled/empty fields after
            if (afterValue === null || afterValue === undefined || afterValue === '') {
                logEntry.stats.emptyAfter++;
                logEntry.emptyFields.push(key);
            } else {
                logEntry.stats.filledAfter++;
            }
            
            // Track fields that changed during normalization
            if (beforeValue !== afterValue && afterValue !== null && afterValue !== undefined) {
                logEntry.changes.push({
                    field: key,
                    before: beforeValue || '(empty)',
                    after: afterValue
                });
            }
        });
        
        // Calculate improvement percentage
        logEntry.stats.improvementPercentage = 
            ((logEntry.stats.filledAfter - logEntry.stats.filledBefore) / 
             logEntry.stats.totalFields * 100).toFixed(2);
        
        // Log to console in a more compact format
        console.log(`\n--- NORMALIZATION [${phase}] - ${after.source_table}:${after.source_id || 'unknown'} ---`);
        console.log(`Fields filled: ${logEntry.stats.filledBefore} → ${logEntry.stats.filledAfter} (${logEntry.stats.improvementPercentage}% improvement)`);
        
        if (logEntry.changes.length > 0) {
            console.log(`Changed ${logEntry.changes.length} fields, including:`);
            // Show just a few important changes
            const keyFields = ['title', 'description', 'sector', 'tender_type', 'estimated_value'];
            const importantChanges = logEntry.changes.filter(change => keyFields.includes(change.field));
            
            importantChanges.forEach(change => {
                let beforeDisplay = typeof change.before === 'string' ? 
                    change.before.substring(0, 50) : 
                    String(change.before);
                
                let afterDisplay = typeof change.after === 'string' ? 
                    change.after.substring(0, 50) : 
                    String(change.after);
                
                if (typeof change.before === 'string' && change.before.length > 50) beforeDisplay += '...';
                if (typeof change.after === 'string' && change.after.length > 50) afterDisplay += '...';
                
                console.log(`  ${change.field}: "${beforeDisplay}" → "${afterDisplay}"`);
            });
        }
        
        // In a production environment, you would write this to a file
        // For now, we'll just update global stats
        updateNormalizationStats(before, after);
        
    } catch (error) {
        console.error('Error in detailed logging:', error);
    }
}

/**
 * Update global statistics for batch reporting
 */
function updateNormalizationStats(before, after) {
    normalizationStats.total++;
    
    // Count fields
    const totalFields = Object.keys(after).filter(k => 
        !['normalized_at', 'normalized_by', 'normalized_method', 'processing_time_ms'].includes(k)
    ).length;
    
    normalizationStats.fieldsProcessed += totalFields;
    
    // Count filled fields before/after
    const filledBefore = Object.values(before).filter(v => 
        v !== null && v !== undefined && v !== ''
    ).length;
    
    const filledAfter = Object.values(after).filter(v => 
        v !== null && v !== undefined && v !== ''
    ).length;
    
    normalizationStats.fieldsFilledBefore += filledBefore;
    normalizationStats.fieldsFilledAfter += filledAfter;
    
    // Track source-specific stats
    const source = after.source_table || 'unknown';
    if (!normalizationStats.sources[source]) {
        normalizationStats.sources[source] = {
            total: 0,
            fieldsFilledBefore: 0,
            fieldsFilledAfter: 0
        };
    }
    
    normalizationStats.sources[source].total++;
    normalizationStats.sources[source].fieldsFilledBefore += filledBefore;
    normalizationStats.sources[source].fieldsFilledAfter += filledAfter;
    
    // Track specific improvements
    if (before.title !== after.title && after.title) normalizationStats.titleChanges++;
    if (!before.sector && after.sector) normalizationStats.sectorAdded++;
    if ((!before.description || before.description !== after.description) && after.description) {
        normalizationStats.descriptionImproved++;
    }
    if ((!before.contact_email && after.contact_email) || 
        (!before.contact_name && after.contact_name) || 
        (!before.contact_phone && after.contact_phone)) {
        normalizationStats.contactInfoAdded++;
    }
    if (!before.estimated_value && after.estimated_value) normalizationStats.valueAdded++;
    
    // Print summary statistics every 100 tenders
    if (normalizationStats.total % 100 === 0) {
        printSummaryStatistics();
    }
}

/**
 * Print summary statistics about normalization effectiveness
 */
function printSummaryStatistics() {
    const elapsedSeconds = (Date.now() - normalizationStats.startTime) / 1000;
    const averageTimePerTender = elapsedSeconds / normalizationStats.total;
    
    console.log('\n========== NORMALIZATION SUMMARY STATISTICS ==========');
    console.log(`Processed ${normalizationStats.total} tenders in ${elapsedSeconds.toFixed(2)}s (${averageTimePerTender.toFixed(3)}s/tender)`);
    
    const beforePercentage = (normalizationStats.fieldsFilledBefore / normalizationStats.fieldsProcessed * 100).toFixed(2);
    const afterPercentage = (normalizationStats.fieldsFilledAfter / normalizationStats.fieldsProcessed * 100).toFixed(2);
    const improvementPercentage = (afterPercentage - beforePercentage).toFixed(2);
    
    console.log(`\nField completion: ${beforePercentage}% → ${afterPercentage}% (${improvementPercentage}% improvement)`);
    console.log(`Fields filled: ${normalizationStats.fieldsFilledBefore} → ${normalizationStats.fieldsFilledAfter} (+${normalizationStats.fieldsFilledAfter - normalizationStats.fieldsFilledBefore})`);
    
    console.log('\nSpecific improvements:');
    console.log(`- Title normalization: ${normalizationStats.titleChanges} tenders`);
    console.log(`- Sector identification: ${normalizationStats.sectorAdded} tenders`);
    console.log(`- Description enhancement: ${normalizationStats.descriptionImproved} tenders`);
    console.log(`- Contact info extraction: ${normalizationStats.contactInfoAdded} tenders`);
    console.log(`- Value extraction: ${normalizationStats.valueAdded} tenders`);
    
    console.log('\nPer-source statistics:');
    Object.entries(normalizationStats.sources).forEach(([source, stats]) => {
        const sourceBefore = (stats.fieldsFilledBefore / (stats.total * 30) * 100).toFixed(2);
        const sourceAfter = (stats.fieldsFilledAfter / (stats.total * 30) * 100).toFixed(2);
        console.log(`- ${source}: ${sourceBefore}% → ${sourceAfter}% (${(sourceAfter - sourceBefore).toFixed(2)}% improvement)`);
    });
    
    console.log('=======================================================\n');
}

/**
 * Log normalization statistics and field changes - replaced with better system
 */
function logNormalizationStats(phase, before, after, note = '') {
    // Use the new detailed logging function instead
    writeDetailedLog(phase, before, after, note);
    
    // Also update tender processing counter
    tenderProcessingCounter++;
}

/**
 * Queries the LLM service with the provided prompt
 * @param {string} prompt - The prompt to send to the LLM
 * @returns {Promise<Object>} The LLM response
 */
async function queryLLM(prompt) {
    // Try to get API keys from Apify first, then environment variables
    let openaiApiKey;
    
    try {
        const apifyEnv = await Actor.getEnv();
        if (apifyEnv && apifyEnv.OPENAI_KEY) {
            openaiApiKey = apifyEnv.OPENAI_KEY;
            console.log('Using OpenAI API key from Apify Key-Value store');
        }
    } catch (error) {
        console.error('Error retrieving OpenAI API key from Apify Key-Value store:', error);
    }
    
    // Fall back to environment variables if Apify didn't provide a key
    if (!openaiApiKey) {
        openaiApiKey = process.env.OPENAI_KEY || process.env.OPENAI_API_KEY;
        console.log('Using OpenAI API key from environment variables');
    }
    
    if (!openaiApiKey) {
        throw new Error('No OpenAI API key found. Please set OPENAI_KEY in environment variables or Apify Key-Value store.');
    }
    
    // Set a timeout for the fetch request
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 120000); // 2 minute timeout
    
    try {
        // Prepare the request to OpenAI API
        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${openaiApiKey}`
            },
            body: JSON.stringify({
                model: 'gpt-4o-mini', // Using gpt-4o-mini as explicitly requested
                messages: [
                    { role: 'user', content: prompt }
                ],
                temperature: 0.2,
                max_tokens: 8192,
                response_format: { type: "json_object" } // Explicitly request JSON format
            }),
            signal: controller.signal
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Error calling OpenAI API: ${response.status} ${response.statusText} - ${errorText}`);
        }
        
        const data = await response.json();
        return data;
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new Error('LLM request timed out after 2 minutes');
        }
        throw error;
    } finally {
        clearTimeout(timeoutId);
    }
}

/**
 * Parses the LLM response to extract the JSON object
 * @param {string} responseText - The text response from the LLM
 * @returns {Object} The parsed JSON object
 */
function parseJSONFromLLMResponse(responseText) {
    try {
        // Try to find JSON between triple backticks
        const jsonMatch = responseText.match(/```(?:json)?\s*([\s\S]*?)\s*```/);
        if (jsonMatch && jsonMatch[1]) {
            try {
                return JSON.parse(jsonMatch[1]);
            } catch (e) {
                console.warn('Could not parse JSON within backticks, attempting repair...');
                // Continue to repair attempts below
            }
        }
        
        // Try to find JSON without backticks
        try {
            return JSON.parse(responseText);
        } catch (e) {
            // If that fails, try to clean the text and parse it
            try {
                const cleanedText = responseText
                    .replace(/^```json\s*/, '')
                    .replace(/\s*```$/, '')
                    .trim();
                
                return JSON.parse(cleanedText);
            } catch (cleanError) {
                console.warn('Could not parse cleaned JSON, attempting repair...');
                // Continue to repair attempts below
            }
        }
        
        // Advanced repair for truncated JSON
        console.log('Attempting to repair potentially truncated JSON');
        
        // Check if the response text starts with a curly brace (JSON object)
        if (responseText.trim().startsWith('{')) {
            // Extract what looks like a JSON object
            const extractedJson = responseText.trim().match(/\{[\s\S]*?(\}|$)/);
            if (extractedJson) {
                let jsonText = extractedJson[0];
                
                // Check if JSON is truncated (doesn't end with })
                if (!jsonText.endsWith('}')) {
                    console.log('JSON appears to be truncated, attempting to complete it');
                    
                    // Count opening and closing braces to check balance
                    const openBraces = (jsonText.match(/\{/g) || []).length;
                    const closeBraces = (jsonText.match(/\}/g) || []).length;
                    
                    // Complete the JSON by adding missing closing braces
                    for (let i = 0; i < openBraces - closeBraces; i++) {
                        jsonText += '}';
                    }
                    
                    // Now check for truncated arrays or strings
                    // Find the last complete property to ensure proper JSON structure
                    const properties = jsonText.match(/"[^"]+"\s*:\s*("[^"]*"|\d+|\{[\s\S]*\}|\[[\s\S]*\]|true|false|null),?/g);
                    
                    if (properties) {
                        // Keep only the text up to the last complete property
                        const validJson = '{' + properties.join(',') + '}';
                        try {
                            return JSON.parse(validJson);
                        } catch (e) {
                            console.warn('Could not parse repaired JSON with properties');
                        }
                    }
                    
                    // If we can't find valid properties, try a simpler approach
                    try {
                        return JSON.parse(jsonText);
                    } catch (e) {
                        console.warn('Could not parse completed JSON');
                    }
                }
            }
        }
        
        // If all attempts to parse JSON fail, try to extract fields we can find
        console.log('All JSON parsing attempts failed, trying to extract fields directly');
        const fields = {};
        
        // Extract common fields using regex
        const extractField = (fieldName) => {
            const regex = new RegExp(`"${fieldName}"\\s*:\\s*"([^"]*)"`, 'i');
            const match = responseText.match(regex);
            if (match && match[1]) {
                fields[fieldName] = match[1];
                return true;
            }
            return false;
        };
        
        // Extract numeric fields
        const extractNumericField = (fieldName) => {
            const regex = new RegExp(`"${fieldName}"\\s*:\\s*(\\d+(?:\\.\\d+)?)`, 'i');
            const match = responseText.match(regex);
            if (match && match[1]) {
                fields[fieldName] = parseFloat(match[1]);
                return true;
            }
            return false;
        };
        
        // Extract boolean fields
        const extractBooleanField = (fieldName) => {
            const regex = new RegExp(`"${fieldName}"\\s*:\\s*(true|false)`, 'i');
            const match = responseText.match(regex);
            if (match && match[1]) {
                fields[fieldName] = match[1].toLowerCase() === 'true';
                return true;
            }
            return false;
        };
        
        // Extract null fields
        const extractNullField = (fieldName) => {
            const regex = new RegExp(`"${fieldName}"\\s*:\\s*null`, 'i');
            const match = responseText.match(regex);
            if (match) {
                fields[fieldName] = null;
                return true;
            }
            return false;
        };
        
        // Try to extract all important fields
        const fieldNames = [
            'title', 'title_english', 'description', 'description_english',
            'tender_type', 'status', 'publication_date', 'deadline_date',
            'country', 'city', 'organization_name', 'organization_name_english',
            'organization_id', 'buyer', 'buyer_english', 'project_name',
            'project_name_english', 'project_id', 'project_number', 'sector',
            'contact_name', 'contact_email', 'contact_phone', 'contact_address',
            'url', 'language', 'notice_id', 'reference_number', 'procurement_method'
        ];
        
        // Extract string fields
        fieldNames.forEach(field => {
            extractField(field) || extractNullField(field);
        });
        
        // Extract numeric fields
        ['estimated_value'].forEach(field => {
            extractNumericField(field) || extractNullField(field);
        });
        
        // Try to extract document_links array
        const linksMatch = responseText.match(/"document_links"\s*:\s*\[(.*?)\]/s);
        if (linksMatch && linksMatch[1]) {
            try {
                // Try to parse the array content
                const linksText = '[' + linksMatch[1] + ']';
                fields.document_links = JSON.parse(linksText);
            } catch (e) {
                // If parsing fails, try to extract individual URLs
                const urls = linksMatch[1].match(/"([^"]*)"/g);
                if (urls) {
                    fields.document_links = urls.map(url => url.replace(/"/g, ''));
                }
            }
        }
        
        // If we found at least some fields, return them
        if (Object.keys(fields).length > 0) {
            console.log(`Extracted ${Object.keys(fields).length} fields from malformed JSON`);
            return fields;
        }
        
        throw new Error('Failed to parse or repair JSON response');
    } catch (error) {
        console.error('Error parsing JSON from LLM response:', error);
        console.error('Response text:', responseText);
        throw new Error('Failed to parse LLM response as JSON');
    }
}

/**
 * Enhances tender titles by standardizing format and removing redundancies
 * @param {Object} normalizedData - The tender data to process
 * @returns {Object} The tender data with improved titles
 */
function enhanceTenderTitles(normalizedData) {
    if (!normalizedData || !normalizedData.title) {
        return normalizedData;
    }
    
    // Clone the input object to preserve the original state for logging
    const originalData = JSON.parse(JSON.stringify(normalizedData));
    
    let title = normalizedData.title;
    let titleEnglish = normalizedData.title_english || '';
    
    // 1. Remove common prefix patterns that don't add value
    const prefixPatterns = [
        /^FORECAST\s*[IVX]*\s*-+\s*/i,   // FORECAST II -
        /^[LR]\s*-+\s*/i,                // L -- or R --
        /^\d+\s*-+\s*/i,                 // 66 --
        /^[A-Z]{1,3}\s*-+\s*/i,          // Other letter prefixes like "A --"
        /^REF[\s.:-]+/i,                 // REF: or REF. or REF -
        /^RFP[\s.:-]+/i,                 // RFP: or RFP. or RFP -
        /^ITB[\s.:-]+/i,                 // ITB: or ITB. or ITB -
        /^NO[\s.:-]+\d+/i,               // NO. 12345 or NO: 12345
        /^[A-Z]\s*--\s*/i,               // Patterns like "R -- "
        /^Sources\s+Sought[:\s-]+/i,     // "Sources Sought:"
        /^Tender[:\s-]+/i,               // "Tender:"
        /^Notice[:\s-]+/i,               // "Notice:"
        /^Solicitation[:\s-]+/i,         // "Solicitation:"
        /^Combined Synopsis[:\s-]+/i,    // "Combined Synopsis:"
        /^Project\s+Title[:\s-]+/i,      // "Project Title:"
        /^Title[:\s-]+/i,                // "Title:"
        /^Brief\s+Description[:\s-]+/i,  // "Brief Description:"
        /^Short\s+Title[:\s-]+/i,        // "Short Title:"
        /^Purchase\s+of[:\s-]+/i,        // "Purchase of:"
        /^Amendment\s+\d+\s+to\s+/i,     // "Amendment 1 to"
        /^Modified\s+/i,                 // "Modified"
        /^Revised\s+/i,                  // "Revised"
        /^Update[d]?\s+/i,               // "Update" or "Updated"
        /^Correction\s+to\s+/i,          // "Correction to"
        /^Re[:\s-]+/i                    // "Re:"
    ];
    
    for (const pattern of prefixPatterns) {
        title = title.replace(pattern, '');
        if (titleEnglish) {
            titleEnglish = titleEnglish.replace(pattern, '');
        }
    }
    
    // 2. Clean up titles that are just abbreviations or codes in parentheses
    title = title.replace(/\s*\([A-Z]{2,5}\)\s*$/i, '');
    title = title.replace(/\(\d+\)\s*$/i, ''); // Remove number in parentheses at end
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\s*\([A-Z]{2,5}\)\s*$/i, '');
        titleEnglish = titleEnglish.replace(/\(\d+\)\s*$/i, '');
    }

    // 3. Move reference numbers to the end of the title
    const refNumberPatterns = [
        /\b([A-Z]{2,4}-\d{2,}-\d{2,})\b/g,     // Format like "ICB-20-12345"
        /\b(\d{5,})\b/g,                        // Random long numbers
        /\b([A-Z]{2,}\d{4,})\b/g,               // Format like "ABC12345"
        /\b([A-Z]{1,3}\/\d{2,}\/\d{2,})\b/g,    // Format like "A/12/345"
        /\b(RFx\d{4,})\b/gi,                    // Format like "RFx12345"
        /\b(RFQ-\d{4,})\b/gi,                   // Format like "RFQ-12345"
        /\b(RFP-\d{4,})\b/gi                    // Format like "RFP-12345"
    ];
    
    let refNumbers = [];
    
    for (const pattern of refNumberPatterns) {
        const matches = [...title.matchAll(pattern)];
        if (matches.length > 0) {
            refNumbers = [...refNumbers, ...matches.map(m => m[0])];
            title = title.replace(pattern, ' ');
        }
    }
    
    // 4. Fix ALL CAPS titles - this is critical for readability
    if (title === title.toUpperCase() && title.length > 10) {
        // Convert to Title Case (capitalize first letter of each word)
        title = title.toLowerCase().replace(/\b\w+/g, word => {
            // Skip short conjunctions, articles, and prepositions unless they're the first word
            const minorWords = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of', 'with', 'under', 'above', 'between', 'among'];
            return minorWords.includes(word) ? word : word.charAt(0).toUpperCase() + word.slice(1);
        });
        
        // Ensure first word is always capitalized
        title = title.charAt(0).toUpperCase() + title.slice(1);
    }
    
    if (titleEnglish === titleEnglish.toUpperCase() && titleEnglish.length > 10) {
        titleEnglish = titleEnglish.toLowerCase().replace(/\b\w+/g, word => {
            const minorWords = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of', 'with', 'under', 'above', 'between', 'among'];
            return minorWords.includes(word) ? word : word.charAt(0).toUpperCase() + word.slice(1);
        });
        titleEnglish = titleEnglish.charAt(0).toUpperCase() + titleEnglish.slice(1);
    }
    
    // 5. Expand common acronyms for better readability
    const acronymMap = {
        'ICB': 'International Competitive Bidding for',
        'NCB': 'National Competitive Bidding for',
        'ICT': 'Information and Communication Technology',
        'IT': 'Information Technology',
        'RFP': 'Request for Proposal:',
        'EOI': 'Expression of Interest:',
        'USAID': 'US Agency for International Development',
        'USPSC': 'US Personal Services Contractor',
        'IQC': 'Indefinite Quantity Contract',
        'SOL': '',  // Often just a prefix to a solicitation number
        'PSC': 'Personal Services Contractor',
        'IDIQ': 'Indefinite Delivery/Indefinite Quantity',
        'FSN': 'Foreign Service National',
        'COP': 'Chief of Party',
        'PAD': 'Project Appraisal Document',
        'RFQ': 'Request for Quotation:',
        'ITB': 'Invitation to Bid:',
        'PMSC': 'Project Management Services Contractor',
        'SME': 'Small and Medium Enterprise',
        'CSO': 'Civil Society Organization',
        'NGO': 'Non-Governmental Organization',
        'UNDP': 'United Nations Development Programme',
        'ADB': 'Asian Development Bank',
        'WB': 'World Bank',
        'EPC': 'Engineering, Procurement, and Construction',
        'HVAC': 'Heating, Ventilation, and Air Conditioning',
        'MOU': 'Memorandum of Understanding',
        'CLIN': 'Contract Line Item Number',
        'GSA': 'General Services Administration',
        'MRO': 'Maintenance, Repair, and Operations',
        'WASH': 'Water, Sanitation, and Hygiene',
        'PPE': 'Personal Protective Equipment',
        'VAT': 'Value Added Tax'
    };
    
    // Only expand acronyms if they are standalone words (surrounded by spaces or at start/end)
    for (const [acronym, expansion] of Object.entries(acronymMap)) {
        const regex = new RegExp(`\\b${acronym}\\b`, 'g');
        
        // Only replace if the acronym is a standalone word and not part of a larger word
        if (regex.test(title)) {
            title = title.replace(regex, expansion);
        }
        
        if (titleEnglish && regex.test(titleEnglish)) {
            titleEnglish = titleEnglish.replace(regex, expansion);
        }
    }
    
    // 6. Remove redundancies in title (repeated words)
    title = title.replace(/\b(\w+)(\s+\1\b)+/gi, '$1');
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\b(\w+)(\s+\1\b)+/gi, '$1');
    }
    
    // 7. Fix titles that are truncated with ellipsis by removing the ellipsis
    title = title.replace(/\.{3,}$/g, '');
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\.{3,}$/g, '');
    }
    
    // 8. Remove very common tender-related generic words at the beginning
    const genericPrefixWords = [
        /^Supply of\s+/i,
        /^Provision of\s+/i,
        /^Procurement of\s+/i,
        /^Purchase of\s+/i,
        /^Tender for\s+/i,
        /^Contract for\s+/i
    ];
    
    // Only remove these if the remaining title is substantial (more than 5 words)
    if (title.split(/\s+/).length > 5) {
        for (const pattern of genericPrefixWords) {
            title = title.replace(pattern, '');
            if (titleEnglish) {
                titleEnglish = titleEnglish.replace(pattern, '');
            }
        }
    }
    
    // 9. Trim extra spaces
    title = title.replace(/\s{2,}/g, ' ').trim();
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\s{2,}/g, ' ').trim();
    }
    
    // 10. Add reference numbers at the end if available
    if (refNumbers.length > 0 && normalizedData.reference_number) {
        // Use the official reference number instead of extracted ones
        if (!title.includes(normalizedData.reference_number)) {
            title = `${title} (${normalizedData.reference_number})`;
        }
    } else if (refNumbers.length > 0) {
        // If no official reference number, use extracted ones
        title = `${title} (${refNumbers.join(' ')})`;
    }
    
    // 11. Ensure the first letter of the title is capitalized
    if (title && title.length > 0) {
        title = title.charAt(0).toUpperCase() + title.slice(1);
    }
    
    if (titleEnglish && titleEnglish.length > 0) {
        titleEnglish = titleEnglish.charAt(0).toUpperCase() + titleEnglish.slice(1);
    }
    
    // 12. Ensure title doesn't end with punctuation
    title = title.replace(/[.,;:-]+$/, '');
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/[.,;:-]+$/, '');
    }
    
    // 13. Fix the casing of specific keywords that should always be capitalized
    const alwaysCapitalizeWords = ['UN', 'US', 'UK', 'EU', 'COVID', 'COVID-19', 'IT', 'IoT', 'AI', 'API', 'ICT', 'SMS'];
    
    for (const word of alwaysCapitalizeWords) {
        const regex = new RegExp(`\\b${word.toLowerCase()}\\b`, 'g');
        title = title.replace(regex, word);
        if (titleEnglish) {
            titleEnglish = titleEnglish.replace(regex, word);
        }
    }
    
    // Update the normalized data with the enhanced titles
    normalizedData.title = title;
    if (titleEnglish) {
        normalizedData.title_english = titleEnglish;
    } else if (isEnglishText(title)) {
        // If the main title is English and we don't have a separate English title, copy it
        normalizedData.title_english = title;
    }
    
    // Log changes for a sample of tenders using the improved logging
    logNormalizationStats('Title Enhancement', originalData, normalizedData, 
        `Title was ${originalData.title ? 'transformed' : 'missing'}`);
    
    return normalizedData;
}

/**
 * Fills missing fields in the normalized data based on available information
 * @param {Object} normalizedData - The tender data to enhance
 * @returns {Object} Enhanced tender data with filled fields
 */
function fillMissingFields(normalizedData) {
    if (!normalizedData) return normalizedData;
    
    // Clone the input object to preserve the original state for logging
    const originalData = JSON.parse(JSON.stringify(normalizedData));
    
    // Track which fields were filled by this function
    const fieldsFilledByFunction = [];
    
    // 1. If we have title but no title_english and the title is in English
    if (normalizedData.title && !normalizedData.title_english && isEnglishText(normalizedData.title)) {
        normalizedData.title_english = normalizedData.title;
        fieldsFilledByFunction.push('title_english');
    }
    
    // 2. If we have description but no description_english and the description is in English
    if (normalizedData.description && !normalizedData.description_english && isEnglishText(normalizedData.description)) {
        normalizedData.description_english = normalizedData.description;
        fieldsFilledByFunction.push('description_english');
    }
    
    // 3. If organization_name is missing but we have organization_id
    if (!normalizedData.organization_name && normalizedData.organization_id) {
        // Map common organization IDs to names
        const orgIdMap = {
            '100171790': 'USAID (US Agency for International Development)',
            '100175108': 'USAID East Africa',
            '100175112': 'USAID Southern Africa',
            '100181493': 'USAID Afghanistan',
            '100184904': 'USAID Kosovo',
            '100187934': 'USAID Sudan',
            '100000000': 'World Bank',
            '100000001': 'Asian Development Bank',
            '100000002': 'European Union',
            '100000003': 'United Nations Development Programme'
        };
        
        if (orgIdMap[normalizedData.organization_id]) {
            normalizedData.organization_name = orgIdMap[normalizedData.organization_id];
            fieldsFilledByFunction.push('organization_name');
            
            normalizedData.organization_name_english = orgIdMap[normalizedData.organization_id];
            fieldsFilledByFunction.push('organization_name_english');
        }
    }
    
    // 4. If we have organization_name but no organization_name_english and org name is in English
    if (normalizedData.organization_name && !normalizedData.organization_name_english && 
        isEnglishText(normalizedData.organization_name)) {
        normalizedData.organization_name_english = normalizedData.organization_name;
        fieldsFilledByFunction.push('organization_name_english');
    }
    
    // 5. If status is missing but we have deadline_date
    if (!normalizedData.status && normalizedData.deadline_date) {
        const deadlineDate = new Date(normalizedData.deadline_date);
        const now = new Date();
        normalizedData.status = deadlineDate > now ? 'Open' : 'Closed';
        fieldsFilledByFunction.push('status');
    }
    
    // 6. Extract buyer from description if missing
    if (!normalizedData.buyer && normalizedData.description) {
        // Look for patterns indicating a buyer in the description
        const buyerPatterns = [
            /(?:issued by|purchaser|buyer|procuring entity|client|contracting authority|awarded to|contractor)[\s:]*([A-Za-z0-9\s.,&'()-]+?)(?:\.|,|\n|$)/i,
            /(?:on behalf of|for the)[\s:]*([A-Za-z0-9\s.,&'()-]+?)(?:\.|,|\n|$)/i
        ];
        
        for (const pattern of buyerPatterns) {
            const match = normalizedData.description.match(pattern);
            if (match && match[1] && match[1].length > 3 && match[1].length < 100) {
                normalizedData.buyer = match[1].trim();
                fieldsFilledByFunction.push('buyer');
                
                if (isEnglishText(normalizedData.buyer)) {
                    normalizedData.buyer_english = normalizedData.buyer;
                    fieldsFilledByFunction.push('buyer_english');
                }
                break;
            }
        }
    }
    
    // 7. Set normalized_at if it's missing
    if (!normalizedData.normalized_at) {
        normalizedData.normalized_at = new Date().toISOString();
    }
    
    // 8. If project_name is missing but can be extracted from description
    if (!normalizedData.project_name && normalizedData.description) {
        const projectPatterns = [
            /project name[\s:]*([A-Za-z0-9\s.,'"()-]+?)(?:\.|,|\n|$)/i,
            /project[\s:]*([A-Za-z0-9\s.,'"()-]+?)(?:\.|,|\n|$)/i,
            /program[\s:]*([A-Za-z0-9\s.,'"()-]+?)(?:\.|,|\n|$)/i,
            /contract for[\s:]*([A-Za-z0-9\s.,'"()-]+?)(?:\.|,|\n|$)/i,
            /related to[\s:]*([A-Za-z0-9\s.,'"()-]+?)(?:\.|,|\n|$)/i
        ];
        
        for (const pattern of projectPatterns) {
            const match = normalizedData.description.match(pattern);
            if (match && match[1] && match[1].length > 3 && match[1].length < 100) {
                normalizedData.project_name = match[1].trim();
                if (isEnglishText(normalizedData.project_name)) {
                    normalizedData.project_name_english = normalizedData.project_name;
                }
                break;
            }
        }
    }
    
    // 9. Extract sector information if missing
    if (!normalizedData.sector && normalizedData.description) {
        // Common sectors that might be mentioned in the description
        const sectorKeywords = {
            'Information Technology': ['IT', 'software', 'hardware', 'computer', 'technology', 'digital', 'cloud', 'data center', 'system integration', 'ERP', 'SAP', 'AI', 'artificial intelligence', 'machine learning', 'database'],
            'Healthcare': ['health', 'medical', 'hospital', 'clinic', 'pharmaceutical', 'drug', 'healthcare', 'medicine', 'patient', 'doctor', 'nurse', 'diagnostic', 'treatment'],
            'Construction': ['construction', 'building', 'infrastructure', 'road', 'bridge', 'dam', 'highway', 'renovation', 'civil works', 'contractor', 'concrete', 'asphalt', 'engineering'],
            'Education': ['education', 'school', 'university', 'college', 'training', 'learning', 'academic', 'student', 'teacher', 'educational', 'curriculum', 'classroom'],
            'Agriculture': ['agriculture', 'farming', 'crop', 'livestock', 'irrigation', 'farm', 'agricultural', 'food', 'seed', 'fertilizer', 'harvesting', 'plantation'],
            'Energy': ['energy', 'power', 'electricity', 'renewable', 'solar', 'wind', 'hydroelectric', 'fossil fuel', 'coal', 'gas', 'oil', 'nuclear', 'grid', 'transmission'],
            'Transportation': ['transport', 'logistics', 'shipping', 'freight', 'rail', 'railway', 'airport', 'port', 'vessel', 'car', 'truck', 'bus', 'metro', 'transit'],
            'Telecommunications': ['telecom', 'communication', 'network', 'cellular', 'mobile', 'fiber optic', 'broadband', 'internet', 'wireless', '5G', '4G', 'LTE', 'cable'],
            'Financial Services': ['financial', 'banking', 'insurance', 'investment', 'finance', 'loan', 'credit', 'bank', 'capital', 'fund', 'pension', 'accounting', 'audit'],
            'Environmental': ['environment', 'environmental', 'conservation', 'sustainability', 'waste', 'pollution', 'recycling', 'climate', 'green', 'eco', 'biodiversity', 'wastewater'],
            'Water & Sanitation': ['water', 'sanitation', 'sewage', 'plumbing', 'drainage', 'potable', 'clean water', 'drinking water', 'pump', 'pipe', 'WASH', 'hygiene'],
            'Defense & Security': ['defense', 'security', 'military', 'weapon', 'surveillance', 'protection', 'police', 'intelligence', 'radar', 'armor', 'combat', 'cyber'],
            'Mining': ['mining', 'mineral', 'ore', 'extraction', 'quarry', 'coal', 'gold', 'silver', 'copper', 'excavation', 'drill'],
            'Manufacturing': ['manufacturing', 'factory', 'industrial', 'assembly', 'production', 'machinery', 'equipment', 'fabrication', 'processing'],
            'Retail & Consumer Goods': ['retail', 'consumer', 'merchandise', 'goods', 'product', 'store', 'ecommerce', 'supply chain'],
            'Tourism & Hospitality': ['tourism', 'hospitality', 'hotel', 'restaurant', 'travel', 'leisure', 'accommodation', 'tourist']
        };
        
        // Find sector keywords in the description
        const description = normalizedData.description.toLowerCase();
        const title = normalizedData.title ? normalizedData.title.toLowerCase() : '';
        
        let bestSector = null;
        let highestCount = 0;
        
        Object.entries(sectorKeywords).forEach(([sector, keywords]) => {
            let count = 0;
            keywords.forEach(keyword => {
                const regex = new RegExp(`\\b${keyword}\\b`, 'ig');
                
                // Check description
                const descMatches = description.match(regex);
                if (descMatches) count += descMatches.length;
                
                // Check title with higher weight (x2)
                if (title) {
                    const titleMatches = title.match(regex);
                    if (titleMatches) count += titleMatches.length * 2;
                }
            });
            
            if (count > highestCount) {
                highestCount = count;
                bestSector = sector;
            }
        });
        
        // Only set sector if we have a reasonable confidence (at least 2 mentions or 1 in title)
        if (highestCount >= 2) {
            normalizedData.sector = bestSector;
        }
    }
    
    // 10. Extract country from other fields if missing
    if (!normalizedData.country && normalizedData.description) {
        // Look for common country mentions in the description
        const commonCountries = [
            'Afghanistan', 'Albania', 'Algeria', 'Angola', 'Argentina', 'Armenia', 'Australia', 'Austria', 
            'Azerbaijan', 'Bangladesh', 'Belgium', 'Benin', 'Bolivia', 'Bosnia', 'Botswana', 'Brazil', 
            'Bulgaria', 'Burkina Faso', 'Burundi', 'Cambodia', 'Cameroon', 'Canada', 'Chad', 'Chile', 
            'China', 'Colombia', 'Congo', 'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 
            'Denmark', 'Dominican Republic', 'DR Congo', 'Ecuador', 'Egypt', 'El Salvador', 'Estonia', 
            'Ethiopia', 'Finland', 'France', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Guatemala', 
            'Guinea', 'Haiti', 'Honduras', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 
            'Ireland', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kosovo', 
            'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 
            'Lithuania', 'Madagascar', 'Malawi', 'Malaysia', 'Mali', 'Mauritania', 'Mexico', 'Moldova', 
            'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia', 'Nepal', 
            'Netherlands', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria', 'North Macedonia', 'Norway', 
            'Pakistan', 'Palestine', 'Panama', 'Papua New Guinea', 'Paraguay', 'Peru', 'Philippines', 
            'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia', 'Rwanda', 'Saudi Arabia', 'Senegal', 
            'Serbia', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Somalia', 'South Africa', 
            'South Korea', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Sweden', 'Switzerland', 
            'Syria', 'Taiwan', 'Tajikistan', 'Tanzania', 'Thailand', 'Timor-Leste', 'Togo', 'Tunisia', 
            'Turkey', 'Turkmenistan', 'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 
            'United States', 'USA', 'Uruguay', 'Uzbekistan', 'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 
            'Zimbabwe'
        ];
        
        // Create a regex pattern for all countries
        const countriesPattern = new RegExp(`\\b(${commonCountries.join('|')})\\b`, 'i');
        
        // Search in description
        const match = normalizedData.description.match(countriesPattern);
        if (match && match[1]) {
            normalizedData.country = match[1];
        }
    }
    
    // 11. Extract reference_number if missing
    if (!normalizedData.reference_number) {
        // Check title and description for common reference number patterns
        const refNumberPatterns = [
            /\b([A-Z]{2,4}-\d{2,}-\d{2,})\b/,        // Format like "ICB-20-12345"
            /\bRef(?:erence)?\s*(?:No)?[:.]\s*([A-Z0-9-/]+)/i,  // Format like "Ref. ABC/123"
            /\bRFP\s*(?:No)?[:.]\s*([A-Z0-9-/]+)/i,  // Format like "RFP No. 12345"
            /\bRFQ\s*(?:No)?[:.]\s*([A-Z0-9-/]+)/i,  // Format like "RFQ No. 12345"
            /\bITB\s*(?:No)?[:.]\s*([A-Z0-9-/]+)/i,  // Format like "ITB No. 12345"
            /\bNo\.\s*([A-Z0-9-/]+)/i,               // Format like "No. ABC123"
            /\bTender ID[:.]\s*([A-Z0-9-/]+)/i,      // Format like "Tender ID: 12345"
            /\bNotice ID[:.]\s*([A-Z0-9-/]+)/i,      // Format like "Notice ID: 12345"
            /\bProject ID[:.]\s*([A-Z0-9-/]+)/i      // Format like "Project ID: 12345"
        ];
        
        // Check title first
        if (normalizedData.title) {
            for (const pattern of refNumberPatterns) {
                const match = normalizedData.title.match(pattern);
                if (match && match[1]) {
                    normalizedData.reference_number = match[1];
                    break;
                }
            }
        }
        
        // If still not found, check description
        if (!normalizedData.reference_number && normalizedData.description) {
            for (const pattern of refNumberPatterns) {
                const match = normalizedData.description.match(pattern);
                if (match && match[1]) {
                    normalizedData.reference_number = match[1];
                    break;
                }
            }
        }
    }
    
    // 12. Extract contact information from description if missing
    if ((!normalizedData.contact_email || !normalizedData.contact_name) && normalizedData.description) {
        // Email extraction
        if (!normalizedData.contact_email) {
            const emailRegex = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g;
            const emailMatches = normalizedData.description.match(emailRegex);
            if (emailMatches && emailMatches.length > 0) {
                normalizedData.contact_email = emailMatches[0];
            }
        }
        
        // Contact name extraction
        if (!normalizedData.contact_name) {
            const contactPatterns = [
                /contact person[\s:]*([A-Za-z0-9\s.,'()-]+?)(?:\.|,|\n|$)/i,
                /contact[\s:]*([A-Za-z0-9\s.,'()-]+?)(?:\.|,|\n|$)/i,
                /(?:attention|attn)[\s:]*([A-Za-z0-9\s.,'()-]+?)(?:\.|,|\n|$)/i,
                /for (?:more )?(?:information|details)[,\s]+(?:please )?contact[\s:]*([A-Za-z0-9\s.,'()-]+?)(?:\.|,|\n|$)/i
            ];
            
            for (const pattern of contactPatterns) {
                const match = normalizedData.description.match(pattern);
                if (match && match[1] && match[1].length > 3 && match[1].length < 50) {
                    normalizedData.contact_name = match[1].trim();
                    break;
                }
            }
        }
        
        // Phone number extraction
        if (!normalizedData.contact_phone) {
            const phonePatterns = [
                /(?:phone|tel|telephone)[\s:]*([+0-9\s.()-]{7,20})(?:\.|,|\n|$)/i,
                /\b(?:[+]?[0-9]{1,3}[-. ]?)?[(]?[0-9]{3}[)]?[-. ]?[0-9]{3}[-. ]?[0-9]{4}\b/g
            ];
            
            for (const pattern of phonePatterns) {
                const match = normalizedData.description.match(pattern);
                if (match && match[1] && match[1].length > 6) {
                    normalizedData.contact_phone = match[1].trim();
                    break;
                } else if (match && match[0] && match[0].length > 6) {
                    normalizedData.contact_phone = match[0].trim();
                    break;
                }
            }
        }
    }
    
    // 13. Extract tender_type from title and description if missing
    if (!normalizedData.tender_type) {
        const tenderTypeKeywords = {
            'Request for Proposal (RFP)': ['request for proposal', 'rfp'],
            'Request for Quotation (RFQ)': ['request for quotation', 'rfq', 'price quotation', 'quote'],
            'Invitation to Bid (ITB)': ['invitation to bid', 'itb', 'bidding'],
            'Expression of Interest (EOI)': ['expression of interest', 'eoi', 'interest'],
            'Request for Information (RFI)': ['request for information', 'rfi'],
            'Pre-Qualification': ['pre-qualification', 'prequalification'],
            'Direct Contract': ['direct contract', 'direct award', 'sole source'],
            'Framework Agreement': ['framework', 'framework agreement', 'indefinite delivery'],
            'Construction Contract': ['construction contract', 'works contract', 'civil works'],
            'Service Contract': ['service contract', 'services'],
            'Supply Contract': ['supply contract', 'supplies', 'goods'],
            'Consulting Services': ['consulting', 'consultant']
        };
        
        let bestMatch = null;
        let highestScore = 0;
        
        const titleText = normalizedData.title ? normalizedData.title.toLowerCase() : '';
        const descText = normalizedData.description ? normalizedData.description.toLowerCase() : '';
        
        Object.entries(tenderTypeKeywords).forEach(([tenderType, keywords]) => {
            let score = 0;
            
            keywords.forEach(keyword => {
                // Check title (higher weight)
                if (titleText.includes(keyword)) {
                    score += 3;
                }
                
                // Check description
                if (descText.includes(keyword)) {
                    score += 1;
                }
            });
            
            if (score > highestScore) {
                highestScore = score;
                bestMatch = tenderType;
            }
        });
        
        if (bestMatch) {
            normalizedData.tender_type = bestMatch;
        }
    }
    
    // 14. Extract or estimate value and currency if missing
    if (!normalizedData.estimated_value && normalizedData.description) {
        const moneyPatterns = [
            /(?:budget|value|amount|contract value|estimated value|cost|price)[\s:]*(?:is|of|:)?\s*(?:USD|EUR|GBP|Rs\.?|₹|£|€|\$)?\s*([0-9,.]+)\s*(?:million|m|billion|b)?\s*(?:USD|EUR|GBP|Rs\.?|₹|£|€|\$)?/i,
            /(?:USD|EUR|GBP|Rs\.?|₹|£|€|\$)\s*([0-9,.]+)\s*(?:million|m|billion|b)?/i
        ];
        
        for (const pattern of moneyPatterns) {
            const match = normalizedData.description.match(pattern);
            if (match && match[1]) {
                let value = match[1].replace(/,/g, '');
                let multiplier = 1;
                
                // Check for million/billion
                if (match[0].toLowerCase().includes('million') || match[0].toLowerCase().includes('m')) {
                    multiplier = 1000000;
                } else if (match[0].toLowerCase().includes('billion') || match[0].toLowerCase().includes('b')) {
                    multiplier = 1000000000;
                }
                
                normalizedData.estimated_value = parseFloat(value) * multiplier;
                
                // Try to extract currency
                const currencySymbols = {
                    '$': 'USD',
                    '€': 'EUR',
                    '£': 'GBP',
                    '₹': 'INR',
                    'Rs': 'INR',
                    'USD': 'USD',
                    'EUR': 'EUR',
                    'GBP': 'GBP'
                };
                
                Object.entries(currencySymbols).forEach(([symbol, currency]) => {
                    if (match[0].includes(symbol)) {
                        normalizedData.currency = currency;
                    }
                });
                
                break;
            }
        }
    }
    
    // 15. Ensure city field has proper capitalization
    if (normalizedData.city && typeof normalizedData.city === 'string') {
        normalizedData.city = normalizedData.city.replace(/\b\w/g, c => c.toUpperCase());
    }
    
    // 16. Ensure country field has proper capitalization
    if (normalizedData.country && typeof normalizedData.country === 'string') {
        // Special handling for countries with specific capitalization
        const specialCountries = {
            'usa': 'USA',
            'uk': 'UK',
            'uae': 'UAE',
            'united states': 'United States',
            'united states of america': 'United States of America',
            'united kingdom': 'United Kingdom',
            'united arab emirates': 'United Arab Emirates'
        };
        
        const lowercaseCountry = normalizedData.country.toLowerCase();
        if (specialCountries[lowercaseCountry]) {
            normalizedData.country = specialCountries[lowercaseCountry];
        } else {
            normalizedData.country = normalizedData.country.replace(/\b\w/g, c => c.toUpperCase());
        }
    }
    
    // At the end of the function, log the changes with improved logging
    logNormalizationStats('Field Filling', originalData, normalizedData, 
        fieldsFilledByFunction.length > 0 ? 
        `Filled ${fieldsFilledByFunction.length} fields: ${fieldsFilledByFunction.join(', ')}` :
        'No fields were filled');
    
    return normalizedData;
}

/**
 * Rule-based tender normalization that works without LLM
 * This is used as a fallback when the LLM service is unavailable
 * 
 * @param {Object} tenderData - Raw tender data from source
 * @param {string} sourceTable - Name of the source table
 * @returns {Object} Basic normalized tender data
 */
function fallbackNormalizeTender(tenderData, sourceTable) {
    console.log(`Using fallback normalization for ${sourceTable} tender`);
    
    // Create a base normalized object with the same fields as LLM output
    const normalizedTender = {
        title: tenderData.title || tenderData.name || null,
        description: tenderData.description || tenderData.details || null,
        tender_type: extractTenderType(tenderData, sourceTable),
        status: extractTenderStatus(tenderData, sourceTable),
        publication_date: tenderData.publication_date || tenderData.publicationDate || tenderData.published_date || null,
        deadline_date: tenderData.deadline_date || tenderData.deadlineDate || tenderData.closing_date || null,
        country: tenderData.country || tenderData.countryName || null,
        city: tenderData.city || tenderData.cityName || null,
        organization_name: tenderData.organization_name || tenderData.organizationName || tenderData.agency || null,
        organization_id: tenderData.organization_id || tenderData.organizationId || null,
        buyer: tenderData.buyer || tenderData.buyerName || null,
        project_name: tenderData.project_name || tenderData.projectName || null,
        project_id: tenderData.project_id || tenderData.projectId || null,
        project_number: tenderData.project_number || tenderData.projectNumber || null,
        sector: tenderData.sector || tenderData.sectorName || null,
        estimated_value: extractNumericValue(tenderData.estimated_value || tenderData.estimatedValue || tenderData.value || null),
        currency: tenderData.currency || null,
        contact_name: tenderData.contact_name || tenderData.contactName || null,
        contact_email: tenderData.contact_email || tenderData.contactEmail || null,
        contact_phone: tenderData.contact_phone || tenderData.contactPhone || null,
        contact_address: tenderData.contact_address || tenderData.contactAddress || null,
        url: tenderData.url || tenderData.noticeUrl || tenderData.tenderUrl || null,
        // Handle document links with consideration for different formats
        document_links: extractDocumentLinks(tenderData),
        language: tenderData.language || 'en',  // Default to English
        notice_id: tenderData.notice_id || tenderData.noticeId || tenderData.id || null,
        reference_number: tenderData.reference_number || tenderData.referenceNumber || tenderData.solicitation_number || null,
        procurement_method: tenderData.procurement_method || tenderData.procurementMethod || null,
        // Include source information for traceability
        source_table: sourceTable,
        source_id: tenderData.id || tenderData.tender_id || tenderData.notice_id || null,
    };
    
    // Handle source-specific formatting
    switch (sourceTable) {
        case 'sam_gov':
            // For SAM.gov tenders, add specific handling
            if (tenderData.original_data) {
                // Extract organization name if needed
                if (!normalizedTender.organization_name && tenderData.original_data.org_key) {
                    normalizedTender.organization_name = `Organization ID: ${tenderData.original_data.org_key}`;
                }
                
                // Extract contacts
                if (tenderData.original_data.contacts && tenderData.original_data.contacts.length > 0) {
                    const primaryContact = tenderData.original_data.contacts.find(c => c.contact_type === 'primary') || 
                                          tenderData.original_data.contacts[0];
                    
                    if (primaryContact) {
                        normalizedTender.contact_name = primaryContact.full_name || normalizedTender.contact_name;
                        normalizedTender.contact_email = primaryContact.email || normalizedTender.contact_email;
                        normalizedTender.contact_phone = primaryContact.phone || normalizedTender.contact_phone;
                    }
                }
                
                // Extract tender type
                if (!normalizedTender.tender_type && tenderData.original_data.opportunity_type) {
                    normalizedTender.tender_type = tenderData.original_data.opportunity_type;
                }
                
                // Extract reference number
                if (!normalizedTender.reference_number && tenderData.original_data.solicitation_number) {
                    normalizedTender.reference_number = tenderData.original_data.solicitation_number;
                }
                
                // Set country as UNITED STATES if it's a SAM.gov tender
                normalizedTender.country = normalizedTender.country || 'UNITED STATES';
            }
            break;
            
        case 'wb':
            // For World Bank tenders, add specific handling
            if (!normalizedTender.organization_name) {
                normalizedTender.organization_name = 'World Bank';
            }
            if (!normalizedTender.organization_name_english) {
                normalizedTender.organization_name_english = 'World Bank';
            }
            break;
            
        case 'adb':
            // For Asian Development Bank tenders, add specific handling
            if (!normalizedTender.organization_name) {
                normalizedTender.organization_name = 'Asian Development Bank';
            }
            if (!normalizedTender.organization_name_english) {
                normalizedTender.organization_name_english = 'Asian Development Bank';
            }
            break;
    }
    
    // Now apply our enhanced title normalization
    const enhancedTender = enhanceTenderTitles(normalizedTender);
    
    // Fill in any remaining missing fields
    const fullyEnhancedTender = fillMissingFields(enhancedTender);
    
    // Make sure we have normalized_at timestamp
    if (!fullyEnhancedTender.normalized_at) {
        fullyEnhancedTender.normalized_at = new Date().toISOString();
    }
    
    // Set normalized_by field to indicate fallback normalization
    fullyEnhancedTender.normalized_by = 'rule-based-fallback';
    
    return fullyEnhancedTender;
}

/**
 * Determines if a tender requires LLM-based normalization or can use faster parsing methods
 * @param {Object} tender - The tender data to evaluate
 * @param {string} sourceTable - The source table name
 * @returns {Object} Decision object with needsLLM boolean and reason string
 */
function evaluateNormalizationNeeds(tender, sourceTable) {
    // Initialize with default assumption that LLM is needed
    const result = {
        needsLLM: true,
        reason: "Default processing path"
    };

    // Fast path for World Bank tenders - immediately return without further checks
    if (sourceTable === "wb") {
        result.needsLLM = false;
        result.reason = "World Bank tenders use fast normalization for performance";
        return result;
    }
    
    // Fast path for ADB tenders - immediately return without further checks
    if (sourceTable === "adb") {
        result.needsLLM = false;
        result.reason = "ADB tenders use fast normalization to prevent timeouts";
        return result;
    }
    
    // Fast path for AFD tenders - immediately return without further checks
    if (sourceTable === "afd_tenders") {
        result.needsLLM = false;
        result.reason = "AFD tenders use fast normalization to prevent timeouts";
        return result;
    }

    // 1. Source-based rules - expand to cover more sources
    if (sourceTable === "sam_gov") {
        // SAM.gov tenders are in English and well-structured
        result.needsLLM = false;
        result.reason = "SAM.gov tenders don't require translation or complex normalization";
    } 
    else if (sourceTable === "un_procurement") {
        // UN tenders are in English and have consistent structure
        result.needsLLM = false;
        result.reason = "UN tenders are in English with consistent structure";
    }
    else if (sourceTable === "dgmarket" && tender.language === "en") {
        // English DGMarket tenders can use fast processing
        result.needsLLM = false;
        result.reason = "English DGMarket tenders can use direct parsing";
    }
    else if (sourceTable === "iadb") {
        // IADB tenders are typically in English/Spanish with consistent structure
        if (tender.language === "en" || (tender.project_name && isEnglishText(tender.project_name))) {
            result.needsLLM = false;
            result.reason = "English IADB tenders can use direct parsing";
        }
    }
    else if (sourceTable === "ted") {
        // TED tenders often have standardized multilingual format
        // If the tender already has both the original and English fields, use fast processing
        if ((tender.title && tender.title_english) || 
            (tender.title && isEnglishText(tender.title))) {
            result.needsLLM = false;
            result.reason = "TED tender with English content can use direct parsing";
        }
    }

    // 2. Completeness-based rules - if the tender already has most critical fields filled
    if (result.needsLLM) {  // Only check if not already decided
        const missingFields = checkForMissingCriticalFields(tender);
        
        // If tender has most important fields already, skip LLM
        if (missingFields.length <= 2) {
            // Tender is nearly complete, just do basic normalization
            result.needsLLM = false;
            result.reason = `Tender already has most critical fields (missing only ${missingFields.length})`;
        }
        // If tender has many fields missing but is in English, still consider direct parsing
        else if (missingFields.length > 2 && missingFields.length <= 5 && 
                (tender.language === 'en' || 
                 (tender.title && isEnglishText(tender.title)) || 
                 (tender.description && isEnglishText(tender.description)))) {
            result.needsLLM = false;
            result.reason = "English tender with some missing fields can use direct parsing";
        }
    }

    // 3. Language-detection enhancement - skip LLM for any English content regardless of source
    // Note: We explicitly set needsLLM to false here
    if (result.needsLLM && (  // Only check if not already decided
        tender.language === 'en' || 
        (tender.title && isStronglyEnglish(tender.title)) || 
        (tender.description && tender.description.length > 100 && isStronglyEnglish(tender.description))
    )) {
        result.needsLLM = false;
        result.reason = "Content is strongly identified as English, using direct parsing";
    }

    // 4. Size-based rules - retained from the original function
    if (tender.description && tender.description.length < 150 && tender.title && tender.title.length < 50) {
        result.needsLLM = false;
        result.reason = "Tender content is minimal, using direct parsing";
    }
    
    // Very large tenders might exceed token limits and should use specialized handling
    if (tender.description && tender.description.length > 15000) {
        result.needsLLM = false;
        result.reason = "Tender description exceeds optimal size for LLM processing, using chunked parsing";
    }

    // 5. Field quality assessment - check if we have high-quality fields already
    if (result.needsLLM && tender.title && tender.description && 
        tender.title.length > 10 && tender.description.length > 100) {
        // Has good title and description, check other key fields
        if ((tender.publication_date || tender.deadline_date) && tender.status) {
            result.needsLLM = false;
            result.reason = "Tender has high-quality core fields already, using direct parsing";
        }
    }

    // Performance tracking
    if (!result.needsLLM) {
        // Track the percentage of tenders skipping LLM processing
        try {
            // This code just increments counters in memory - could be replaced with a proper metrics system
            global.normalizationStats = global.normalizationStats || {
                total: 0,
                skippedLLM: 0,
                sources: {}
            };
            
            global.normalizationStats.total += 1;
            global.normalizationStats.skippedLLM += 1;
            
            // Track by source
            if (!global.normalizationStats.sources[sourceTable]) {
                global.normalizationStats.sources[sourceTable] = { total: 0, skippedLLM: 0 };
            }
            global.normalizationStats.sources[sourceTable].total += 1;
            global.normalizationStats.sources[sourceTable].skippedLLM += 1;
            
            // Log efficiency stats every 100 tenders
            if (global.normalizationStats.total % 100 === 0) {
                const efficiency = (global.normalizationStats.skippedLLM / global.normalizationStats.total * 100).toFixed(2);
                console.log(`Normalization efficiency: ${efficiency}% of tenders processed without LLM (${global.normalizationStats.skippedLLM}/${global.normalizationStats.total})`);
                
                // Log source-specific stats
                Object.entries(global.normalizationStats.sources).forEach(([source, stats]) => {
                    const sourceEfficiency = (stats.skippedLLM / stats.total * 100).toFixed(2);
                    console.log(`  ${source}: ${sourceEfficiency}% (${stats.skippedLLM}/${stats.total})`);
                });
            }
        } catch (e) {
            // Ignore errors in statistics tracking
        }
    }

    return result;
}

/**
 * More comprehensive check for English text
 * @param {string} text - The text to analyze
 * @returns {boolean} Whether the text is strongly identified as English
 */
function isStronglyEnglish(text) {
    if (!text || typeof text !== 'string') return false;
    if (text.length < 20) return false; // Need enough text to analyze
    
    // Enhanced list of common English words
    const englishWords = [
        'the', 'and', 'for', 'this', 'that', 'with', 'from', 
        'have', 'has', 'had', 'not', 'are', 'were', 'was',
        'will', 'would', 'should', 'could', 'can', 'may',
        'than', 'then', 'they', 'them', 'their', 'there',
        'here', 'where', 'when', 'which', 'what', 'who'
    ];
    
    const textLower = text.toLowerCase();
    let englishWordCount = 0;
    let wordCount = 0;
    
    // Count total words (approximate)
    wordCount = textLower.split(/\s+/).length;
    
    // Count English words
    englishWords.forEach(word => {
        const pattern = new RegExp(`\\b${word}\\b`, 'g');
        const matches = textLower.match(pattern);
        if (matches) englishWordCount += matches.length;
    });
    
    // If text has a significant percentage of common English words, it's strongly English
    // For short texts (< 100 words), require at least 3 matches
    // For longer texts, require at least 5% of words to be common English words
    if (wordCount < 100) {
        return englishWordCount >= 3;
    } else {
        return (englishWordCount / wordCount) >= 0.05;
    }
}

/**
 * Checks if a tender is missing any critical fields that would require normalization
 * @param {Object} tender - The tender to check
 * @returns {Array} Array of missing field names
 */
function checkForMissingCriticalFields(tender) {
    const criticalFields = [
        'title', 
        'description', 
        'publication_date', 
        'deadline_date', 
        'status',
        'tender_type',
        'estimated_value'
    ];
    
    return criticalFields.filter(field => !tender[field]);
}

/**
 * Basic detection of English text
 * @param {string} text - The text to analyze
 * @returns {boolean} Whether the text appears to be in English
 */
function isEnglishText(text) {
    if (!text || typeof text !== 'string') return false;
    
    // Simple heuristic: check for common English words
    const englishWords = ['the', 'and', 'for', 'this', 'that', 'with', 'from'];
    const textLower = text.toLowerCase();
    let englishWordCount = 0;
    
    englishWords.forEach(word => {
        const pattern = new RegExp(`\\b${word}\\b`, 'g');
        const matches = textLower.match(pattern);
        if (matches) englishWordCount += matches.length;
    });
    
    // If we find several common English words, it's likely English
    return englishWordCount >= 3;
}

/**
 * Normalizes a tender using fast rule-based approaches
 * @param {Object} tenderData - The tender data to normalize
 * @param {string} sourceTable - The name of the source table
 * @returns {Object} Normalized tender data with appropriate fields
 */
async function fastNormalizeTender(tenderData, sourceTable) {
    console.log(`Using fast normalization for tender: ${tenderData.id || 'unknown'}`);
    
    // Check if this source has a dedicated fallback handler
    const dedicatedSources = ['sam_gov', 'ted', 'iadb', 'un_procurement', 'adb', 'afd_tenders'];
    
    if (dedicatedSources.includes(sourceTable)) {
        console.log(`Using dedicated handler for ${sourceTable}`);
        return fallbackNormalizeTender(tenderData, sourceTable);
    }
    
    // Generic handling for other sources
    console.log(`Using generic fast normalization for ${sourceTable}`);
    
    // Create a normalized data object with all possible fields
    const normalized = {
        title: null,
        title_english: null,
        description: null,
        description_english: null,
        tender_type: null,
        status: null,
        publication_date: null,
        deadline_date: null,
        country: null,
        city: null,
        organization_name: null,
        organization_name_english: null,
        organization_id: null,
        buyer: null,
        buyer_english: null,
        project_name: null,
        project_name_english: null,
        project_id: null,
        project_number: null,
        sector: null,
        estimated_value: null,
        currency: null,
        contact_name: null,
        contact_email: null,
        contact_phone: null,
        contact_address: null,
        url: null,
        document_links: [],
        language: 'en', // Default assumption
        notice_id: null,
        reference_number: null,
        procurement_method: null
    };
    
    // Common field mapping - try different field names across sources
    mapField(tenderData, normalized, 'title', ['title', 'opportunity_title', 'name', 'project_name', 'contract_title']);
    mapField(tenderData, normalized, 'description', ['description', 'body', 'summary', 'details', 'project_description']);
    mapField(tenderData, normalized, 'status', ['status', 'state', 'opportunity_status', 'tender_status']);
    mapField(tenderData, normalized, 'country', ['country', 'country_name', 'nation', 'location_country']);
    mapField(tenderData, normalized, 'city', ['city', 'town', 'location_city', 'place']);
    mapField(tenderData, normalized, 'organization_name', ['organization_name', 'agency', 'authority', 'contracting_authority', 'buyer_name']);
    mapField(tenderData, normalized, 'organization_id', ['organization_id', 'agency_id', 'authority_id']);
    mapField(tenderData, normalized, 'notice_id', ['notice_id', 'id', 'tender_id', 'opportunity_id', 'document_number']);
    mapField(tenderData, normalized, 'reference_number', ['reference_number', 'reference', 'ref', 'solicitation_number']);
    
    // Try to extract the language if provided
    mapField(tenderData, normalized, 'language', ['language', 'lang']);
    
    // Handle dates with various formats
    extractDate(tenderData, normalized, 'publication_date', ['publication_date', 'published', 'publish_date', 'issued_date', 'created_at']);
    extractDate(tenderData, normalized, 'deadline_date', ['deadline_date', 'deadline', 'closing_date', 'response_date', 'submission_deadline']);
    
    // Extract money values and currency
    extractMoney(tenderData, normalized, ['value', 'amount', 'contract_value', 'estimated_value', 'budget']);
    
    // Extract email from description or explicit fields
    extractContacts(tenderData, normalized);
    
    // Extract document links if present
    extractDocumentLinks(tenderData, normalized);
    
    // Normalize URL field
    if (tenderData.url) normalized.url = tenderData.url;
    else if (tenderData.link) normalized.url = tenderData.link;
    else if (tenderData.web_link) normalized.url = tenderData.web_link;
    
    // Infer status from dates if not explicitly provided
    inferStatusFromDates(normalized);
    
    // Clean up and enhance all extracted fields
    enhanceExtractedFields(normalized);
    
    // Apply our enhanced title normalization and field filling
    const enhancedTender = enhanceTenderTitles(normalized);
    const fullyEnhancedTender = fillMissingFields(enhancedTender);
    
    return fullyEnhancedTender;
}

/**
 * Maps a field from source to target with multiple possible source field names
 * @param {Object} source - Source object
 * @param {Object} target - Target object
 * @param {string} targetField - Target field name
 * @param {Array} sourceFields - Array of possible source field names
 */
function mapField(source, target, targetField, sourceFields) {
    for (const field of sourceFields) {
        if (source[field] !== undefined && source[field] !== null) {
            target[targetField] = source[field];
            return;
        }
    }
}

/**
 * Extracts and normalizes a date field
 * @param {Object} source - Source object
 * @param {Object} target - Target object
 * @param {string} targetField - Target field name
 * @param {Array} sourceFields - Array of possible source field names
 */
function extractDate(source, target, targetField, sourceFields) {
    for (const field of sourceFields) {
        if (source[field]) {
            try {
                // Handle various date formats
                const date = new Date(source[field]);
                if (!isNaN(date.getTime())) {
                    target[targetField] = date.toISOString().split('T')[0]; // YYYY-MM-DD format
                    return;
                }
            } catch (e) {
                console.log(`Could not parse date from ${field}: ${source[field]}`);
            }
        }
    }
}

/**
 * Extracts money value and currency
 * @param {Object} source - Source object
 * @param {Object} target - Target object
 * @param {Array} sourceFields - Array of possible source field names
 */
function extractMoney(source, target, sourceFields) {
    for (const field of sourceFields) {
        if (source[field]) {
            // Check if the field is already an object with value and currency
            if (typeof source[field] === 'object' && source[field].value) {
                target.estimated_value = parseFloat(source[field].value);
                if (source[field].currency) {
                    target.currency = source[field].currency.toUpperCase();
                }
                return;
            }
            
            // If it's a string, try to extract numeric value and currency
            if (typeof source[field] === 'string') {
                const moneyString = source[field].trim();
                
                // First try to extract the numeric value - handle both formats
                const valueMatch = moneyString.match(/[\d,]+(\.\d+)?/);
                if (valueMatch) {
                    // Remove commas and convert to number
                    const numericValue = parseFloat(valueMatch[0].replace(/,/g, ''));
                    if (!isNaN(numericValue)) {
                        target.estimated_value = numericValue;
                    }
                }
                
                // Then look for currency codes or symbols
                const currencyRegex = /(\$|€|£|[A-Z]{3})\s*|\s*(\$|€|£|[A-Z]{3})/i;
                const currencyMatch = moneyString.match(currencyRegex);
                
                if (currencyMatch) {
                    const symbol = (currencyMatch[1] || currencyMatch[2]).toUpperCase();
                    const currencyMap = {
                        '$': 'USD',
                        '€': 'EUR',
                        '£': 'GBP',
                        'USD': 'USD',
                        'EUR': 'EUR',
                        'GBP': 'GBP',
                        'JPY': 'JPY',
                        'CHF': 'CHF',
                        'MXN': 'MXN',
                        'LKR': 'LKR',
                        'PHP': 'PHP',
                        'XOF': 'XOF',
                        'INR': 'INR',
                        'BDT': 'BDT',
                        'PKR': 'PKR',
                        'NPR': 'NPR',
                        'IDR': 'IDR',
                        'THB': 'THB',
                        'VND': 'VND',
                        'CNY': 'CNY',
                        'KRW': 'KRW',
                        'AUD': 'AUD',
                        'NZD': 'NZD',
                        'CAD': 'CAD',
                        // Add more currency mappings as needed
                    };
                    target.currency = currencyMap[symbol] || symbol;
                }
                
                // Ensure we have a valid numeric value
                if (target.estimated_value === undefined || isNaN(target.estimated_value)) {
                    target.estimated_value = null;
                }
                
                return;
            }
            
            // If it's just a number, use it directly
            if (typeof source[field] === 'number') {
                target.estimated_value = source[field];
                return;
            }
        }
    }
}

/**
 * Extracts contact information from tender data
 * @param {Object} source - Source object
 * @param {Object} target - Target object
 */
function extractContacts(source, target) {
    // Direct mapping of contact fields
    mapField(source, target, 'contact_name', ['contact_name', 'contact', 'contact_person', 'poc', 'point_of_contact']);
    mapField(source, target, 'contact_email', ['contact_email', 'email']);
    mapField(source, target, 'contact_phone', ['contact_phone', 'phone', 'telephone']);
    mapField(source, target, 'contact_address', ['contact_address', 'address']);
    
    // If contact email is not found, try to extract from description
    if (!target.contact_email && source.description) {
        const emailRegex = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g;
        const emailMatches = source.description.match(emailRegex);
        if (emailMatches && emailMatches.length > 0) {
            target.contact_email = emailMatches[0];
        }
    }
    
    // Try to extract phone numbers from description if not found directly
    if (!target.contact_phone && source.description) {
        const phoneRegex = /\b\+?[0-9]{1,3}[-. (]?[0-9]{3}[-. )]?[0-9]{3}[-. ]?[0-9]{4}\b/g;
        const phoneMatches = source.description.match(phoneRegex);
        if (phoneMatches && phoneMatches.length > 0) {
            target.contact_phone = phoneMatches[0];
        }
    }
}

/**
 * Extracts document links from tender data
 * @param {Object} source - Source object
 * @param {Object} target - Target object
 */
function extractDocumentLinks(source, target) {
    // Check for attachments or documents array
    if (Array.isArray(source.attachments)) {
        source.attachments.forEach(attachment => {
            const doc = { 
                title: attachment.title || attachment.name || 'Attachment', 
                url: attachment.url || attachment.link || attachment.file_url 
            };
            if (doc.url) {
                target.document_links.push(doc);
            }
        });
        return;
    }
    
    if (Array.isArray(source.documents)) {
        source.documents.forEach(doc => {
            const document = { 
                title: doc.title || doc.name || 'Document', 
                url: doc.url || doc.link || doc.file_url 
            };
            if (document.url) {
                target.document_links.push(document);
            }
        });
        return;
    }
    
    // Check for single document link
    if (source.document_url || source.attachment_url) {
        const docUrl = source.document_url || source.attachment_url;
        const docTitle = source.document_title || source.attachment_title || 'Document';
        target.document_links.push({ title: docTitle, url: docUrl });
    }
}

/**
 * Infers status from dates if not explicitly provided
 * @param {Object} tender - The tender object to modify
 */
function inferStatusFromDates(tender) {
    if (!tender.status && tender.deadline_date) {
        const now = new Date();
        const deadline = new Date(tender.deadline_date);
        tender.status = deadline > now ? 'Open' : 'Closed';
    }
}

/**
 * Enhances all extracted fields for consistency
 * @param {Object} tender - The tender object to enhance
 */
function enhanceExtractedFields(tender) {
    // Clean up and normalize text fields
    Object.keys(tender).forEach(key => {
        if (typeof tender[key] === 'string') {
            // Trim whitespace and normalize spaces
            tender[key] = tender[key].trim().replace(/\s+/g, ' ');
            
            // Capitalize first letter of titles and names
            if (['title', 'organization_name', 'buyer', 'project_name'].includes(key)) {
                if (tender[key].length > 0) {
                    tender[key] = tender[key].charAt(0).toUpperCase() + tender[key].slice(1);
                }
            }
        }
    });
    
    // Normalize status values
    if (tender.status) {
        const status = tender.status.toLowerCase();
        if (['active', 'open', 'ongoing', 'current'].includes(status)) {
            tender.status = 'Open';
        } else if (['closed', 'expired', 'completed', 'inactive', 'archived'].includes(status)) {
            tender.status = 'Closed';
        } else if (['awarded', 'contract awarded', 'winner selected'].includes(status)) {
            tender.status = 'Awarded';
        } else if (['canceled', 'cancelled', 'terminated'].includes(status)) {
            tender.status = 'Canceled';
        }
    }
    
    // Apply title enhancement function from the LLM workflow
    return enhanceTenderTitles(tender);
}

/**
 * Enhanced normalizeTender function with selective LLM usage
 * @param {Object} tender - The tender data to normalize
 * @param {string} sourceTable - The source table name 
 * @returns {Promise<Object>} Normalized tender data
 */
async function normalizeTender(tender, sourceTable) {
    console.log(`Evaluating normalization needs for tender from ${sourceTable}`);
    const startTime = Date.now();
    
    // Save original state for logging
    const originalTender = JSON.parse(JSON.stringify(tender));
    
    // Only log initial state for 1 in 50 tenders to reduce console spam
    tenderProcessingCounter++;
    if (tenderProcessingCounter % 50 === 0) {
        // Use simplified logging to prevent overwhelming the console
        const filledFields = Object.values(tender).filter(v => v !== null && v !== undefined && v !== '').length;
        const totalFields = Object.keys(tender).length;
        
        console.log(`\n--- PROCESSING TENDER #${tenderProcessingCounter} - ${sourceTable}:${tender.id || 'unknown'} ---`);
        console.log(`Initial completeness: ${filledFields}/${totalFields} fields (${(filledFields/totalFields*100).toFixed(2)}%)`);
    }
    
    try {
        // Skip LLM for certain tenders
        const evaluation = evaluateNormalizationNeeds(tender, sourceTable);
        
        if (!evaluation.needsLLM) {
            console.log(`Using fast normalization for tender: ${evaluation.reason}`);
            const normalizedData = fastNormalizeTender(tender, sourceTable);
            
            const endTime = Date.now();
            console.log(`Fast normalization completed in ${(endTime - startTime) / 1000} seconds`);
            
            // Add metadata
            normalizedData.normalized_at = new Date().toISOString();
            normalizedData.normalized_method = 'rule-based-fast';
            normalizedData.source_table = sourceTable;
            normalizedData.processing_time_ms = endTime - startTime;
            
            // For final results logging, also reduce frequency and simplify
            if (tenderProcessingCounter % 50 === 0) {
                // Get the result from whatever normalization path was used
                let finalResult;
                if (typeof fullyEnhancedData !== 'undefined') {
                    finalResult = fullyEnhancedData;
                } else if (typeof enhancedData !== 'undefined') {
                    finalResult = enhancedData;
                } else if (typeof normalizedData !== 'undefined') {
                    finalResult = normalizedData;
                } else {
                    finalResult = result;
                }
                
                const originalFilled = Object.values(originalTender).filter(v => v !== null && v !== undefined && v !== '').length;
                const finalFilled = Object.values(finalResult).filter(v => v !== null && v !== undefined && v !== '').length;
                const totalFields = Object.keys(finalResult).length;
                
                console.log(`\n--- FINAL RESULT - ${finalResult.normalized_method || 'unknown'} - ${(finalResult.processing_time_ms || 0)/1000}s ---`);
                console.log(`Completeness: ${originalFilled}/${totalFields} → ${finalFilled}/${totalFields} fields`);
                console.log(`Improvement: +${finalFilled - originalFilled} fields (${((finalFilled-originalFilled)/totalFields*100).toFixed(2)}%)`);
                
                // Show just a few key field changes to avoid log flooding
                const keyFields = ['title', 'description', 'status', 'sector'];
                keyFields.forEach(field => {
                    const before = originalTender[field];
                    const after = finalResult[field];
                    
                    if (before !== after && after) {
                        const beforeStr = before ? (before.substring(0, 40) + (before.length > 40 ? '...' : '')) : '(empty)';
                        const afterStr = after.substring(0, 40) + (after.length > 40 ? '...' : '');
                        console.log(`${field}: "${beforeStr}" → "${afterStr}"`);
                    }
                });
            }
            
            return normalizedData;
        }
        
        console.log(`Using LLM normalization for tender: ${evaluation.reason}`);
        
        // Generate a prompt for the LLM
        const prompt = generatePrompt(tender, sourceTable);
        
        // Query the LLM
        const llmResponse = await queryLLM(prompt);
        
        if (!llmResponse || !llmResponse.choices || llmResponse.choices.length === 0) {
            console.warn('LLM returned empty response, falling back to rule-based normalization');
            return fallbackWithMetadata(tender, sourceTable, 'empty-llm-response', startTime);
        }
        
        // Extract the LLM response text
        const responseText = llmResponse.choices[0].message.content;
        
        // Parse the JSON from the LLM response
        const normalizedData = parseJSONFromLLMResponse(responseText);
        
        if (!normalizedData) {
            console.warn('Failed to parse JSON from LLM response, falling back to rule-based normalization');
            return fallbackWithMetadata(tender, sourceTable, 'json-parse-failure', startTime);
        }
        
        // Enhance the normalized data with better titles and fill missing fields
        const enhancedData = enhanceTenderTitles(normalizedData);
        const fullyEnhancedData = fillMissingFields(enhancedData);
        
        // Add metadata
        fullyEnhancedData.normalized_at = new Date().toISOString();
        fullyEnhancedData.normalized_method = 'llm';
        fullyEnhancedData.source_table = sourceTable;
        
        const endTime = Date.now();
        fullyEnhancedData.processing_time_ms = endTime - startTime;
        
        console.log(`LLM normalization completed in ${(endTime - startTime) / 1000} seconds`);
        
        // Whether we used LLM, fast, or fallback normalization, log final results for sample tenders
        if (tenderProcessingCounter % 50 === 0) {
            // Get the result, whether it's enhancedData, normalizedData, or result from fallback
            let finalResult;
            if (typeof fullyEnhancedData !== 'undefined') {
                finalResult = fullyEnhancedData;
            } else if (typeof enhancedData !== 'undefined') {
                finalResult = enhancedData;
            } else if (typeof normalizedData !== 'undefined') {
                finalResult = normalizedData;
            } else {
                finalResult = result;
            }
            
            const originalFilled = Object.values(originalTender).filter(v => v !== null && v !== undefined && v !== '').length;
            const finalFilled = Object.values(finalResult).filter(v => v !== null && v !== undefined && v !== '').length;
            const totalFields = Object.keys(finalResult).length;
            
            console.log(`\n--- FINAL RESULT - ${finalResult.normalized_method || 'unknown'} - ${(finalResult.processing_time_ms || 0)/1000}s ---`);
            console.log(`Completeness: ${originalFilled}/${totalFields} → ${finalFilled}/${totalFields} fields`);
            console.log(`Improvement: +${finalFilled - originalFilled} fields (${((finalFilled-originalFilled)/totalFields*100).toFixed(2)}%)`);
            
            // Show just a few key field changes to avoid log flooding
            const keyFields = ['title', 'description', 'status', 'sector'];
            keyFields.forEach(field => {
                const before = originalTender[field];
                const after = finalResult[field];
                
                if (before !== after && after) {
                    const beforeStr = before ? (before.substring(0, 40) + (before.length > 40 ? '...' : '')) : '(empty)';
                    const afterStr = after.substring(0, 40) + (after.length > 40 ? '...' : '');
                    console.log(`${field}: "${beforeStr}" → "${afterStr}"`);
                }
            });
        }
        
        return fullyEnhancedData;
    } catch (error) {
        console.error(`Error normalizing tender: ${error.message}`);
        
        // Check if error is related to OpenAI credits
        if (error.message && (
            error.message.includes('402') || 
            error.message.includes('Payment Required') ||
            error.message.includes('insufficient_quota') ||
            error.message.includes('billing')
        )) {
            console.warn('OpenAI API credit issue detected, using rule-based normalization');
            return fallbackWithMetadata(tender, sourceTable, 'api-credit-issue', startTime);
        }
        
        // For other errors, also use fallback
        return fallbackWithMetadata(tender, sourceTable, `error: ${error.message}`, startTime);
    }
}

/**
 * Fallback normalization with additional metadata
 * @param {Object} tender - The tender data to normalize
 * @param {string} sourceTable - The source table name
 * @param {string} [method='fallback'] - The normalization method to record
 * @returns {Object} Normalized tender with metadata
 */
function fallbackWithMetadata(tender, sourceTable, method = 'fallback') {
    console.log(`Using ${method} normalization for ${sourceTable}: ${tender.id || 'unknown'}`);
    const startTime = performance.now();
    
    // Save original state for logging
    const originalTender = JSON.parse(JSON.stringify(tender));
    
    let result;
    try {
        result = fallbackNormalizeTender(tender, sourceTable);
        
        // Handle special cases
        if (sourceTable === 'adb' && method === 'rule-based-fast') {
            console.log('Fast normalization completed for ADB tender using optimized path');
            performanceStats.fastNormalization++;
            result.normalized_method = 'rule-based-fast';
        } else if (sourceTable === 'afd_tenders' && method === 'rule-based-fast') {
            console.log('Fast normalization completed for AFD tender using optimized path');
            performanceStats.fastNormalization++;
            result.normalized_method = 'rule-based-fast';
        } else {
            performanceStats.fallbackNormalization++;
            result.normalized_method = method;
        }
    } catch (error) {
        console.error(`Error in fallback normalization for ${sourceTable}:`, error);
        // Create a minimal normalized object with error information
        result = {
            normalized_method: `${method}-error`,
            description: error.message || 'Unknown error in fallback normalization',
            title: tender.title || tender.name || 'Unknown tender',
            status: 'error',
            source_data: tender
        };
    }
    
    const endTime = performance.now();
    const processingTime = (endTime - startTime) / 1000; // Convert to seconds
    
    console.log(`${method === 'rule-based-fast' ? 'Fast' : 'Fallback'} normalization completed in ${processingTime.toFixed(3)} seconds`);
    
    // Add source info
    result.source_table = sourceTable;
    result.source_id = tender.id;
    
    // Log fallback results for sampling of tenders
    if (tenderProcessingCounter % 50 === 0) {
        console.log(`\n========== FINAL NORMALIZATION RESULTS (FALLBACK) ==========`);
        console.log(`Source: ${sourceTable}, ID: ${tender.id || 'unknown'}`);
        console.log(`Normalization method: ${result.normalized_method}`);
        console.log(`Processing time: ${processingTime.toFixed(3)} seconds`);
        
        try {
            // Compare original vs final state
            const originalFilledCount = Object.values(originalTender).filter(v => v !== null && v !== undefined && v !== '').length;
            const finalFilledCount = Object.values(result).filter(v => v !== null && v !== undefined && v !== '').length;
            
            console.log(`\nFIELD COMPLETION IMPROVEMENT:`);
            console.log(`Original filled fields: ${originalFilledCount}`);
            console.log(`Final filled fields: ${finalFilledCount}`);
            console.log(`Improvement: ${finalFilledCount - originalFilledCount} new fields filled (${((finalFilledCount - originalFilledCount) / Object.keys(result).length * 100).toFixed(2)}%)`);
            
            // Show dramatic changes in key fields
            const keyFields = ['title', 'description', 'organization_name', 'status', 'sector', 'tender_type'];
            console.log(`\nKEY FIELD CHANGES:`);
            keyFields.forEach(field => {
                const originalValue = originalTender[field] || '(empty)';
                const finalValue = result[field] || '(empty)';
                
                if (originalValue !== finalValue) {
                    console.log(`${field}:\n  BEFORE: ${originalValue}\n  AFTER:  ${finalValue}`);
                }
            });
        } catch (e) {
            console.error('Error during logging:', e);
        }
        
        console.log(`==================================================\n`);
    }
    
    return result;
}

/**
 * Generates a prompt for the LLM based on tender data
 * @param {Object} tender - The tender data
 * @param {string} sourceTable - The source table name
 * @returns {string} The prompt for the LLM
 */
function generatePrompt(tender, sourceTable) {
    // Common fields to extract
    const fields = [
        "title", "title_english", "description", "description_english", "tender_type", 
        "status", "publication_date", "deadline_date", "country", "city", 
        "organization_name", "organization_name_english", "organization_id", 
        "buyer", "buyer_english", "project_name", "project_name_english", 
        "project_id", "project_number", "sector", "estimated_value", "currency",
        "contact_name", "contact_email", "contact_phone", "contact_address",
        "url", "document_links", "language", "notice_id", "reference_number",
        "procurement_method"
    ];
    
    // Create a JSON schema for the response
    const fieldDefinitions = fields.map(field => {
        return `    "${field}": "The ${field.replace(/_/g, ' ')} of the tender"`;
    }).join(',\n');
    
    // Special handling for document_links which should be an array
    const schemaStr = `{
${fieldDefinitions.replace('"document_links": "The document links of the tender"', '"document_links": ["Array of document URLs, each with title and url properties"]')}
}`;
    
    // Base prompt structure
    let prompt = `You are an expert procurement data analyst tasked with extracting and normalizing tender information.
Given the raw tender data below, please extract the following structured information.
If the information is not available, use null for that field. 
Normalize dates to YYYY-MM-DD format.
Normalize all text fields to use proper capitalization and remove any redundant spacing, prefixes, or unwanted patterns.

The response should be a single JSON object with the following structure:
${schemaStr}

For document_links, each item should have the structure: {"title": "Document title", "url": "Document URL"}

`;

    // Add source-specific instructions
    if (sourceTable === 'sam_gov') {
        prompt += 'This is a tender from SAM.gov (US Government). ';
        prompt += 'For SAM.gov tenders, the country is "UNITED STATES". ';
        prompt += 'The organization_name can be found in either organizationName or agency fields. ';
        prompt += 'The status should be mapped from opportunity_status: active → Open, inactive/archived → Closed, awarded → Awarded, canceled → Canceled. ';
    } else if (sourceTable === 'ted') {
        prompt += 'This is a tender from TED (Tenders Electronic Daily, European Union). ';
        prompt += 'For TED tenders, infer the status based on the deadline_date compared to current date. ';
    } else if (sourceTable === 'un_procurement') {
        prompt += 'This is a tender from United Nations Procurement. ';
        prompt += 'For UN tenders, the organization is typically the specific UN agency if mentioned, otherwise "United Nations". ';
    }
    
    prompt += `\nRaw tender data:\n${JSON.stringify(tender, null, 2)}`;
    
    return prompt;
}

module.exports = {
    normalizeTender,
    queryLLM,
    enhanceTenderTitles,
    fallbackNormalizeTender,
    evaluateNormalizationNeeds,
    fastNormalizeTender,
    fillMissingFields
};