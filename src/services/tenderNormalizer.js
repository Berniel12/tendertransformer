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
 * Post-processes tender titles to improve clarity and consistency
 * @param {Object} normalizedData - The normalized tender data from LLM
 * @returns {Object} The tender data with improved titles
 */
function enhanceTenderTitles(normalizedData) {
    if (!normalizedData || !normalizedData.title) {
        return normalizedData;
    }
    
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
        /^NO[\s.:-]+\d+/i                // NO. 12345 or NO: 12345
    ];
    
    for (const pattern of prefixPatterns) {
        title = title.replace(pattern, '');
        if (titleEnglish) {
            titleEnglish = titleEnglish.replace(pattern, '');
        }
    }
    
    // 2. Clean up titles that are just abbreviations or codes in parentheses
    title = title.replace(/\s*\([A-Z]{2,5}\)\s*$/i, '');
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\s*\([A-Z]{2,5}\)\s*$/i, '');
    }

    // 3. Move reference numbers to the end of the title
    const refNumberPatterns = [
        /\b([A-Z]{2,4}-\d{2,}-\d{2,})\b/g,     // Format like "ICB-20-12345"
        /\b(\d{5,})\b/g,                        // Random long numbers
        /\b([A-Z]{2,}\d{4,})\b/g                // Format like "ABC12345"
    ];
    
    let refNumbers = [];
    
    for (const pattern of refNumberPatterns) {
        const matches = [...title.matchAll(pattern)];
        if (matches.length > 0) {
            refNumbers = [...refNumbers, ...matches.map(m => m[0])];
            title = title.replace(pattern, ' ');
        }
    }
    
    // 4. Fix ALL CAPS titles
    if (title === title.toUpperCase() && title.length > 10) {
        // Convert to Title Case (capitalize first letter of each word)
        title = title.toLowerCase().replace(/\b\w+/g, word => {
            // Skip short conjunctions, articles, and prepositions unless they're the first word
            const minorWords = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of'];
            return minorWords.includes(word) ? word : word.charAt(0).toUpperCase() + word.slice(1);
        });
        
        // Ensure first word is always capitalized
        title = title.charAt(0).toUpperCase() + title.slice(1);
    }
    
    if (titleEnglish === titleEnglish.toUpperCase() && titleEnglish.length > 10) {
        titleEnglish = titleEnglish.toLowerCase().replace(/\b\w+/g, word => {
            const minorWords = ['a', 'an', 'the', 'and', 'but', 'or', 'for', 'nor', 'on', 'at', 'to', 'from', 'by', 'in', 'of'];
            return minorWords.includes(word) ? word : word.charAt(0).toUpperCase() + word.slice(1);
        });
        titleEnglish = titleEnglish.charAt(0).toUpperCase() + titleEnglish.slice(1);
    }
    
    // 5. Expand common acronyms
    const acronymMap = {
        'ICB': 'International Competitive Bidding for',
        'NCB': 'National Competitive Bidding for',
        'ICT': 'Information and Communication Technology',
        'IT': 'Information Technology',
        'RFP': 'Request for Proposal:',
        'EOI': 'Expression of Interest:',
        'HVAC': 'Heating, Ventilation, and Air Conditioning',
        'PPE': 'Personal Protective Equipment',
        'O&M': 'Operation and Maintenance',
        'M&E': 'Monitoring and Evaluation'
    };
    
    // Replace standalone acronyms (surrounded by spaces or at start/end)
    for (const [acronym, expansion] of Object.entries(acronymMap)) {
        const pattern = new RegExp(`\\b${acronym}\\b`, 'g');
        if (pattern.test(title)) {
            title = title.replace(pattern, expansion);
        }
        if (titleEnglish && pattern.test(titleEnglish)) {
            titleEnglish = titleEnglish.replace(pattern, expansion);
        }
    }
    
    // 6. Clean up excessive spacing
    title = title.replace(/\s+/g, ' ').trim();
    if (titleEnglish) {
        titleEnglish = titleEnglish.replace(/\s+/g, ' ').trim();
    }
    
    // 7. Truncate extremely long titles (over 150 chars) with ellipsis but preserve meaning
    if (title.length > 150) {
        // Split by common separators and keep first meaningful part
        const parts = title.split(/\s[-–—:]\s|\s*\|\s*|\s*;\s*/);
        if (parts.length > 1 && parts[0].length > 20) {
            title = parts[0].trim() + '...';
        } else if (title.length > 150) {
            title = title.substring(0, 147) + '...';
        }
    }

    if (titleEnglish && titleEnglish.length > 150) {
        const parts = titleEnglish.split(/\s[-–—:]\s|\s*\|\s*|\s*;\s*/);
        if (parts.length > 1 && parts[0].length > 20) {
            titleEnglish = parts[0].trim() + '...';
        } else if (titleEnglish.length > 150) {
            titleEnglish = titleEnglish.substring(0, 147) + '...';
        }
    }
    
    // 8. Capitalize first letter if entire title is lowercase
    if (/^[a-z]/.test(title)) {
        title = title.charAt(0).toUpperCase() + title.slice(1);
    }
    
    if (titleEnglish && /^[a-z]/.test(titleEnglish)) {
        titleEnglish = titleEnglish.charAt(0).toUpperCase() + titleEnglish.slice(1);
    }
    
    // 9. Add reference numbers back at the end if found
    if (refNumbers.length > 0) {
        title = title + ` (Ref: ${refNumbers.join(', ')})`;
        if (titleEnglish) {
            titleEnglish = titleEnglish + ` (Ref: ${refNumbers.join(', ')})`;
        }
    }
    
    // Update the normalized data
    normalizedData.title = title;
    if (titleEnglish) {
        normalizedData.title_english = titleEnglish;
    }
    
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
    console.log(`Using fallback normalization for ${sourceTable} due to LLM unavailability`);
    
    // Initialize normalized data with null values
    const normalizedData = {
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

    // Source-specific field mapping
    if (sourceTable === 'sam_gov') {
        // SAM.gov specific fields
        normalizedData.title = tenderData.opportunity_title || null;
        normalizedData.description = tenderData.description || null;
        normalizedData.notice_id = tenderData.opportunity_id?.toString() || null;
        normalizedData.reference_number = tenderData.solicitation_number || null;
        normalizedData.organization_id = tenderData.organization_id || null;
        normalizedData.country = "UNITED STATES"; // Default for SAM.gov
        
        // Try to extract dates
        if (tenderData.publish_date) {
            try {
                normalizedData.publication_date = new Date(tenderData.publish_date).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse publication date: ${tenderData.publish_date}`);
            }
        }
        
        if (tenderData.response_date) {
            try {
                normalizedData.deadline_date = new Date(tenderData.response_date).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse deadline date: ${tenderData.response_date}`);
            }
        }
        
        // Status mapping
        if (tenderData.opportunity_status) {
            const status = tenderData.opportunity_status.toLowerCase();
            if (status === 'active') normalizedData.status = 'Open';
            else if (status === 'inactive' || status === 'archived') normalizedData.status = 'Closed';
            else if (status === 'awarded') normalizedData.status = 'Awarded';
            else if (status === 'canceled') normalizedData.status = 'Canceled';
            else normalizedData.status = tenderData.opportunity_status;
        }
        
        // Type mapping
        if (tenderData.opportunity_type) {
            const type = tenderData.opportunity_type.toLowerCase();
            if (type === 'solicitation') normalizedData.tender_type = 'Tender';
            else if (type === 'presolicitation') normalizedData.tender_type = 'Prior Information Notice';
            else if (type === 'award') normalizedData.tender_type = 'Contract Award';
            else if (type === 'intent_to_award') normalizedData.tender_type = 'Intent to Award';
            else if (type === 'sources_sought') normalizedData.tender_type = 'Request for Information';
            else if (type === 'special_notice') normalizedData.tender_type = 'Special Notice';
            else if (type === 'sale_of_surplus') normalizedData.tender_type = 'Sale';
            else if (type === 'combined_synopsis_solicitation') normalizedData.tender_type = 'Request for Proposal';
            else normalizedData.tender_type = tenderData.opportunity_type;
        }
        
        // Try to extract email from description
        const emailRegex = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g;
        if (normalizedData.description) {
            const emailMatches = normalizedData.description.match(emailRegex);
            if (emailMatches && emailMatches.length > 0) {
                normalizedData.contact_email = emailMatches[0];
            }
        }
        
        // Generate URL
        if (tenderData.opportunity_id) {
            normalizedData.url = `https://sam.gov/opp/${tenderData.opportunity_id}/view`;
        }
        
        // Extract organization name if available
        if (tenderData.organizationName) {
            normalizedData.organization_name = tenderData.organizationName;
        } else if (tenderData.agency) {
            normalizedData.organization_name = tenderData.agency;
        }
        
        // Extract sector from NAICS code if available
        if (tenderData.naics_code) {
            const naicsPrefix = tenderData.naics_code.substring(0, 3);
            const naicsFirstTwo = tenderData.naics_code.substring(0, 2);
            
            if (naicsPrefix === '541') normalizedData.sector = "Information Technology";
            else if (['236', '237', '238'].includes(naicsPrefix)) normalizedData.sector = "Construction";
            else if (['31', '32', '33'].includes(naicsFirstTwo)) normalizedData.sector = "Manufacturing";
            else if (naicsFirstTwo === '54') normalizedData.sector = "Professional Services";
            else if (naicsFirstTwo === '62') normalizedData.sector = "Healthcare";
            else if (naicsPrefix === '336') normalizedData.sector = "Defense and Aerospace";
            else if (naicsFirstTwo === '48') normalizedData.sector = "Transportation";
            else if (naicsFirstTwo === '22') normalizedData.sector = "Utilities";
            else if (naicsFirstTwo === '11') normalizedData.sector = "Agriculture";
        }

        // Extract currency values
        try {
            // Helper function to extract numeric values
            function extractNumericValue(value) {
                if (value === null || value === undefined) return null;
                try {
                    if (typeof value === 'number') return value;
                    let cleanValue = value.toString()
                        .replace(/[A-Z]{3}\s*/g, '')
                        .replace(/[$€£¥]/g, '')
                        .replace(/,/g, '')
                        .trim();
                    const numericValue = parseFloat(cleanValue);
                    return isNaN(numericValue) ? null : numericValue;
                } catch (error) {
                    console.warn(`Failed to extract numeric value from: ${value}`);
                    return null;
                }
            }

            normalizedData.estimated_value = extractNumericValue(
                tenderData.estimated_value || 
                tenderData.potential_award_amount || 
                tenderData.award_amount
            );
            
            normalizedData.contract_value = extractNumericValue(
                tenderData.contract_value || 
                tenderData.award_amount || 
                tenderData.potential_award_amount
            );
        } catch (e) {
            console.warn(`Failed to process currency values: ${e.message}`);
        }
    }
    else if (sourceTable === 'ted') {
        // TED (Tenders Electronic Daily) specific fields
        normalizedData.title = tenderData.title || null;
        normalizedData.reference_number = tenderData.document_number || null;
        normalizedData.notice_id = tenderData.document_number || null;
        normalizedData.country = tenderData.country || null;
        normalizedData.organization_name = tenderData.contracting_authority || null;
        
        // Extract dates
        if (tenderData.publication_date) {
            try {
                normalizedData.publication_date = new Date(tenderData.publication_date).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse TED publication date: ${tenderData.publication_date}`);
            }
        }
        
        if (tenderData.deadline_date) {
            try {
                normalizedData.deadline_date = new Date(tenderData.deadline_date).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse TED deadline date: ${tenderData.deadline_date}`);
            }
        }
        
        // Try to determine status based on dates
        if (normalizedData.deadline_date) {
            const now = new Date();
            const deadline = new Date(normalizedData.deadline_date);
            normalizedData.status = deadline > now ? 'Open' : 'Closed';
        }
        
        // Set URL
        if (tenderData.document_number) {
            normalizedData.url = `https://ted.europa.eu/udl?uri=TED:NOTICE:${tenderData.document_number}:TEXT:EN:HTML`;
        }
        
        // Set language - typically multilingual with English
        normalizedData.language = 'en';
    }
    else if (sourceTable === 'iadb') {
        // Inter-American Development Bank fields
        normalizedData.title = tenderData.project_name || null;
        normalizedData.description = tenderData.description || tenderData.project_description || null;
        normalizedData.project_id = tenderData.project_number?.toString() || null;
        normalizedData.project_name = tenderData.project_name || null;
        normalizedData.country = tenderData.country || null;
        normalizedData.organization_name = "Inter-American Development Bank";
        normalizedData.organization_name_english = "Inter-American Development Bank";
        
        // Process dates if available
        if (tenderData.publication_date) {
            try {
                normalizedData.publication_date = new Date(tenderData.publication_date).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse IADB publication date: ${tenderData.publication_date}`);
            }
        }
        
        if (tenderData.deadline) {
            try {
                normalizedData.deadline_date = new Date(tenderData.deadline).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse IADB deadline date: ${tenderData.deadline}`);
            }
        }
        
        // Generate URL
        if (tenderData.project_number) {
            normalizedData.url = `https://www.iadb.org/en/project/${tenderData.project_number}`;
        }
        
        // Default values for IADB
        normalizedData.tender_type = 'Development Project';
        normalizedData.sector = tenderData.sector || null;
    }
    else if (sourceTable === 'un_procurement') {
        // UN Procurement fields
        normalizedData.title = tenderData.title || null;
        normalizedData.reference_number = tenderData.reference || null;
        normalizedData.organization_name = tenderData.agency || "United Nations";
        normalizedData.organization_name_english = tenderData.agency || "United Nations";
        
        // Process dates if available
        if (tenderData.published) {
            try {
                normalizedData.publication_date = new Date(tenderData.published).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse UN publication date: ${tenderData.published}`);
            }
        }
        
        if (tenderData.deadline) {
            try {
                normalizedData.deadline_date = new Date(tenderData.deadline).toISOString().split('T')[0];
            } catch (e) {
                console.warn(`Failed to parse UN deadline date: ${tenderData.deadline}`);
            }
        }
        
        // Set URL if available
        normalizedData.url = tenderData.url || null;
        
        // Set language - UN typically uses English
        normalizedData.language = 'en';
        
        // Try to determine status based on dates
        if (normalizedData.deadline_date) {
            const now = new Date();
            const deadline = new Date(normalizedData.deadline_date);
            normalizedData.status = deadline > now ? 'Open' : 'Closed';
        }
    }
    // Add other source handlers as needed
    
    // Apply title normalization to the basic extracted title
    if (normalizedData.title) {
        const enhancedData = enhanceTenderTitles(normalizedData);
        return enhancedData;
    }
    
    return normalizedData;
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
    
    return normalized;
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
        
        // Enhance the normalized data
        const enhancedData = enhanceTenderTitles(normalizedData);
        
        // Add metadata
        enhancedData.normalized_at = new Date().toISOString();
        enhancedData.normalized_method = 'llm';
        enhancedData.source_table = sourceTable;
        
        const endTime = Date.now();
        enhancedData.processing_time_ms = endTime - startTime;
        
        console.log(`LLM normalization completed in ${(endTime - startTime) / 1000} seconds`);
        
        return enhancedData;
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
    fastNormalizeTender
};