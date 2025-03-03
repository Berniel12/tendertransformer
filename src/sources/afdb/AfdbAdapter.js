/**
 * AfdbAdapter.js
 * Adapter for processing African Development Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class AfdbAdapter extends BaseSourceAdapter {
  constructor() {
    super('afdb');
  }
  
  /**
   * Extract source ID from AfDB tender
   */
  getSourceId(tender) {
    return tender.tender_id?.toString() || tender.reference_number?.toString() || tender.id?.toString();
  }
  
  /**
   * Safely parse a date string to YYYY-MM-DD format
   * Returns null if date is invalid
   */
  safeParseDate(dateStr) {
    if (!dateStr) return null;
    
    try {
      // First try parsing as ISO date
      const date = new Date(dateStr);
      if (isNaN(date.getTime())) {
        // If invalid, try other common formats
        const formats = [
          // DD/MM/YYYY
          /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/,
          // DD-MM-YYYY
          /^(\d{1,2})-(\d{1,2})-(\d{4})$/,
          // YYYY/MM/DD
          /^(\d{4})\/(\d{1,2})\/(\d{1,2})$/,
          // YYYY-MM-DD
          /^(\d{4})-(\d{1,2})-(\d{1,2})$/
        ];

        for (const format of formats) {
          const match = dateStr.match(format);
          if (match) {
            const [_, first, second, third] = match;
            // Check if year is first or last based on format
            const isYearFirst = first.length === 4;
            const year = isYearFirst ? first : third;
            const month = isYearFirst ? second : first;
            const day = isYearFirst ? third : second;
            
            // Create date object and validate
            const parsedDate = new Date(year, month - 1, day);
            if (!isNaN(parsedDate.getTime())) {
              return parsedDate.toISOString().split('T')[0];
            }
          }
        }
        return null;
      }
      return date.toISOString().split('T')[0];
    } catch (e) {
      console.warn(`Failed to parse date: ${dateStr}`, e);
      return null;
    }
  }
  
  /**
   * Map AfDB fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.tender_title,
      description: tender.description || tender.tender_description,
      publication_date: this.safeParseDate(tender.publication_date),
      deadline_date: this.safeParseDate(tender.deadline_date || tender.closing_date),
      notice_id: tender.tender_id?.toString() || tender.reference_number?.toString(),
      reference_number: tender.reference_number || tender.tender_id,
      organization_id: tender.borrower || tender.organization || tender.agency,
      country: tender.country || tender.location,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_type || tender.tender_type || "Tender",
      sector: tender.sector || tender.category
    };
  }
  
  /**
   * Generate standard URL for AfDB tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.tender_id) {
      return `https://www.afdb.org/en/projects-operations/procurement/tender/${tender.tender_id}`;
    }
    return null;
  }
  
  /**
   * Map AfDB specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "open": "Open",
      "closed": "Closed",
      "awarded": "Awarded",
      "cancelled": "Canceled",
      "canceled": "Canceled"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get AfDB specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR AFRICAN DEVELOPMENT BANK DATA:
    1. For 'borrower', 'organization', or 'agency' field - use as the organization name.
    2. Extract country information from 'country' or 'location' fields.
    3. For 'procurement_type' or 'tender_type' - map to the appropriate tender type.
    4. For 'sector' or 'category' - use as category information.
    5. For 'description' or 'tender_description' - extract as much structured information as possible.
    6. Extract financial information from 'estimated_cost', 'contract_value', or similar fields.
    7. Look for project information in 'project_name', 'project_id', or related fields.
    8. Watch for multilingual entries - prioritize English but retain information in other languages if useful.
    `;
  }
}

module.exports = AfdbAdapter; 