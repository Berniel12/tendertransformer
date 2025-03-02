/**
 * WbAdapter.js
 * Adapter for processing World Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class WbAdapter extends BaseSourceAdapter {
  constructor() {
    super('wb');
  }
  
  /**
   * Extract source ID from World Bank tender
   */
  getSourceId(tender) {
    return tender.id?.toString() || tender.notice_no?.toString();
  }
  
  /**
   * Map World Bank fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.notice_title || tender.project_name,
      description: tender.notice_details || tender.description,
      publication_date: tender.published_date ? new Date(tender.published_date).toISOString().split('T')[0] : null,
      deadline_date: tender.closing_date ? new Date(tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.notice_no?.toString() || tender.id?.toString(),
      reference_number: tender.notice_no?.toString(),
      organization_id: tender.borrower || tender.organization,
      country: tender.country || tender.borrower_country,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_method || "Tender",
      sector: tender.sector
    };
  }
  
  /**
   * Generate standard URL for World Bank tenders
   */
  generateUrl(tender) {
    if (tender.notice_no) {
      return `https://projects.worldbank.org/en/projects-operations/procurement/notice/${tender.notice_no}`;
    }
    return null;
  }
  
  /**
   * Map World Bank specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "closed": "Closed",
      "awarded": "Awarded",
      "canceled": "Canceled"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get World Bank specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR WORLD BANK DATA:
    1. For 'borrower' field - use as the organization name.
    2. Extract country information from 'borrower_country' or 'country' fields.
    3. For 'procurement_method' - map to the appropriate tender type.
    4. For 'sector' - use as category information.
    5. For 'notice_details' - extract as much structured information as possible.
    6. Generate standard URL format: https://projects.worldbank.org/en/projects-operations/procurement/notice/{notice_no}
    7. Always look for 'estimated_value' or similar fields for financial information.
    `;
  }
}

module.exports = WbAdapter; 